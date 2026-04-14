use super::*;

#[derive(Clone, Debug)]
pub(super) struct TerminalRuntimeFault {
    operation: &'static str,
    message: String,
}

impl std::fmt::Display for TerminalRuntimeFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.operation, self.message)
    }
}

#[derive(Debug)]
pub(super) enum ReplicationRuntimeMessage {
    PublishChanges(Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>),
    CreateGroup(Ask<CreateGroupRequest, Result<GroupId, ApiError>>),
    ChangeGroupMembership(Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>),
    #[cfg(test)]
    InstallGroupForTest(Ask<TestInstallGroup, Result<(), GroupInstallError>>),
    #[cfg(test)]
    ApplyUpdateBatchForTest(Ask<TestApplyUpdateBatch, Result<(), InboundDeliveryError>>),
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct TestInstallGroup {
    pub(super) group_id: GroupId,
    pub(super) members: GroupMembers,
}

#[cfg(test)]
#[derive(Debug)]
pub(super) struct TestApplyUpdateBatch {
    pub(super) sender: MemberIdentity,
    pub(super) message: UpdateBatchMessage,
}

#[derive(ComponentDefinition)]
pub(super) struct ReplicationRuntimeComponent {
    pub(super) ctx: ComponentContext<Self>,
    pub(super) group_broadcast: RequiredPort<crate::delivery::contracts::GroupBroadcastPort>,
    pub(super) reliable_delivery: RequiredPort<crate::delivery::contracts::ReliableDeliveryPort>,
    pub(super) local_member: MemberIdentity,
    pub(super) store: Arc<dyn ReplicationStore>,
    pub(super) listener: Arc<dyn ReplicationEventListener>,
    pub(super) group_memberships: SharedGroupMemberships,
    pub(super) state: RuntimeState,
    pub(super) terminal_fault: Option<TerminalRuntimeFault>,
}

impl ReplicationRuntimeComponent {
    pub(super) fn new(
        local_member: MemberIdentity,
        store: Arc<dyn ReplicationStore>,
        listener: Arc<dyn ReplicationEventListener>,
        group_memberships: SharedGroupMemberships,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            group_broadcast: RequiredPort::uninitialised(),
            reliable_delivery: RequiredPort::uninitialised(),
            local_member,
            store,
            listener,
            group_memberships,
            state: RuntimeState::default(),
            terminal_fault: None,
        }
    }

    fn ensure_running(&self) -> Result<(), ApiError> {
        if let Some(fault) = self.terminal_fault.clone() {
            return Err(ApiError::ApiExternal {
                source: Box::new(io::Error::other(format!(
                    "replication runtime terminated after inbound delivery failure: {fault}"
                ))),
            });
        }
        Ok(())
    }

    fn record_terminal_fault(&mut self, operation: &'static str, error: &InboundDeliveryError) {
        if self.terminal_fault.is_some() {
            return;
        }
        let fault = TerminalRuntimeFault {
            operation,
            message: error.to_string(),
        };
        error!(self.log(), "terminal runtime failure: {}", fault);
        self.terminal_fault = Some(fault);
    }

    fn missing_dataset_ids_for_publish(
        &self,
        group_id: GroupId,
        dataset_ids: &[DatasetId],
    ) -> Result<Vec<DatasetId>, PublishChangesError> {
        let hosted_group = self
            .state
            .groups
            .get(&group_id)
            .context(UnknownGroupSnafu { group_id })?;
        let mut missing_dataset_ids = Vec::new();
        for dataset_id in dataset_ids {
            if hosted_group.datasets.contains_key(dataset_id)
                || missing_dataset_ids.contains(dataset_id)
            {
                continue;
            }
            missing_dataset_ids.push(dataset_id.clone());
        }
        Ok(missing_dataset_ids)
    }

    fn missing_dataset_ids_for_inbound(
        &self,
        group_id: GroupId,
        dataset_ids: &[DatasetId],
    ) -> Result<Vec<DatasetId>, InboundDeliveryError> {
        let hosted_group = self
            .state
            .groups
            .get(&group_id)
            .context(UnknownHostedGroupSnafu { group_id })?;
        let mut missing_dataset_ids = Vec::new();
        for dataset_id in dataset_ids {
            if hosted_group.datasets.contains_key(dataset_id)
                || missing_dataset_ids.contains(dataset_id)
            {
                continue;
            }
            missing_dataset_ids.push(dataset_id.clone());
        }
        Ok(missing_dataset_ids)
    }

    fn create_group(&mut self, req: CreateGroupRequest) -> Result<GroupId, CreateGroupError> {
        if req.initial_state.is_some() {
            return InitialStateUnsupportedSnafu.fail();
        }

        let members =
            GroupMembers::from_ordered_members(req.members).context(InvalidMembersSnafu)?;
        if !members.contains(&self.local_member) {
            return LocalMemberMissingSnafu {
                local_member: self.local_member.clone(),
            }
            .fail();
        }

        let group_id = GroupId(Uuid::new_v4());
        self.install_group(group_id, members.clone())
            .context(InstallGroupSnafu)?;

        let bootstrap = RuntimeMessage::BootstrapGroup(BootstrapGroupMessage {
            group_id,
            members: members.ordered_members(),
        });
        // Temporary byte serialisation at the delivery-envelope boundary.
        // See flotsync-ylo for the payload/encryption redesign.
        let payload = bootstrap.encode_to_proto().encode_to_bytes();
        for recipient in members
            .ordered_members()
            .into_iter()
            .filter(|member| member != &self.local_member)
        {
            self.reliable_delivery.trigger(
                crate::delivery::contracts::ReliableDeliveryPortRequest::Submit(
                    ReliableDeliverySubmit {
                        envelope: ReliableMessageEnvelope {
                            header: ReliableMessageHeader {
                                sender: self.local_member.clone(),
                                recipient,
                                message_id: MessageId(Uuid::new_v4()),
                            },
                            payload: EncryptedPayload {
                                ciphertext: payload.clone(),
                            },
                            footer: placeholder_signed_footer(),
                        },
                    },
                ),
            );
        }
        Ok(group_id)
    }

    fn prepare_local_publish(
        &mut self,
        changes: Vec<RowMutation>,
        loaded_schemas: &HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<PreparedPublish, PublishChangesError> {
        let (group_id, _) = publish_scope(&changes)?;
        let hosted_group = self
            .state
            .groups
            .get_mut(&group_id)
            .context(UnknownGroupSnafu { group_id })?;

        let read_versions = hosted_group.version_vector.clone();
        let next_local_version = hosted_group
            .applied_version(hosted_group.local_member_index)
            .checked_add(1)
            .context(ExhaustedUpdateIdsSnafu { group_id })?;
        let update_id = UpdateId {
            version: next_local_version,
            node_index: hosted_group.local_member_index.as_u32(),
        };

        let mut dataset_order = Vec::new();
        let mut staged_datasets = HashMap::<DatasetId, HostedDataset>::new();
        let mut encoded_operations =
            HashMap::<DatasetId, Vec<flotsync_messages::datamodel::SchemaOperation>>::new();
        for mutation in changes {
            match mutation {
                RowMutation::Upsert { row_id, row } => {
                    if !dataset_order.contains(&row_id.dataset_id) {
                        dataset_order.push(row_id.dataset_id.clone());
                    }
                    let staged_dataset = staged_dataset_for_publish(
                        &mut staged_datasets,
                        hosted_group,
                        loaded_schemas,
                        &row_id.dataset_id,
                    )?;
                    if let Some(encoded_operation) =
                        apply_local_upsert(staged_dataset, &row_id, &row, update_id)?
                    {
                        encoded_operations
                            .entry(row_id.dataset_id.clone())
                            .or_default()
                            .push(encoded_operation);
                    }
                }
                RowMutation::Delete { row_id } => {
                    if !dataset_order.contains(&row_id.dataset_id) {
                        dataset_order.push(row_id.dataset_id.clone());
                    }
                    let staged_dataset = staged_dataset_for_publish(
                        &mut staged_datasets,
                        hosted_group,
                        loaded_schemas,
                        &row_id.dataset_id,
                    )?;
                    let encoded_operation = apply_local_delete(staged_dataset, &row_id, update_id)?;
                    encoded_operations
                        .entry(row_id.dataset_id.clone())
                        .or_default()
                        .push(encoded_operation);
                }
            }
        }

        ensure!(
            !encoded_operations.is_empty(),
            NoEffectiveChangesSnafu { group_id }
        );

        for (dataset_id, staged_dataset) in staged_datasets {
            hosted_group.datasets.insert(dataset_id, staged_dataset);
        }
        hosted_group
            .version_vector
            .increment_at(hosted_group.local_member_index.as_u32() as usize);

        let dataset_updates = dataset_order
            .into_iter()
            .filter_map(|dataset_id| {
                encoded_operations
                    .remove(&dataset_id)
                    .map(|operations| DatasetUpdateMessage {
                        dataset_id,
                        operations,
                    })
            })
            .collect();
        // Temporary byte serialisation at the delivery-envelope boundary.
        // See flotsync-ylo for the payload/encryption redesign.
        let payload = RuntimeMessage::UpdateBatch(UpdateBatchMessage {
            group_id,
            update_id,
            read_versions,
            dataset_updates,
        })
        .encode_to_proto()
        .encode_to_bytes();
        Ok(PreparedPublish {
            group_id,
            update_id,
            payload,
        })
    }

    fn handle_reliable_delivery(
        &mut self,
        deliver: ReliableDeliveryDeliver,
    ) -> Result<(), InboundDeliveryError> {
        let message = WireRuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
            .context(DecodeMessageSnafu)?;
        match message {
            WireRuntimeMessage::BootstrapGroup(message) => {
                let group_id = message.group_id;
                let members = GroupMembers::from_ordered_members(message.members)
                    .context(InvalidBootstrapMembersSnafu)?;
                if !members.contains(&self.local_member) {
                    return BootstrapMissingLocalMemberSnafu {
                        group_id,
                        local_member: self.local_member.clone(),
                    }
                    .fail();
                }
                self.install_group(group_id, members)
                    .context(InstallBootstrapGroupSnafu { group_id })?;
                deliver.processed.complete().map_err(|source| {
                    InboundDeliveryError::CompleteProcessedPromise { group_id, source }
                })?;
                Ok(())
            }
            WireRuntimeMessage::UpdateBatch(_) => UnexpectedReliableMessageSnafu.fail(),
        }
    }

    fn handle_group_delivery(
        &mut self,
        deliver: GroupBroadcastDeliver,
    ) -> Result<Handled, InboundDeliveryError> {
        let sender = deliver.envelope.header.sender.clone();
        let message = WireRuntimeMessage::decode_from_slice(&deliver.envelope.payload.ciphertext)
            .context(DecodeMessageSnafu)?;
        match message {
            WireRuntimeMessage::BootstrapGroup(_) => UnexpectedGroupMessageSnafu.fail(),
            WireRuntimeMessage::UpdateBatch(message) => {
                Ok(self.handle_update_batch(sender, message))
            }
        }
    }

    fn handle_update_batch(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
    ) -> Handled {
        let dataset_ids: Vec<_> = message
            .dataset_updates
            .iter()
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect();
        let missing_dataset_ids =
            match self.missing_dataset_ids_for_inbound(message.group_id, &dataset_ids) {
                Ok(missing_dataset_ids) => missing_dataset_ids,
                Err(error) => {
                    self.record_terminal_fault("group delivery", &error);
                    return Handled::Ok;
                }
            };
        let store = self.store.clone();
        let listener = self.listener.clone();
        Handled::block_on(self, move |mut async_self| async move {
            let loaded_schemas = load_inbound_schemas(store, missing_dataset_ids).await;
            let reply = loaded_schemas.and_then(|loaded_schemas| {
                async_self.apply_wire_update_batch_loaded(sender, message, loaded_schemas)
            });
            match reply {
                Ok(event_batches) => {
                    for row_changes in event_batches {
                        let notify_result = listener
                            .on_event(ReplicationEvent::DataChanged {
                                rows: Box::new(VecRowProvider::new(row_changes)),
                            })
                            .await;
                        if let Err(error) = notify_result {
                            async_self.record_terminal_fault(
                                "group delivery",
                                &InboundDeliveryError::NotifyListener { source: error },
                            );
                            return;
                        }
                    }
                }
                Err(error) => {
                    // Temporary conservative failure mode for the
                    // first replication slice. See flotsync-4x8
                    // for relaxing transient inbound ordering
                    // errors without terminating the runtime.
                    async_self.record_terminal_fault("group delivery", &error);
                }
            }
        })
    }

    fn apply_wire_update_batch_loaded(
        &mut self,
        sender: MemberIdentity,
        message: WireUpdateBatchMessage,
        loaded_schemas: HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<Vec<Vec<RowChange>>, InboundDeliveryError> {
        let group_id = message.group_id;
        let hosted_group = self
            .state
            .groups
            .get_mut(&group_id)
            .context(UnknownHostedGroupSnafu { group_id })?;
        let member_count =
            NonZeroUsize::new(hosted_group.members.len()).expect("hosted group must be non-empty");
        let message = message
            .into_runtime(member_count)
            .context(DecodeReadVersionsSnafu { group_id })?;
        self.apply_update_batch_loaded(sender, message, loaded_schemas)
    }

    fn apply_update_batch_loaded(
        &mut self,
        sender: MemberIdentity,
        message: UpdateBatchMessage,
        loaded_schemas: HashMap<DatasetId, Arc<Schema>>,
    ) -> Result<Vec<Vec<RowChange>>, InboundDeliveryError> {
        let hosted_group =
            self.state
                .groups
                .get_mut(&message.group_id)
                .context(UnknownHostedGroupSnafu {
                    group_id: message.group_id,
                })?;
        let expected_sender_index =
            hosted_group
                .members
                .member_index(&sender)
                .context(UpdateSenderNotInGroupSnafu {
                    group_id: message.group_id,
                    sender: sender.clone(),
                })?;
        ensure!(
            expected_sender_index.as_u32() == message.update_id.node_index,
            UpdateIdSenderMismatchSnafu {
                group_id: message.group_id,
                sender: sender.clone(),
                expected_index: expected_sender_index,
                actual_index: MemberIndex::new(message.update_id.node_index),
            }
        );
        ensure!(
            message.update_id.version > 0,
            InvalidUpdateVersionSnafu {
                group_id: message.group_id,
                update_id: message.update_id,
            }
        );
        if hosted_group.has_applied(message.update_id) {
            return Ok(Vec::new());
        }
        if !version_vector_covers(&hosted_group.version_vector, &message.read_versions)
            || hosted_group.expected_next_version(expected_sender_index) < message.update_id.version
        {
            hosted_group.buffer_update(PendingInboundUpdate {
                message,
                loaded_schemas,
            })?;
            return Ok(Vec::new());
        }

        let mut event_batches = Vec::new();
        let row_changes = apply_one_update_batch(hosted_group, message, &loaded_schemas)?;
        if !row_changes.is_empty() {
            event_batches.push(row_changes);
        }
        while let Some(action) = hosted_group.take_next_actionable_pending_update() {
            let pending_update = match action {
                PendingUpdateAction::DropDuplicate => {
                    continue;
                }
                PendingUpdateAction::Apply(pending_update) => pending_update,
            };
            let row_changes = apply_one_update_batch(
                hosted_group,
                pending_update.message,
                &pending_update.loaded_schemas,
            )?;
            if !row_changes.is_empty() {
                event_batches.push(row_changes);
            }
        }

        Ok(event_batches)
    }

    fn install_group(
        &mut self,
        group_id: GroupId,
        members: GroupMembers,
    ) -> Result<(), GroupInstallError> {
        if let Some(existing_group) = self.state.groups.get(&group_id) {
            if existing_group.members == members {
                return Ok(());
            }
            return ConflictingExistingGroupSnafu { group_id }.fail();
        }

        let hosted_group = HostedGroup::new(&self.local_member, members)?;
        self.state.groups.insert(group_id, hosted_group);
        self.group_memberships
            .replace(self.state.membership_snapshot());
        Ok(())
    }

    fn handle_publish_changes(
        &mut self,
        ask: Ask<Vec<RowMutation>, Result<PublishReceipt, ApiError>>,
    ) -> Handled {
        let (promise, changes) = ask.take();
        let preflight = self.ensure_running().and_then(|()| {
            let (group_id, dataset_ids) =
                publish_scope(&changes).boxed().context(ApiExternalSnafu)?;
            let missing_dataset_ids = self
                .missing_dataset_ids_for_publish(group_id, &dataset_ids)
                .boxed()
                .context(ApiExternalSnafu)?;
            Ok(missing_dataset_ids)
        });
        let missing_dataset_ids = match preflight {
            Ok(missing_dataset_ids) => missing_dataset_ids,
            Err(error) => {
                if promise.fulfil(Err(error)).is_err() {
                    debug!(self.log(), "dropping publish_changes reply");
                }
                return Handled::Ok;
            }
        };
        let store = self.store.clone();
        Handled::block_on(self, move |mut async_self| async move {
            let loaded_schemas = load_publish_schemas(store, missing_dataset_ids).await;
            let reply = (|| -> Result<PublishReceipt, ApiError> {
                let loaded_schemas = loaded_schemas.boxed().context(ApiExternalSnafu)?;
                let prepared_publish = async_self
                    .prepare_local_publish(changes, &loaded_schemas)
                    .boxed()
                    .context(ApiExternalSnafu)?;
                let local_member = async_self.local_member.clone();
                async_self.group_broadcast.trigger(
                    crate::delivery::contracts::GroupBroadcastPortRequest::Submit(
                        GroupBroadcastSubmit {
                            delivery_class: DeliveryClass::BestEffort,
                            envelope: GroupMessageEnvelope {
                                header: GroupMessageHeader {
                                    group_id: prepared_publish.group_id,
                                    sender: local_member,
                                    message_id: MessageId(Uuid::new_v4()),
                                },
                                payload: EncryptedPayload {
                                    ciphertext: prepared_publish.payload,
                                },
                                footer: placeholder_signed_footer(),
                            },
                            suppress_self_delivery: true,
                        },
                    ),
                );
                Ok(PublishReceipt {
                    update_id: prepared_publish.update_id,
                })
            })();
            if promise.fulfil(reply).is_err() {
                debug!(async_self.log(), "dropping publish_changes reply");
            }
        })
    }

    fn handle_create_group(
        &mut self,
        ask: Ask<CreateGroupRequest, Result<GroupId, ApiError>>,
    ) -> Handled {
        let (promise, req) = ask.take();
        let reply = self
            .ensure_running()
            .and_then(|()| self.create_group(req).boxed().context(ApiExternalSnafu));
        if promise.fulfil(reply).is_err() {
            debug!(self.log(), "dropping create_group reply");
        }
        Handled::Ok
    }

    fn handle_change_group_membership(
        &mut self,
        ask: Ask<ChangeGroupMembershipRequest, Result<GroupMigration, ApiError>>,
    ) -> Handled {
        let (promise, req) = ask.take();
        let _ = req;
        let reply = self
            .ensure_running()
            .and_then(|()| Err(unavailable_api("change_group_membership")));
        if promise.fulfil(reply).is_err() {
            debug!(self.log(), "dropping change_group_membership reply");
        }
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_install_group(
        &mut self,
        ask: Ask<TestInstallGroup, Result<(), GroupInstallError>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        let reply = self.install_group(request.group_id, request.members);
        let _ = promise.fulfil(reply);
        Handled::Ok
    }

    #[cfg(test)]
    fn handle_test_apply_update_batch(
        &mut self,
        ask: Ask<TestApplyUpdateBatch, Result<(), InboundDeliveryError>>,
    ) -> Handled {
        let (promise, request) = ask.take();
        let dataset_ids: Vec<_> = request
            .message
            .dataset_updates
            .iter()
            .map(|dataset_update| dataset_update.dataset_id.clone())
            .collect();
        let missing_dataset_ids =
            match self.missing_dataset_ids_for_inbound(request.message.group_id, &dataset_ids) {
                Ok(missing_dataset_ids) => missing_dataset_ids,
                Err(error) => {
                    let _ = promise.fulfil(Err(error));
                    return Handled::Ok;
                }
            };
        let store = self.store.clone();
        let listener = self.listener.clone();
        Handled::block_on(self, move |mut async_self| async move {
            let loaded_schemas = load_inbound_schemas(store, missing_dataset_ids).await;
            let reply = match loaded_schemas {
                Ok(loaded_schemas) => {
                    match async_self.apply_update_batch_loaded(
                        request.sender,
                        request.message,
                        loaded_schemas,
                    ) {
                        Ok(event_batches) => {
                            let mut notify_error = None;
                            for row_changes in event_batches {
                                let notify_result = listener
                                    .on_event(ReplicationEvent::DataChanged {
                                        rows: Box::new(VecRowProvider::new(row_changes)),
                                    })
                                    .await;
                                if let Err(error) = notify_result {
                                    notify_error = Some(InboundDeliveryError::NotifyListener {
                                        source: error,
                                    });
                                    break;
                                }
                            }
                            match notify_error {
                                Some(error) => Err(error),
                                None => Ok(()),
                            }
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            };
            let _ = promise.fulfil(reply);
        })
    }
}

ignore_lifecycle!(ReplicationRuntimeComponent);

impl Require<crate::delivery::contracts::ReliableDeliveryPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: ReliableDeliveryPortIndication) -> Handled {
        if self.terminal_fault.is_some() {
            return Handled::Ok;
        }
        match indication {
            ReliableDeliveryPortIndication::Deliver(deliver) => {
                if let Err(error) = self.handle_reliable_delivery(deliver) {
                    self.record_terminal_fault("reliable delivery", &error);
                }
                Handled::Ok
            }
        }
    }
}

impl Require<crate::delivery::contracts::GroupBroadcastPort> for ReplicationRuntimeComponent {
    fn handle(&mut self, indication: GroupBroadcastPortIndication) -> Handled {
        if self.terminal_fault.is_some() {
            return Handled::Ok;
        }
        match indication {
            GroupBroadcastPortIndication::Deliver(deliver) => {
                match self.handle_group_delivery(deliver) {
                    Ok(handled) => handled,
                    Err(error) => {
                        self.record_terminal_fault("group delivery", &error);
                        Handled::Ok
                    }
                }
            }
        }
    }
}

impl LocalActor for ReplicationRuntimeComponent {
    type Message = ReplicationRuntimeMessage;

    fn receive(&mut self, msg: Self::Message) -> Handled {
        match msg {
            ReplicationRuntimeMessage::PublishChanges(ask) => self.handle_publish_changes(ask),
            ReplicationRuntimeMessage::CreateGroup(ask) => self.handle_create_group(ask),
            ReplicationRuntimeMessage::ChangeGroupMembership(ask) => {
                self.handle_change_group_membership(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::InstallGroupForTest(ask) => {
                self.handle_test_install_group(ask)
            }
            #[cfg(test)]
            ReplicationRuntimeMessage::ApplyUpdateBatchForTest(ask) => {
                self.handle_test_apply_update_batch(ask)
            }
        }
    }
}

impl_local_actor!(ReplicationRuntimeComponent);

fn unavailable_api(operation: &'static str) -> ApiError {
    ApiError::ApiExternal {
        source: Box::new(io::Error::other(format!(
            "replication runtime host is ready, but {operation} is not implemented yet"
        ))),
    }
}

fn placeholder_signed_footer() -> SignedEnvelopeFooter {
    SignedEnvelopeFooter {
        signature: DetachedSignature {
            scheme: SignatureScheme::Ed25519,
            bytes: bytes::Bytes::from_static(b"runtime-placeholder-signature"),
        },
    }
}
