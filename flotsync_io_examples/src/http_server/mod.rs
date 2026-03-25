use crate::{
    app::ExampleRuntime,
    support::{OutcomePromise, complete_outcome, new_outcome_promise, wait_for_component_outcome},
};
use bytes::Buf;
use clap::Parser;
use flotsync_io::prelude::{
    BoundedCollector,
    CloseReason,
    CollectUntil,
    IoBridgeHandle,
    IoPayload,
    OpenTcpListener,
    PayloadWriter,
    PendingTcpSession,
    TcpListenerEvent,
    TcpListenerRef,
    TcpListenerRequest,
    TcpSessionEvent,
    TcpSessionRef,
    TransmissionId,
};
use http::{Method, StatusCode};
use itoa::Buffer as ItoaBuffer;
use kompact::prelude::*;
use snafu::{FromString, Whatever, prelude::*};
use std::{
    io::{self, Read},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

/// Result type used by the HTTP server example binary.
pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type Error = Whatever;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 64 * 1024;
const MAX_REQUEST_HEADERS: usize = 32;
const ROOT_BODY: &[u8] = b"hello from flotsync_io\n";

/// Command-line arguments for the minimal HTTP server example.
#[derive(Debug, Parser)]
#[command(
    name = "http_server",
    version,
    about = "Minimal HTTP/1.1 server example on top of flotsync_io"
)]
pub struct HttpServerArgs {
    /// Local address to bind the TCP listener to.
    #[arg(long)]
    bind: SocketAddr,
}

/// Runs the HTTP server example until stdin reaches EOF or the listener fails.
pub fn run(args: HttpServerArgs) -> Result<()> {
    let runtime = ExampleRuntime::setup()?;
    let (outcome_promise, outcome_future) = new_outcome_promise();
    let bridge_handle = runtime.bridge_handle().clone();
    let http_component = runtime
        .system()
        .create(move || HttpListenerComponent::new(bridge_handle, args.bind, outcome_promise));
    start_component(&runtime, &http_component)?;
    install_shutdown_input_source(http_component.actor_ref());
    let run_result = wait_for_component_outcome(&http_component, outcome_future, "http_server");
    let shutdown_result = runtime.shutdown();
    run_result?;
    shutdown_result?;
    Ok(())
}

fn start_component<C>(runtime: &ExampleRuntime, component: &Arc<Component<C>>) -> Result<()>
where
    C: ComponentDefinition + Actor + Sized + 'static,
{
    let start_driver = runtime.system().start_notify(runtime.driver_component());
    whatever!(
        start_driver.wait_timeout(CONTROL_TIMEOUT),
        "timed out waiting for the shared IoDriverComponent to start"
    );

    let start_bridge = runtime.system().start_notify(runtime.bridge_component());
    whatever!(
        start_bridge.wait_timeout(CONTROL_TIMEOUT),
        "timed out waiting for the shared IoBridge to start"
    );

    let start_component = runtime.system().start_notify(component);
    whatever!(
        start_component.wait_timeout(CONTROL_TIMEOUT),
        "timed out waiting for the HTTP server component to start"
    );

    Ok(())
}

/// Requests listener shutdown once stdin reaches EOF.
///
/// This is intentionally EOF-driven rather than line-driven, so pressing Enter on an attached
/// terminal does nothing; use `Ctrl-D` on Unix to close stdin.
fn install_shutdown_input_source(actor_ref: ActorRef<HttpListenerMessage>) {
    std::thread::spawn(move || {
        let mut stdin = io::stdin();
        let mut buffer = [0_u8; 1024];
        loop {
            match stdin.read(&mut buffer) {
                Ok(0) => {
                    actor_ref.tell(HttpListenerMessage::ShutdownRequested);
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    log::warn!("stdin read failed for http_server: {error}");
                    actor_ref.tell(HttpListenerMessage::ShutdownRequested);
                    return;
                }
            }
        }
    });
}

/// Listener lifecycle for the top-level HTTP example component.
#[derive(Clone, Debug)]
enum HttpListenerState {
    Opening,
    Ready(TcpListenerRef),
    Closing(TcpListenerRef),
    Closed,
}

impl HttpListenerState {
    fn mark_ready(&mut self, listener: TcpListenerRef) {
        *self = Self::Ready(listener);
    }

    fn request_close(&mut self) -> bool {
        match std::mem::replace(self, Self::Closed) {
            Self::Ready(listener) => {
                listener.tell(TcpListenerRequest::Close);
                *self = Self::Closing(listener);
                true
            }
            Self::Closing(listener) => {
                *self = Self::Closing(listener);
                false
            }
            Self::Opening => {
                *self = Self::Opening;
                false
            }
            Self::Closed => false,
        }
    }

    fn mark_closed(&mut self) {
        *self = Self::Closed;
    }
}

/// Local mailbox for the top-level HTTP listener component.
#[derive(Debug)]
enum HttpListenerMessage {
    ListenerEvent(TcpListenerEvent),
    ShutdownRequested,
}

/// Top-level listener that accepts inbound TCP sessions and spawns one HTTP connection handler
/// component per accepted socket.
#[derive(ComponentDefinition)]
struct HttpListenerComponent {
    ctx: ComponentContext<Self>,
    bridge_handle: IoBridgeHandle,
    bind_addr: SocketAddr,
    state: HttpListenerState,
    shutdown_requested: bool,
    outcome: Option<OutcomePromise>,
}

impl HttpListenerComponent {
    fn new(bridge_handle: IoBridgeHandle, bind_addr: SocketAddr, outcome: OutcomePromise) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            bridge_handle,
            bind_addr,
            state: HttpListenerState::Opening,
            shutdown_requested: false,
            outcome: Some(outcome),
        }
    }

    fn handle_listener_event(&mut self, event: TcpListenerEvent) -> Handled {
        match event {
            TcpListenerEvent::Incoming { peer_addr, pending } => {
                self.spawn_connection(peer_addr, pending);
                Handled::Ok
            }
            TcpListenerEvent::Closed => {
                self.state.mark_closed();
                self.terminate_success();
                Handled::DieNow
            }
        }
    }

    fn handle_shutdown_requested(&mut self) -> Handled {
        self.shutdown_requested = true;
        if self.state.request_close() {
            return Handled::Ok;
        }
        if matches!(self.state, HttpListenerState::Closed) {
            self.terminate_success();
            return Handled::DieNow;
        }
        Handled::Ok
    }

    fn spawn_connection(&mut self, peer_addr: SocketAddr, pending: PendingTcpSession) {
        let connection_component = self
            .ctx
            .system()
            .create(|| HttpConnectionComponent::new(peer_addr, pending));
        self.ctx.system().start(&connection_component);
    }

    fn terminate_success(&mut self) {
        complete_outcome(&mut self.outcome, Ok(()));
    }

    fn fail_and_die(&mut self, message: String) -> Handled {
        complete_outcome(&mut self.outcome, Err(Whatever::without_source(message)));
        Handled::DieNow
    }
}

impl ComponentLifecycle for HttpListenerComponent {
    fn on_start(&mut self) -> Handled {
        let open = self.bridge_handle.open_tcp_listener(OpenTcpListener {
            local_addr: self.bind_addr,
            incoming_to: self
                .actor_ref()
                .recipient_with(HttpListenerMessage::ListenerEvent),
        });
        Handled::block_on(self, move |mut async_self| async move {
            let listener_reply = match open.await {
                Ok(reply) => reply,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "HTTP listener open reply dropped before completion: {error}"
                    ));
                }
            };
            match listener_reply {
                Ok(opened) => {
                    log::info!("HTTP listening on {}", opened.local_addr);
                    async_self.state.mark_ready(opened.listener);
                    if async_self.shutdown_requested {
                        return async_self.handle_shutdown_requested();
                    }
                    Handled::Ok
                }
                Err(error) => {
                    async_self.fail_and_die(format!("failed to open HTTP listener: {error:?}"))
                }
            }
        })
    }
}

impl Actor for HttpListenerComponent {
    type Message = HttpListenerMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            HttpListenerMessage::ListenerEvent(event) => self.handle_listener_event(event),
            HttpListenerMessage::ShutdownRequested => self.handle_shutdown_requested(),
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("HTTP listener example does not use network actor messages")
    }
}

/// Session lifecycle for one accepted HTTP connection.
#[derive(Clone, Debug)]
enum HttpSessionState {
    Accepting,
    Ready(TcpSessionRef),
    Closing(TcpSessionRef),
    Closed,
}

impl HttpSessionState {
    fn session(&self) -> Option<&TcpSessionRef> {
        match self {
            Self::Ready(session) | Self::Closing(session) => Some(session),
            Self::Accepting | Self::Closed => None,
        }
    }

    fn mark_ready(&mut self, session: TcpSessionRef) {
        *self = Self::Ready(session);
    }

    fn request_close(&mut self, abort: bool) -> bool {
        match std::mem::replace(self, Self::Closed) {
            Self::Ready(session) => {
                session.close(abort);
                *self = Self::Closing(session);
                true
            }
            Self::Closing(session) => {
                *self = Self::Closing(session);
                false
            }
            Self::Accepting => {
                *self = Self::Accepting;
                false
            }
            Self::Closed => false,
        }
    }

    fn mark_closed(&mut self) {
        *self = Self::Closed;
    }
}

/// Route selected for one successfully parsed request.
#[derive(Clone, Copy, Debug)]
enum HttpRoute {
    RootGet,
    RootHead,
    EchoPost,
}

/// Parsed request metadata needed to finish Proposal A handling.
#[derive(Clone, Copy, Debug)]
struct ParsedRequest {
    route: HttpRoute,
    expected_body_len: usize,
}

/// Response template built once request parsing and validation completes.
#[derive(Clone, Debug)]
struct HttpResponseSpec {
    status: StatusCode,
    content_type: &'static str,
    body: HttpResponseBody,
    content_length: usize,
    include_body: bool,
    allow: Option<&'static str>,
}

/// Body source written into the response once headers are ready.
#[derive(Clone, Debug)]
enum HttpResponseBody {
    Static(&'static [u8]),
    EchoedRequestBody,
}

/// One retained body range inside an inbound transport payload.
#[derive(Clone, Copy, Debug)]
struct PayloadSlice {
    payload_index: usize,
    offset: usize,
    len: usize,
}

/// Result of consuming one inbound payload or payload chunk.
enum PayloadConsumeOutcome {
    NeedMore,
    Respond(HttpResponseSpec),
}

/// Walks one payload cursor until the HTTP parser either needs more bytes, chooses a response, or
/// encounters a terminal transport error.
///
/// Proposal A serves at most one response per connection. Once parsing has committed to a
/// response, later bytes in the same payload are ignored and the connection is expected to close
/// after the response is sent.
fn consume_payload_cursor<B, F>(mut cursor: B, mut consume_chunk: F) -> PayloadConsumeOutcome
where
    B: Buf,
    F: FnMut(usize, &[u8]) -> PayloadConsumeOutcome,
{
    let mut payload_offset = 0;
    while cursor.has_remaining() {
        let chunk = cursor.chunk();
        let chunk_len = chunk.len();
        debug_assert!(
            chunk_len > 0,
            "Buf::chunk() must not return an empty slice while bytes remain"
        );
        let outcome = consume_chunk(payload_offset, chunk);
        cursor.advance(chunk_len);
        payload_offset += chunk_len;
        match &outcome {
            PayloadConsumeOutcome::NeedMore => {}
            PayloadConsumeOutcome::Respond(_) => {
                return outcome;
            }
        }
    }
    PayloadConsumeOutcome::NeedMore
}

/// Per-connection HTTP handler.
///
/// The component intentionally retains every inbound `IoPayload` until it has sent a response and
/// observed the terminal close. Header bytes still accumulate into one bounded parse buffer because
/// `httparse` expects contiguous request heads, but request-body bytes stay as retained payload
/// slices so the example can echo them back without flattening them first.
#[derive(ComponentDefinition)]
struct HttpConnectionComponent {
    ctx: ComponentContext<Self>,
    peer_addr: SocketAddr,
    pending: Option<PendingTcpSession>,
    session_state: HttpSessionState,
    retained_input: Vec<IoPayload>,
    header_collector: BoundedCollector,
    request: Option<ParsedRequest>,
    body_slices: Vec<PayloadSlice>,
    received_body_len: usize,
    response_started: bool,
    next_transmission_id: TransmissionId,
}

impl HttpConnectionComponent {
    fn new(peer_addr: SocketAddr, pending: PendingTcpSession) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            peer_addr,
            pending: Some(pending),
            session_state: HttpSessionState::Accepting,
            retained_input: Vec::new(),
            header_collector: BoundedCollector::new(MAX_HEADER_BYTES),
            request: None,
            body_slices: Vec::new(),
            received_body_len: 0,
            response_started: false,
            next_transmission_id: TransmissionId::ONE,
        }
    }

    fn handle_session_event(&mut self, event: TcpSessionEvent) -> Handled {
        match event {
            TcpSessionEvent::Received { payload } => self.handle_received_payload(payload),
            TcpSessionEvent::SendAck { transmission_id } => {
                log::debug!(
                    "HTTP response send {} drained for {}",
                    transmission_id,
                    self.peer_addr
                );
                Handled::Ok
            }
            TcpSessionEvent::SendNack {
                transmission_id,
                reason,
            } => self.fail_and_die(format!(
                "HTTP response send {} failed for {}: {:?}",
                transmission_id, self.peer_addr, reason
            )),
            TcpSessionEvent::ReadSuspended => {
                log::debug!("HTTP session read suspended for {}", self.peer_addr);
                Handled::Ok
            }
            TcpSessionEvent::ReadResumed => {
                log::debug!("HTTP session read resumed for {}", self.peer_addr);
                Handled::Ok
            }
            TcpSessionEvent::WriteSuspended => {
                log::debug!("HTTP session write suspended for {}", self.peer_addr);
                Handled::Ok
            }
            TcpSessionEvent::WriteResumed => {
                log::debug!("HTTP session write resumed for {}", self.peer_addr);
                Handled::Ok
            }
            TcpSessionEvent::Closed { reason } => {
                self.session_state.mark_closed();
                if self.response_started && reason == CloseReason::Graceful {
                    return Handled::DieNow;
                }
                self.fail_and_die(format!(
                    "HTTP session for {} closed before a response completed: {:?}",
                    self.peer_addr, reason
                ))
            }
        }
    }

    fn handle_received_payload(&mut self, payload: IoPayload) -> Handled {
        if self.response_started {
            return self.fail_and_die(format!(
                "HTTP session for {} received bytes after committing the single supported response",
                self.peer_addr
            ));
        }

        let payload_index = self.retained_input.len();
        match self.consume_payload(payload_index, &payload) {
            PayloadConsumeOutcome::NeedMore => {
                self.retained_input.push(payload);
            }
            PayloadConsumeOutcome::Respond(response) => {
                self.retained_input.push(payload);
                self.start_response(response);
            }
        }
        Handled::Ok
    }

    fn consume_payload(
        &mut self,
        payload_index: usize,
        payload: &IoPayload,
    ) -> PayloadConsumeOutcome {
        consume_payload_cursor(payload.cursor(), |payload_offset, chunk| {
            self.consume_payload_chunk(payload_index, payload_offset, chunk)
        })
    }

    fn consume_payload_chunk(
        &mut self,
        payload_index: usize,
        payload_offset: usize,
        bytes: &[u8],
    ) -> PayloadConsumeOutcome {
        let mut body_offset = payload_offset;
        let mut body_bytes = bytes;

        if self.request.is_none() {
            match self.header_collector.push_until(bytes, b"\r\n\r\n") {
                CollectUntil::NeedMore => return PayloadConsumeOutcome::NeedMore,
                CollectUntil::LimitExceeded => {
                    return PayloadConsumeOutcome::Respond(bad_request_response(
                        "request headers too large\n",
                    ));
                }
                CollectUntil::Complete {
                    frame,
                    trailing_offset,
                } => {
                    let request = match parse_complete_request_head(frame) {
                        Ok(request) => request,
                        Err(response) => {
                            self.header_collector.clear();
                            return PayloadConsumeOutcome::Respond(response);
                        }
                    };
                    self.header_collector.clear();
                    self.request = Some(request);
                    body_offset += trailing_offset;
                    body_bytes = &bytes[trailing_offset..];
                }
            }
        }

        self.consume_request_body_bytes(payload_index, body_offset, body_bytes)
    }

    fn consume_request_body_bytes(
        &mut self,
        payload_index: usize,
        payload_offset: usize,
        bytes: &[u8],
    ) -> PayloadConsumeOutcome {
        let Some(request) = self.request else {
            return PayloadConsumeOutcome::NeedMore;
        };

        let remaining_body_bytes = request
            .expected_body_len
            .saturating_sub(self.received_body_len);
        let to_take = remaining_body_bytes.min(bytes.len());
        if to_take > 0 {
            self.append_body_slice(PayloadSlice {
                payload_index,
                offset: payload_offset,
                len: to_take,
            });
            self.received_body_len += to_take;
        }

        if self.received_body_len < request.expected_body_len {
            return PayloadConsumeOutcome::NeedMore;
        }
        PayloadConsumeOutcome::Respond(build_response(request))
    }

    fn append_body_slice(&mut self, next: PayloadSlice) {
        if let Some(previous) = self.body_slices.last_mut() {
            let previous_end = previous.offset + previous.len;
            if previous.payload_index == next.payload_index && previous_end == next.offset {
                previous.len += next.len;
                return;
            }
        }
        self.body_slices.push(next);
    }

    fn start_response(&mut self, response: HttpResponseSpec) {
        let Some(session) = self.session_state.session().cloned() else {
            let _ = self.fail_and_die(format!(
                "HTTP session for {} became unavailable before sending a response",
                self.peer_addr
            ));
            return;
        };
        let mut encode_plan = ResponseEncodePlan {
            response,
            retained_input: std::mem::take(&mut self.retained_input)
                .into_iter()
                .map(Some)
                .collect(),
            body_slices: std::mem::take(&mut self.body_slices),
        };
        let transmission_id = self.next_transmission_id.take_next();
        let peer_addr = self.peer_addr;
        let hint_bytes = Some(reserved_response_header_capacity(&encode_plan.response));
        self.response_started = true;
        self.spawn_local(move |mut async_self| async move {
            let send_result = session
                .send_and_close_with(transmission_id, hint_bytes, async |writer| {
                    write_response_head(writer, &encode_plan.response).await?;
                    match encode_plan.response.body {
                        HttpResponseBody::Static(body) if encode_plan.response.include_body => {
                            writer.splice_static(body).await?;
                        }
                        HttpResponseBody::EchoedRequestBody
                            if encode_plan.response.include_body =>
                        {
                            writer
                                .adopt_payload(build_echo_body_payload(&mut encode_plan)?)
                                .await?;
                        }
                        HttpResponseBody::Static(_) | HttpResponseBody::EchoedRequestBody => {}
                    }
                    Ok(())
                })
                .await;
            if let Err(error) = send_result {
                return async_self.fail_and_die(format!(
                    "failed to encode HTTP response for {peer_addr}: {error}",
                ));
            }
            Handled::Ok
        });
    }

    fn fail_and_die(&mut self, message: String) -> Handled {
        self.session_state.request_close(true);
        log::error!("{message}");
        Handled::DieNow
    }
}

impl ComponentLifecycle for HttpConnectionComponent {
    fn on_start(&mut self) -> Handled {
        let pending = self
            .pending
            .take()
            .expect("HTTP connection component must start with a pending session");
        Handled::block_on(self, move |mut async_self| async move {
            let accept_reply = match pending.accept(async_self.actor_ref()).await {
                Ok(reply) => reply,
                Err(error) => {
                    return async_self.fail_and_die(format!(
                        "HTTP pending-session accept reply dropped before completion: {error}"
                    ));
                }
            };
            match accept_reply {
                Ok(session) => {
                    async_self.session_state.mark_ready(session);
                    Handled::Ok
                }
                Err(error) => {
                    let peer_addr = async_self.peer_addr;
                    async_self.fail_and_die(format!(
                        "failed to accept inbound HTTP session from {peer_addr}: {error}",
                    ))
                }
            }
        })
    }
}

impl Actor for HttpConnectionComponent {
    type Message = TcpSessionEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.handle_session_event(msg)
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unimplemented!("HTTP connection example does not use network actor messages")
    }
}

type HttpParseResult<T> = std::result::Result<T, HttpResponseSpec>;

/// Response data captured before the async send path reserves pooled egress memory.
#[derive(Clone, Debug)]
struct ResponseEncodePlan {
    response: HttpResponseSpec,
    retained_input: Vec<Option<IoPayload>>,
    body_slices: Vec<PayloadSlice>,
}

/// Parses a complete HTTP request head after `\r\n\r\n` has already been found.
fn parse_complete_request_head(buffer: &[u8]) -> HttpParseResult<ParsedRequest> {
    let mut headers = [httparse::EMPTY_HEADER; MAX_REQUEST_HEADERS];
    let mut request = httparse::Request::new(&mut headers);
    match request.parse(buffer) {
        Ok(httparse::Status::Complete(header_len)) => {
            if header_len != buffer.len() {
                unreachable!("complete header parser returned trailing bytes after terminator");
            }
            parse_complete_request(&request)
        }
        Ok(httparse::Status::Partial) => {
            unreachable!("header terminator search passed an incomplete request head")
        }
        Err(_) => Err(bad_request_response("malformed HTTP request\n")),
    }
}

fn parse_complete_request(request: &httparse::Request<'_, '_>) -> HttpParseResult<ParsedRequest> {
    let method = request
        .method
        .ok_or_else(|| bad_request_response("missing HTTP method\n"))
        .and_then(parse_method)?;
    let target = request
        .path
        .ok_or_else(|| bad_request_response("missing request target\n"))?;
    let version = request
        .version
        .ok_or_else(|| bad_request_response("missing HTTP version\n"))?;
    if version != 1 {
        return Err(simple_text_response(
            StatusCode::HTTP_VERSION_NOT_SUPPORTED,
            "http version not supported\n",
            Some("text/plain; charset=utf-8"),
            None,
            true,
        ));
    }
    if !target.starts_with('/') {
        return Err(bad_request_response(
            "only origin-form request targets are supported\n",
        ));
    }

    let mut host_count = 0_usize;
    let mut content_length: Option<usize> = None;
    for header in request.headers.iter() {
        if header.name.eq_ignore_ascii_case("host") {
            host_count += 1;
        } else if header.name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(bad_request_response(
                    "multiple Content-Length headers are not supported\n",
                ));
            }
            let value = std::str::from_utf8(header.value)
                .map_err(|_| bad_request_response("Content-Length must be valid ASCII digits\n"))?;
            let parsed = value.trim().parse::<usize>().map_err(|_| {
                bad_request_response("Content-Length must be a non-negative integer\n")
            })?;
            if parsed > MAX_REQUEST_BODY_BYTES {
                return Err(bad_request_response("request body too large\n"));
            }
            content_length = Some(parsed);
        } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(bad_request_response(
                "Transfer-Encoding is not supported by this example\n",
            ));
        }
    }

    if host_count != 1 {
        return Err(bad_request_response(
            "HTTP/1.1 requests must contain exactly one Host header\n",
        ));
    }

    let route = match (method, target) {
        (Method::GET, "/") => HttpRoute::RootGet,
        (Method::HEAD, "/") => HttpRoute::RootHead,
        (Method::POST, "/echo") => HttpRoute::EchoPost,
        _ if target == "/" => return Err(method_not_allowed_response("GET, HEAD")),
        _ if target == "/echo" => return Err(method_not_allowed_response("POST")),
        _ => {
            return Err(simple_text_response(
                StatusCode::NOT_FOUND,
                "not found\n",
                Some("text/plain; charset=utf-8"),
                None,
                true,
            ));
        }
    };

    let expected_body_len = match route {
        HttpRoute::RootGet | HttpRoute::RootHead => {
            if content_length.unwrap_or(0) != 0 {
                return Err(bad_request_response(
                    "this example does not accept request bodies on /\n",
                ));
            }
            0
        }
        HttpRoute::EchoPost => content_length
            .ok_or_else(|| bad_request_response("POST /echo requires Content-Length\n"))?,
    };

    Ok(ParsedRequest {
        route,
        expected_body_len,
    })
}

fn parse_method(method: &str) -> HttpParseResult<Method> {
    Method::from_bytes(method.as_bytes())
        .map_err(|_| bad_request_response("invalid HTTP method token\n"))
}

fn build_response(request: ParsedRequest) -> HttpResponseSpec {
    match request.route {
        HttpRoute::RootGet => HttpResponseSpec {
            status: StatusCode::OK,
            content_type: "text/plain; charset=utf-8",
            body: HttpResponseBody::Static(ROOT_BODY),
            content_length: ROOT_BODY.len(),
            include_body: true,
            allow: None,
        },
        HttpRoute::RootHead => HttpResponseSpec {
            status: StatusCode::OK,
            content_type: "text/plain; charset=utf-8",
            body: HttpResponseBody::Static(ROOT_BODY),
            content_length: ROOT_BODY.len(),
            include_body: false,
            allow: None,
        },
        HttpRoute::EchoPost => HttpResponseSpec {
            status: StatusCode::OK,
            content_type: "application/octet-stream",
            body: HttpResponseBody::EchoedRequestBody,
            content_length: request.expected_body_len,
            include_body: true,
            allow: None,
        },
    }
}

fn bad_request_response(body: &'static str) -> HttpResponseSpec {
    simple_text_response(
        StatusCode::BAD_REQUEST,
        body,
        Some("text/plain; charset=utf-8"),
        None,
        true,
    )
}

fn method_not_allowed_response(allow: &'static str) -> HttpResponseSpec {
    simple_text_response(
        StatusCode::METHOD_NOT_ALLOWED,
        "method not allowed\n",
        Some("text/plain; charset=utf-8"),
        Some(allow),
        true,
    )
}

fn simple_text_response(
    status: StatusCode,
    body: &'static str,
    content_type: Option<&'static str>,
    allow: Option<&'static str>,
    include_body: bool,
) -> HttpResponseSpec {
    HttpResponseSpec {
        status,
        content_type: content_type.unwrap_or("text/plain; charset=utf-8"),
        body: HttpResponseBody::Static(body.as_bytes()),
        content_length: body.len(),
        include_body,
        allow,
    }
}

const BASE_RESPONSE_HEADER_RESERVATION: usize =
    b"HTTP/1.1 999 HTTP Version Not Supported\r\nConnection: close\r\nContent-Length: \r\nContent-Type: \r\n\r\n"
        .len();
const MAX_CONTENT_LENGTH_DIGITS: usize = 20;
const ALLOW_HEADER_RESERVATION: usize = b"Allow: \r\n".len();

/// Returns a conservative pooled-capacity reservation for one encoded HTTP response.
///
/// Proposal A responses are tiny, so readability is more important here than exact byte math. This
/// is only a hint for the async writer's initial reservation, so a small over-estimate is fine.
fn reserved_response_header_capacity(response: &HttpResponseSpec) -> usize {
    let mut reserved =
        BASE_RESPONSE_HEADER_RESERVATION + MAX_CONTENT_LENGTH_DIGITS + response.content_type.len();
    if let Some(allow) = response.allow {
        reserved += ALLOW_HEADER_RESERVATION + allow.len();
    }
    reserved
}

async fn write_response_head(
    writer: &mut impl PayloadWriter,
    response: &HttpResponseSpec,
) -> flotsync_io::errors::Result<()> {
    let mut content_length_buffer = ItoaBuffer::new();
    let content_length_text = content_length_buffer.format(response.content_length);
    let reason = response.status.canonical_reason().unwrap_or("Unknown");

    writer.write_slice(b"HTTP/1.1 ").await?;
    writer
        .write_slice(response.status.as_str().as_bytes())
        .await?;
    writer.write_slice(b" ").await?;
    writer.write_slice(reason.as_bytes()).await?;
    writer
        .write_slice(b"\r\nConnection: close\r\nContent-Length: ")
        .await?;
    writer.write_slice(content_length_text.as_bytes()).await?;
    writer.write_slice(b"\r\nContent-Type: ").await?;
    writer.write_slice(response.content_type.as_bytes()).await?;
    writer.write_slice(b"\r\n").await?;
    if let Some(allow) = response.allow {
        writer.write_slice(b"Allow: ").await?;
        writer.write_slice(allow.as_bytes()).await?;
        writer.write_slice(b"\r\n").await?;
    }
    writer.write_slice(b"\r\n").await?;
    Ok(())
}

fn build_echo_body_payload(
    plan: &mut ResponseEncodePlan,
) -> flotsync_io::errors::Result<IoPayload> {
    let mut parts = Vec::with_capacity(plan.body_slices.len());
    for slice in &plan.body_slices {
        let payload = plan.retained_input[slice.payload_index]
            .take()
            .unwrap_or_else(|| {
                panic!(
                    "echo response reused retained payload {} unexpectedly",
                    slice.payload_index
                )
            });
        let payload = payload
            .try_slice(slice.offset, slice.len)
            .unwrap_or_else(|| {
                panic!(
                    "echo response slice {}..{} fell outside retained payload {}",
                    slice.offset,
                    slice.offset + slice.len,
                    slice.payload_index
                )
            });
        parts.push(payload);
    }
    Ok(IoPayload::chain(parts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flotsync_io::prelude::{IoBufferConfig, IoBufferPools, IoLease, PoolRequest};
    use std::{thread, time::Duration};

    const POOL_WAIT_INTERVAL: Duration = Duration::from_millis(1);

    #[test]
    fn consume_payload_cursor_preserves_response_from_first_segment() {
        let lease = multi_segment_lease(&[b'x'; 160]);
        let mut seen_offsets = Vec::new();

        let outcome = consume_payload_cursor(lease.cursor(), |payload_offset, _chunk| {
            seen_offsets.push(payload_offset);
            PayloadConsumeOutcome::Respond(bad_request_response("synthetic bad request\n"))
        });

        assert_eq!(seen_offsets, vec![0]);

        let PayloadConsumeOutcome::Respond(response) = outcome else {
            panic!("expected response outcome from first cursor segment");
        };
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        match response.body {
            HttpResponseBody::Static(body) => {
                assert_eq!(body, b"synthetic bad request\n");
            }
            HttpResponseBody::EchoedRequestBody => {
                panic!("synthetic bad-request response unexpectedly echoed a request body");
            }
        }
    }

    fn multi_segment_lease(bytes: &[u8]) -> IoLease {
        let mut pool_config = IoBufferConfig::default();
        pool_config.egress.chunk_size = 128;
        pool_config.egress.initial_chunk_count = 2;
        pool_config.egress.max_chunk_count = 2;
        pool_config.egress.encode_buf_min_free_space = 8;

        let pools = IoBufferPools::new(pool_config).expect("create test buffer pools");
        let reservation_request = pools
            .egress()
            .reserve(bytes.len())
            .expect("reserve egress capacity");
        let reservation = wait_for_pool_request(reservation_request);
        reservation
            .copy_bytes(bytes)
            .expect("write multi-segment lease")
    }

    fn wait_for_pool_request<T>(mut request: PoolRequest<T>) -> T {
        loop {
            match request.try_receive().expect("poll pool request") {
                Some(reply) => return reply,
                None => thread::sleep(POOL_WAIT_INTERVAL),
            }
        }
    }
}
