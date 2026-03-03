use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use flotsync_core::versions::UpdateId;
use flotsync_data_types::{
    IdWithIndex,
    any_data::{LinearLatestValueWins, list::LinearList},
    schema::{
        ArrayType,
        BasicDataType,
        NullableBasicDataType,
        PrimitiveType,
        datamodel::{
            BasicValue as ModelBasicValue,
            DataModelValueError,
            NullableBasicValue as ModelNullableBasicValue,
        },
        values::PrimitiveValueArray as ModelPrimitiveValueArray,
    },
    snapshot::{SnapshotHeader, SnapshotNode, SnapshotNodeRef, SnapshotSink},
    text::LinearString,
};
use flotsync_messages::{
    codecs::datamodel::{
        ColumnarHistoryCodecError,
        decode_columnar_latest_value_wins_history_snapshot,
        decode_columnar_linear_list_history_snapshot,
        decode_columnar_linear_string_history_snapshot,
        encode_columnar_latest_value_wins_history_snapshot,
        encode_columnar_linear_list_history_snapshot,
        encode_columnar_linear_string_history_snapshot,
    },
    datamodel as proto,
    protobuf::Message,
};
use std::time::Duration;

struct CollectSink<Id, Borrowed: ?Sized, Owned, Mapper> {
    nodes: Vec<SnapshotNode<Id, Owned>>,
    map_value: Mapper,
    _marker: std::marker::PhantomData<fn(&Borrowed)>,
}

impl<Id, Borrowed: ?Sized, Owned, Mapper> CollectSink<Id, Borrowed, Owned, Mapper> {
    fn new(map_value: Mapper) -> Self {
        Self {
            nodes: Vec::new(),
            map_value,
            _marker: std::marker::PhantomData,
        }
    }

    fn into_nodes(self) -> Vec<SnapshotNode<Id, Owned>> {
        self.nodes
    }
}

impl<Id, Borrowed: ?Sized, Owned, Mapper> SnapshotSink<Id, Borrowed>
    for CollectSink<Id, Borrowed, Owned, Mapper>
where
    Id: Clone,
    Mapper: FnMut(&Borrowed) -> Owned,
{
    type Error = std::convert::Infallible;

    fn begin(&mut self, _header: SnapshotHeader) -> Result<(), Self::Error> {
        Ok(())
    }

    fn node(
        &mut self,
        _index: usize,
        node: SnapshotNodeRef<'_, Id, Borrowed>,
    ) -> Result<(), Self::Error> {
        self.nodes.push(SnapshotNode {
            id: node.id.clone(),
            left: node.left.cloned(),
            right: node.right.cloned(),
            deleted: node.deleted,
            value: node.value.map(|value| (self.map_value)(value)),
        });
        Ok(())
    }

    fn end(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct LvwFixture {
    name: &'static str,
    value_type: NullableBasicDataType,
    nodes: Vec<SnapshotNode<UpdateId, ModelNullableBasicValue>>,
}

struct LinearStringFixture {
    name: &'static str,
    nodes: Vec<SnapshotNode<IdWithIndex<UpdateId>, String>>,
}

struct LinearListFixture {
    name: &'static str,
    value_type: PrimitiveType,
    nodes: Vec<SnapshotNode<IdWithIndex<UpdateId>, ModelPrimitiveValueArray>>,
}

fn update_id(version: u64) -> UpdateId {
    UpdateId {
        version,
        node_index: (version % 7) as u32,
    }
}

fn indexed_update_id(version: u64) -> IdWithIndex<UpdateId> {
    IdWithIndex::zero(update_id(version))
}

fn build_lvw_array_fixture(name: &'static str, updates: usize) -> LvwFixture {
    let value_type = NullableBasicDataType::Nullable(BasicDataType::Array(Box::new(ArrayType {
        element_type: PrimitiveType::Int,
    })));
    let mut value = LinearLatestValueWins::new(
        ModelNullableBasicValue::Value(ModelBasicValue::Array(ModelPrimitiveValueArray::Int(
            vec![1, 2, 3],
        ))),
        [update_id(1), update_id(2), update_id(3)],
    );

    for step in 0..updates {
        let version = (step as u64) + 4;
        let value_update = match step % 3 {
            0 => ModelNullableBasicValue::Null,
            1 => ModelNullableBasicValue::Value(ModelBasicValue::Array(
                ModelPrimitiveValueArray::Int(Vec::new()),
            )),
            _ => ModelNullableBasicValue::Value(ModelBasicValue::Array(
                ModelPrimitiveValueArray::Int(vec![step as i64, step as i64 + 1, step as i64 + 2]),
            )),
        };
        value.update(update_id(version), value_update);
    }

    let mut sink = CollectSink::new(Clone::clone);
    value.encode_snapshot(&mut sink).unwrap();
    LvwFixture {
        name,
        value_type,
        nodes: sink.into_nodes(),
    }
}

fn string_chunk(index: usize) -> String {
    format!("{:04}", index % 10_000)
}

fn build_linear_string_fixture(
    name: &'static str,
    chunk_count: usize,
    delete_every: usize,
) -> LinearStringFixture {
    let mut value = LinearString::with_value(string_chunk(0), update_id(100));

    for index in 1..chunk_count {
        value.append(indexed_update_id(100 + index as u64), string_chunk(index));
    }

    if delete_every > 0 {
        let deleted_chunks = (1..chunk_count).step_by(delete_every).collect::<Vec<_>>();
        for index in deleted_chunks.into_iter().rev() {
            let start = index * 4;
            let end = start + 4;
            value
                .ids_in_range(start..end)
                .unwrap()
                .delete(&mut value)
                .unwrap();
        }
    }

    let mut sink = CollectSink::new(str::to_owned);
    value.encode_snapshot(&mut sink).unwrap();
    LinearStringFixture {
        name,
        nodes: sink.into_nodes(),
    }
}

fn build_linear_list_fixture(
    name: &'static str,
    chunk_count: usize,
    chunk_size: usize,
    delete_every: usize,
) -> LinearListFixture {
    let initial_values = (0..chunk_size)
        .map(|value| value as i64)
        .collect::<Vec<_>>();
    let mut value = LinearList::with_values(initial_values.clone(), update_id(200));

    for chunk_index in 1..chunk_count {
        let start = (chunk_index * chunk_size) as i64;
        let chunk = (start..start + chunk_size as i64).collect::<Vec<_>>();
        value.append(indexed_update_id(200 + chunk_index as u64), chunk);
    }

    if delete_every > 0 {
        let deleted_chunks = (1..chunk_count).step_by(delete_every).collect::<Vec<_>>();
        for chunk_index in deleted_chunks.into_iter().rev() {
            let start = chunk_index * chunk_size;
            let end = start + chunk_size;
            value
                .ids_in_range(start..end)
                .unwrap()
                .delete(&mut value)
                .unwrap();
        }
    }

    let mut sink =
        CollectSink::new(|values: &[i64]| ModelPrimitiveValueArray::Int(values.to_vec()));
    value.encode_snapshot(&mut sink).unwrap();
    LinearListFixture {
        name,
        value_type: PrimitiveType::Int,
        nodes: sink.into_nodes(),
    }
}

fn lvw_bytes(fixture: &LvwFixture) -> Vec<u8> {
    encode_columnar_latest_value_wins_history_snapshot(&fixture.nodes, &fixture.value_type)
        .unwrap()
        .write_to_bytes()
        .unwrap()
}

fn string_bytes(fixture: &LinearStringFixture) -> Vec<u8> {
    encode_columnar_linear_string_history_snapshot(&fixture.nodes)
        .unwrap()
        .write_to_bytes()
        .unwrap()
}

fn list_bytes(fixture: &LinearListFixture) -> Vec<u8> {
    encode_columnar_linear_list_history_snapshot(&fixture.nodes, fixture.value_type)
        .unwrap()
        .write_to_bytes()
        .unwrap()
}

fn decode_lvw(
    bytes: &[u8],
    fixture: &LvwFixture,
) -> Vec<SnapshotNode<UpdateId, ModelNullableBasicValue>> {
    let history = proto::HistorySnapshot::parse_from_bytes(bytes).unwrap();
    decode_columnar_latest_value_wins_history_snapshot(history, fixture.value_type.clone()).unwrap()
}

fn decode_string(bytes: &[u8]) -> Vec<SnapshotNode<IdWithIndex<UpdateId>, String>> {
    let history = proto::HistorySnapshot::parse_from_bytes(bytes).unwrap();
    decode_columnar_linear_string_history_snapshot(history).unwrap()
}

fn decode_list(
    bytes: &[u8],
    fixture: &LinearListFixture,
) -> Vec<SnapshotNode<IdWithIndex<UpdateId>, ModelPrimitiveValueArray>> {
    let history = proto::HistorySnapshot::parse_from_bytes(bytes).unwrap();
    decode_columnar_linear_list_history_snapshot(history, fixture.value_type).unwrap()
}

fn reconstruct_lvw(nodes: Vec<SnapshotNode<UpdateId, ModelNullableBasicValue>>) {
    let value = LinearLatestValueWins::from_snapshot_nodes(
        nodes.into_iter().map(Ok::<_, ColumnarHistoryCodecError>),
    )
    .unwrap();
    value.validate_integrity().unwrap();
}

fn reconstruct_string(nodes: Vec<SnapshotNode<IdWithIndex<UpdateId>, String>>) {
    let value = LinearString::from_snapshot_nodes(
        nodes.into_iter().map(Ok::<_, ColumnarHistoryCodecError>),
    )
    .unwrap();
    value.validate_integrity().unwrap();
}

fn reconstruct_list(nodes: Vec<SnapshotNode<IdWithIndex<UpdateId>, ModelPrimitiveValueArray>>) {
    let value = LinearList::from_snapshot_nodes(nodes.into_iter().map(|node| {
        let value = node
            .value
            .map(|value| match value {
                ModelPrimitiveValueArray::Int(values) => Ok(values),
                _ => Err(ColumnarHistoryCodecError::InvalidSnapshotValue {
                    source: DataModelValueError::InvalidSnapshotValueForType,
                }),
            })
            .transpose()?;
        Ok::<SnapshotNode<IdWithIndex<UpdateId>, Vec<i64>>, ColumnarHistoryCodecError>(
            SnapshotNode {
                id: node.id,
                left: node.left,
                right: node.right,
                deleted: node.deleted,
                value,
            },
        )
    }))
    .unwrap();
    value.validate_integrity().unwrap();
}

fn bench_lvw_fixture(c: &mut Criterion, fixture: &LvwFixture) {
    let bytes = lvw_bytes(fixture);
    eprintln!("fixture={} encoded_bytes={}", fixture.name, bytes.len());

    let mut encode_group = c.benchmark_group("history_snapshot/lvw/encode_bytes");
    encode_group.throughput(Throughput::Elements(fixture.nodes.len() as u64));
    encode_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| black_box(lvw_bytes(black_box(fixture))))
    });
    encode_group.finish();

    let mut decode_group = c.benchmark_group("history_snapshot/lvw/decode_bytes");
    decode_group.throughput(Throughput::Bytes(bytes.len() as u64));
    decode_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| black_box(decode_lvw(black_box(&bytes), fixture)))
    });
    decode_group.finish();

    let mut reconstruct_group = c.benchmark_group("history_snapshot/lvw/decode_and_reconstruct");
    reconstruct_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| reconstruct_lvw(decode_lvw(black_box(&bytes), fixture)))
    });
    reconstruct_group.finish();
}

fn bench_linear_string_fixture(c: &mut Criterion, fixture: &LinearStringFixture) {
    let bytes = string_bytes(fixture);
    eprintln!("fixture={} encoded_bytes={}", fixture.name, bytes.len());

    let mut encode_group = c.benchmark_group("history_snapshot/linear_string/encode_bytes");
    encode_group.throughput(Throughput::Elements(fixture.nodes.len() as u64));
    encode_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| black_box(string_bytes(black_box(fixture))))
    });
    encode_group.finish();

    let mut decode_group = c.benchmark_group("history_snapshot/linear_string/decode_bytes");
    decode_group.throughput(Throughput::Bytes(bytes.len() as u64));
    decode_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| black_box(decode_string(black_box(&bytes))))
    });
    decode_group.finish();

    let mut reconstruct_group =
        c.benchmark_group("history_snapshot/linear_string/decode_and_reconstruct");
    reconstruct_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| reconstruct_string(decode_string(black_box(&bytes))))
    });
    reconstruct_group.finish();
}

fn bench_linear_list_fixture(c: &mut Criterion, fixture: &LinearListFixture) {
    let bytes = list_bytes(fixture);
    eprintln!("fixture={} encoded_bytes={}", fixture.name, bytes.len());

    let mut encode_group = c.benchmark_group("history_snapshot/linear_list/encode_bytes");
    encode_group.throughput(Throughput::Elements(fixture.nodes.len() as u64));
    encode_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| black_box(list_bytes(black_box(fixture))))
    });
    encode_group.finish();

    let mut decode_group = c.benchmark_group("history_snapshot/linear_list/decode_bytes");
    decode_group.throughput(Throughput::Bytes(bytes.len() as u64));
    decode_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| black_box(decode_list(black_box(&bytes), fixture)))
    });
    decode_group.finish();

    let mut reconstruct_group =
        c.benchmark_group("history_snapshot/linear_list/decode_and_reconstruct");
    reconstruct_group.bench_function(BenchmarkId::new("current", fixture.name), |b| {
        b.iter(|| reconstruct_list(decode_list(black_box(&bytes), fixture)))
    });
    reconstruct_group.finish();
}

fn bench_history_snapshot_formats(c: &mut Criterion) {
    let lvw_fixtures = [
        build_lvw_array_fixture("lvw_array_medium", 128),
        build_lvw_array_fixture("lvw_array_large", 1024),
    ];
    let linear_string_fixtures = [
        build_linear_string_fixture("linear_string_tombstone_medium", 128, 5),
        build_linear_string_fixture("linear_string_tombstone_large", 1024, 6),
    ];
    let linear_list_fixtures = [
        build_linear_list_fixture("linear_list_tombstone_medium", 96, 4, 5),
        build_linear_list_fixture("linear_list_tombstone_large", 256, 8, 6),
    ];

    for fixture in &lvw_fixtures {
        bench_lvw_fixture(c, fixture);
    }
    for fixture in &linear_string_fixtures {
        bench_linear_string_fixture(c, fixture);
    }
    for fixture in &linear_list_fixtures {
        bench_linear_list_fixture(c, fixture);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = bench_history_snapshot_formats
}
criterion_main!(benches);
