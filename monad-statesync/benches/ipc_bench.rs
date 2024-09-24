use std::{path::PathBuf, sync::Arc};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    task::JoinHandle,
};

const MIN_PAYLOAD_SIZE: usize = 128 * 1024;

pub fn criterion_benchmark(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .expect("failed to init tokio rt");

    let buffer = {
        let mut buffer: Vec<u8> = Vec::new();
        while buffer.len() < MIN_PAYLOAD_SIZE {
            // fill buffer with upserts
            let len = rand::random::<u8>() % 32;
            buffer.push(len);
            buffer.extend(0..len);
        }
        Arc::new(buffer)
    };

    let mut group = c.benchmark_group("ipc");
    group.throughput(Throughput::Bytes(buffer.len() as u64));
    group.bench_function("raw", |b| {
        b.to_async(&rt)
            .iter(|| async { ipc(buffer.clone(), |s| s).await.expect("io err") })
    });

    group.bench_function("buffered", |b| {
        b.to_async(&rt)
            .iter(|| async { ipc(buffer.clone(), BufReader::new).await.expect("io err") })
    });
}

async fn ipc<S: AsyncRead + Unpin>(
    buffer: Arc<Vec<u8>>,
    read_stream_wrapper: impl FnOnce(UnixStream) -> S,
) -> tokio::io::Result<()> {
    let tempdir = tempfile::tempdir().expect("failed to create tempdir");
    let path = {
        let mut path = PathBuf::new();
        path.push(tempdir.path());
        path.push("bench.sock");
        Arc::new(path)
    };
    let sock_server = UnixListener::bind(&*path)?;

    let buffer_clone = buffer.clone();
    let client_handle: JoinHandle<tokio::io::Result<()>> = tokio::spawn(async move {
        let mut sock_client_stream = UnixStream::connect(&*path).await?;
        sock_client_stream.write_all(&buffer_clone).await?;
        Ok(())
    });

    let (sock_server_stream, _) = sock_server.accept().await?;
    let mut sock_server_stream = read_stream_wrapper(sock_server_stream);

    let mut read_buffer = Vec::new();
    while read_buffer.len() < MIN_PAYLOAD_SIZE {
        let len = sock_server_stream.read_u8().await?;
        read_buffer.push(len);
        read_buffer.extend((0..).take(len.into()));
        let read_buffer_len = read_buffer.len();
        sock_server_stream
            .read_exact(&mut read_buffer[(read_buffer_len - len as usize)..])
            .await?;
    }

    client_handle.await??;

    assert_eq!(&*buffer, &read_buffer);
    Ok(())
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
