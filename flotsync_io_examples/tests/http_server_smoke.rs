use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::{Child, ChildStdin, Command, Output, Stdio},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

fn smoke_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn get_root_returns_hello_response() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 200 OK");
    assert_eq!(
        parsed.headers.get("content-type"),
        Some(&"text/plain; charset=utf-8".to_string())
    );
    assert_eq!(parsed.headers.get("connection"), Some(&"close".to_string()));
    assert_eq!(parsed.body, b"hello from flotsync_io\n");

    let _output = server.kill_and_collect();
}

#[test]
fn head_root_returns_headers_without_body() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"HEAD / HTTP/1.1\r\nHost: localhost\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 200 OK");
    assert_eq!(parsed.body.len(), 0);
    assert_eq!(
        parsed.headers.get("content-length"),
        Some(&"23".to_string())
    );

    let _output = server.kill_and_collect();
}

#[test]
fn post_echo_returns_exact_body() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(
        addr,
        &[b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello"],
    );
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 200 OK");
    assert_eq!(
        parsed.headers.get("content-type"),
        Some(&"application/octet-stream".to_string())
    );
    assert_eq!(parsed.body, b"hello");

    let _output = server.kill_and_collect();
}

#[test]
fn post_echo_handles_split_writes() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(
        addr,
        &[
            b"POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 11\r\n\r\nhel",
            b"lo world",
        ],
    );
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 200 OK");
    assert_eq!(parsed.body, b"hello world");

    let _output = server.kill_and_collect();
}

#[test]
fn pipelined_second_request_still_receives_first_response() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(
        addr,
        &[b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\nGET / HTTP/1.1\r\nHost: localhost\r\n\r\n"],
    );
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 200 OK");
    assert_eq!(parsed.body, b"hello from flotsync_io\n");

    let _output = server.kill_and_collect();
}

#[test]
fn malformed_request_with_trailing_bytes_still_returns_bad_request() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(
        addr,
        &[b"GET / HTTP/1.1\r\n\r\nGET / HTTP/1.1\r\nHost: localhost\r\n\r\n"],
    );
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 400 Bad Request");
    let body_text = std::str::from_utf8(&parsed.body).expect("bad-request body UTF-8");
    assert!(
        body_text.contains("Host header"),
        "unexpected bad-request body: {body_text:?}"
    );

    let _output = server.kill_and_collect();
}

#[test]
fn post_echo_accepts_large_single_payload_body() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let body = vec![b'x'; 20 * 1024];
    let mut request = format!(
        "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(&body);

    let response = exchange(addr, &[request.as_slice()]);
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 200 OK");
    assert_eq!(
        parsed.headers.get("content-length"),
        Some(&body.len().to_string())
    );
    assert_eq!(parsed.body, body);

    let _output = server.kill_and_collect();
}

#[test]
fn unknown_path_returns_not_found() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 404 Not Found");
    assert_eq!(parsed.body, b"not found\n");

    let _output = server.kill_and_collect();
}

#[test]
fn wrong_method_returns_allow_header() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(
        addr,
        &[b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n"],
    );
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 405 Method Not Allowed");
    assert_eq!(parsed.headers.get("allow"), Some(&"GET, HEAD".to_string()));

    let _output = server.kill_and_collect();
}

#[test]
fn missing_host_returns_bad_request() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"GET / HTTP/1.1\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 400 Bad Request");

    let _output = server.kill_and_collect();
}

#[test]
fn http_10_returns_version_not_supported() {
    let _guard = smoke_test_guard();
    let server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(
        parsed.status_line,
        "HTTP/1.1 505 HTTP Version Not Supported"
    );

    let _output = server.kill_and_collect();
}

struct ParsedResponse {
    status_line: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn parse_response(bytes: &[u8]) -> ParsedResponse {
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response header terminator")
        + 4;
    let header_text = std::str::from_utf8(&bytes[..split]).expect("response header UTF-8");
    let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
    let status_line = lines.next().expect("status line").to_string();
    let mut headers = HashMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').expect("header separator");
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    ParsedResponse {
        status_line,
        headers,
        body: bytes[split..].to_vec(),
    }
}

fn exchange(addr: SocketAddr, chunks: &[&[u8]]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).expect("connect HTTP example");
    stream
        .set_read_timeout(Some(WAIT_TIMEOUT))
        .expect("set read timeout");
    stream
        .set_write_timeout(Some(WAIT_TIMEOUT))
        .expect("set write timeout");

    for chunk in chunks {
        stream.write_all(chunk).expect("write request chunk");
        stream.flush().expect("flush request chunk");
        thread::sleep(Duration::from_millis(10));
    }
    stream
        .shutdown(std::net::Shutdown::Write)
        .expect("shutdown write");

    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    response
}

struct SpawnedHttpServer {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
}

impl SpawnedHttpServer {
    fn wait_for_socket_addr(&self, marker: &str) -> SocketAddr {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(addr) = self.find_socket_addr(marker) {
                return addr;
            }

            if Instant::now() >= deadline {
                panic!("timed out waiting for HTTP server socket address after marker: {marker}");
            }

            thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill_and_collect(mut self) -> Output {
        self.stdin.take();
        let mut child = self.child.take().expect("live child process");
        let _ = child.kill();
        let mut output = child
            .wait_with_output()
            .expect("collect killed child output");
        output.stdout = self.join_stdout();
        output.stderr = self.join_stderr();
        output
    }

    fn join_stdout(&mut self) -> Vec<u8> {
        if let Some(thread) = self.stdout_thread.take() {
            thread.join().expect("join stdout reader");
        }
        std::mem::take(&mut self.stdout.lock().expect("stdout lock"))
    }

    fn join_stderr(&mut self) -> Vec<u8> {
        if let Some(thread) = self.stderr_thread.take() {
            thread.join().expect("join stderr reader");
        }
        std::mem::take(&mut self.stderr.lock().expect("stderr lock"))
    }

    fn find_socket_addr(&self, marker: &str) -> Option<SocketAddr> {
        let stderr = self.stderr.lock().expect("stderr lock");
        let stderr = String::from_utf8_lossy(&stderr);
        for line in stderr.lines().rev() {
            let Some((_, suffix)) = line.rsplit_once(marker) else {
                continue;
            };
            let candidate = suffix.trim();
            if let Ok(addr) = candidate.parse() {
                return Some(addr);
            }
        }
        None
    }
}

fn spawn_http_server() -> SpawnedHttpServer {
    let mut command = Command::new(env!("CARGO_BIN_EXE_http_server"));
    command.args(["--bind", "127.0.0.1:0"]);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn http_server example");
    let stdin = child.stdin.take().expect("child stdin pipe");
    let stdout = Arc::new(Mutex::new(Vec::new()));
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let stdout_capture = Arc::clone(&stdout);
    let stderr_capture = Arc::clone(&stderr);
    let stdout_handle = child.stdout.take().expect("child stdout pipe");
    let stderr_handle = child.stderr.take().expect("child stderr pipe");
    let stdout_thread = thread::spawn(move || {
        let mut stdout_handle = stdout_handle;
        let mut chunk = [0_u8; 1024];
        loop {
            let bytes_read = stdout_handle.read(&mut chunk).expect("read child stdout");
            if bytes_read == 0 {
                break;
            }
            let mut stdout = stdout_capture.lock().expect("stdout lock");
            stdout.extend_from_slice(&chunk[..bytes_read]);
        }
    });
    let stderr_thread = thread::spawn(move || {
        let mut stderr_handle = stderr_handle;
        let mut chunk = [0_u8; 1024];
        loop {
            let bytes_read = stderr_handle.read(&mut chunk).expect("read child stderr");
            if bytes_read == 0 {
                break;
            }
            let mut stderr = stderr_capture.lock().expect("stderr lock");
            stderr.extend_from_slice(&chunk[..bytes_read]);
        }
    });
    SpawnedHttpServer {
        child: Some(child),
        stdin: Some(stdin),
        stdout,
        stderr,
        stdout_thread: Some(stdout_thread),
        stderr_thread: Some(stderr_thread),
    }
}
