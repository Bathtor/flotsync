use flotsync_io::{
    config_keys,
    test_support::{ReservedSocketKind, ReservedSocketLease, reserve_sockets},
};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::{Child, ChildStdin, Command, ExitStatus, Output, Stdio},
    sync::{Arc, Mutex},
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn get_root_returns_hello_response() {
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 404 Not Found");
    assert_eq!(parsed.body, b"not found\n");

    let _output = server.kill_and_collect();
}

#[test]
fn wrong_method_returns_allow_header() {
    let mut server = spawn_http_server();
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
    let mut server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"GET / HTTP/1.1\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(parsed.status_line, "HTTP/1.1 400 Bad Request");

    let _output = server.kill_and_collect();
}

#[test]
fn http_10_returns_version_not_supported() {
    let mut server = spawn_http_server();
    let addr = server.wait_for_socket_addr("HTTP listening on ");

    let response = exchange(addr, &[b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n"]);
    let parsed = parse_response(&response);

    assert_eq!(
        parsed.status_line,
        "HTTP/1.1 505 HTTP Version Not Supported"
    );

    let _output = server.kill_and_collect();
}

#[test]
fn stdin_eof_shuts_the_server_down() {
    let mut server = spawn_http_server();
    let _addr = server.wait_for_socket_addr("HTTP listening on ");

    let output = server.close_stdin_and_wait();
    assert!(
        output.status.success(),
        "http_server did not exit cleanly after stdin EOF\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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
    _socket_lease: ReservedSocketLease,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
}

impl SpawnedHttpServer {
    fn wait_for_socket_addr(&mut self, marker: &str) -> SocketAddr {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(addr) = self.find_socket_addr(marker) {
                return addr;
            }

            if let Some(status) = self
                .child
                .as_mut()
                .expect("live http_server child process")
                .try_wait()
                .expect("poll http_server child process")
            {
                self.panic_with_child_diagnostics(
                    format!(
                        "http_server exited before reporting socket address after marker: {marker}"
                    ),
                    Some(status),
                );
            }

            if Instant::now() >= deadline {
                self.panic_with_child_diagnostics(
                    format!(
                        "timed out waiting for HTTP server socket address after marker: {marker}"
                    ),
                    None,
                );
            }

            thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill_and_collect(mut self) -> Output {
        self.kill_and_collect_in_place()
    }

    fn kill_and_collect_in_place(&mut self) -> Output {
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

    fn close_stdin_and_wait(mut self) -> Output {
        self.stdin.take();
        let mut child = self.child.take().expect("live child process");
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            match child.try_wait().expect("poll child exit after stdin EOF") {
                Some(_status) => break,
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
                None => {
                    let _ = child.kill();
                    let mut output = child
                        .wait_with_output()
                        .expect("collect killed child output after stdin EOF timeout");
                    output.stdout = self.join_stdout();
                    output.stderr = self.join_stderr();
                    panic!(
                        "timed out waiting for http_server to exit after stdin EOF\nstdout:\n{}\nstderr:\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
        }
        let mut output = child.wait_with_output().expect("collect child output");
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

    fn collect_exited_output(&mut self) -> (Vec<u8>, Vec<u8>) {
        self.stdin.take();
        let mut child = self.child.take().expect("live child process");
        let _status = child.wait().expect("wait for exited child process");
        let stdout = self.join_stdout();
        let stderr = self.join_stderr();
        (stdout, stderr)
    }

    fn panic_with_child_diagnostics(
        &mut self,
        context: String,
        observed_status: Option<ExitStatus>,
    ) -> ! {
        let (status, stdout, stderr) = match observed_status {
            Some(status) => {
                let (stdout, stderr) = self.collect_exited_output();
                (status, stdout, stderr)
            }
            None => {
                let output = self.kill_and_collect_in_place();
                (output.status, output.stdout, output.stderr)
            }
        };
        panic!(
            "{context}\nexit status: {}\nstdout:\n{}\nstderr:\n{}",
            format_exit_status(status),
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        );
    }
}

fn format_exit_status(status: ExitStatus) -> String {
    match status.code() {
        Some(code) => format!("code {code}"),
        None => "terminated by signal".to_owned(),
    }
}

fn spawn_http_server() -> SpawnedHttpServer {
    let socket_lease = reserve_sockets(&[ReservedSocketKind::TcpListener]);
    let bind_addr = socket_lease.addr(0);
    let mut command = Command::new(env!("CARGO_BIN_EXE_http_server"));
    let bind_reuse_config = bind_reuse_config_string();
    command.args(["--kompact-config", bind_reuse_config.as_str()]);
    command.args(["--bind", &bind_addr.to_string()]);
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
        _socket_lease: socket_lease,
        child: Some(child),
        stdin: Some(stdin),
        stdout,
        stderr,
        stdout_thread: Some(stdout_thread),
        stderr_thread: Some(stderr_thread),
    }
}

fn bind_reuse_config_string() -> String {
    format!("{} = true", config_keys::BIND_REUSE_ADDRESS.key)
}
