use std::{
    io::Read,
    net::SocketAddr,
    process::{Child, ChildStdin, Command, Output, Stdio},
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

fn smoke_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    // These smoke tests spawn real child processes that briefly reserve loopback ports and infer
    // readiness from their stdout/stderr. Running them concurrently makes the "closed port" and
    // listener/client assumptions race with each other, so keep the whole suite process-local and
    // serial.
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("smoke-test guard lock")
}

#[test]
fn udp_bind_pair_exchanges_one_scripted_line() {
    let _guard = smoke_test_guard();
    let receiver = spawn_netcat(&["udp", "bind", "--bind", "127.0.0.1:0"]);
    let receiver_addr = receiver.wait_for_socket_addr("UDP bound ");

    let sender = spawn_netcat(&[
        "--send",
        "ping",
        "--exit-after-send",
        "udp",
        "sendto",
        "--target",
        &receiver_addr.to_string(),
    ]);

    let sender_output = sender.wait_for_output();
    receiver.wait_for_stdout("ping");
    let receiver_output = receiver.kill_and_collect();

    assert!(
        sender_output.status.success(),
        "sender failed: {sender_output:?}"
    );

    let receiver_stdout = String::from_utf8_lossy(&receiver_output.stdout);
    assert!(
        receiver_stdout.contains("ping"),
        "receiver stdout: {receiver_stdout}"
    );
}

#[test]
fn udp_sendto_interactive_mode_stays_alive_until_explicit_shutdown() {
    let _guard = smoke_test_guard();
    let receiver = spawn_netcat(&["udp", "bind", "--bind", "127.0.0.1:0"]);
    let receiver_addr = receiver.wait_for_socket_addr("UDP bound ");

    let mut sender = spawn_netcat(&["udp", "sendto", "--target", &receiver_addr.to_string()]);
    let _sender_addr = sender.wait_for_socket_addr("UDP bound ");

    thread::sleep(Duration::from_secs(3));
    assert!(
        sender.is_running(),
        "interactive UDP sender exited unexpectedly: stderr={}",
        sender.stderr_string()
    );

    sender.send_line("ping");
    receiver.wait_for_stdout("ping");

    let _sender_output = sender.kill_and_collect();
    let receiver_output = receiver.kill_and_collect();

    let receiver_stdout = String::from_utf8_lossy(&receiver_output.stdout);
    assert!(
        receiver_stdout.contains("ping"),
        "receiver stdout: {receiver_stdout}"
    );
}

#[test]
fn udp_connect_client_exchanges_one_scripted_line() {
    let _guard = smoke_test_guard();
    let receiver = spawn_netcat(&["udp", "bind", "--bind", "127.0.0.1:0"]);
    let receiver_addr = receiver.wait_for_socket_addr("UDP bound ");

    let sender = spawn_netcat(&[
        "--send",
        "ping",
        "--exit-after-send",
        "udp",
        "connect",
        "--remote",
        &receiver_addr.to_string(),
    ]);

    let sender_output = sender.wait_for_output();
    receiver.wait_for_stdout("ping");
    let receiver_output = receiver.kill_and_collect();

    assert!(
        sender_output.status.success(),
        "sender failed: {sender_output:?}"
    );

    let receiver_stdout = String::from_utf8_lossy(&receiver_output.stdout);
    assert!(
        receiver_stdout.contains("ping"),
        "receiver stdout: {receiver_stdout}"
    );
}

#[cfg(target_os = "macos")]
#[test]
fn udp_connect_client_exits_after_server_disappears() {
    let _guard = smoke_test_guard();
    let server = spawn_netcat(&["udp", "bind", "--bind", "127.0.0.1:0"]);
    let server_addr = server.wait_for_socket_addr("UDP bound ");

    let mut client = spawn_netcat(&["udp", "connect", "--remote", &server_addr.to_string()]);
    client.wait_for_stderr("UDP connected ");

    let _server_output = server.kill_and_collect();

    client.send_line("ping");
    let client_output = client.wait_for_output();

    assert!(
        client_output.status.success(),
        "client failed: {client_output:?}"
    );

    let stderr = String::from_utf8_lossy(&client_output.stderr);
    assert!(
        stderr.contains("UDP closed (Disconnected)")
            || stderr.contains("UDP closed (Disconnected) for remote"),
        "client stderr: {stderr}"
    );
}

#[test]
fn tcp_connect_to_closed_port_fails() {
    let _guard = smoke_test_guard();
    let closed_addr = closed_loopback_tcp_addr();
    let client = spawn_netcat(&[
        "--send",
        "ping",
        "--exit-after-send",
        "tcp",
        "connect",
        "--remote",
        &closed_addr.to_string(),
    ]);

    let output = client.wait_for_output();

    assert!(
        !output.status.success(),
        "client unexpectedly succeeded: {output:?}"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("TCP connect to") || stderr.contains("failed to open TCP session"),
        "client stderr: {stderr}"
    );
}

#[test]
fn tcp_listener_and_client_exchange_one_scripted_line() {
    let _guard = smoke_test_guard();
    let listener = spawn_netcat(&["tcp", "listen", "--bind", "127.0.0.1:0"]);
    let listener_addr = listener.wait_for_socket_addr("TCP listening on ");

    let client = spawn_netcat(&[
        "--send",
        "ping",
        "--exit-after-send",
        "tcp",
        "connect",
        "--remote",
        &listener_addr.to_string(),
    ]);

    let client_output = client.wait_for_output();
    listener.wait_for_stdout("ping");
    let listener_output = listener.kill_and_collect();

    assert!(
        client_output.status.success(),
        "client failed: {client_output:?}"
    );
    let listener_stdout = String::from_utf8_lossy(&listener_output.stdout);
    assert!(
        listener_stdout.contains("ping"),
        "listener stdout: {listener_stdout}"
    );
}

#[test]
fn tcp_connect_empty_scripted_line_exits_cleanly() {
    let _guard = smoke_test_guard();
    let listener = spawn_netcat(&["tcp", "listen", "--bind", "127.0.0.1:0"]);
    let listener_addr = listener.wait_for_socket_addr("TCP listening on ");

    let client = spawn_netcat(&[
        "--send",
        "",
        "--exit-after-send",
        "tcp",
        "connect",
        "--remote",
        &listener_addr.to_string(),
    ]);

    let client_output = client.wait_for_output();
    let listener_output = listener.kill_and_collect();

    assert!(
        client_output.status.success(),
        "client failed on empty scripted send\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&client_output.stdout),
        String::from_utf8_lossy(&client_output.stderr)
    );

    assert!(
        listener_output.status.success() || listener_output.status.code().is_none(),
        "listener teardown failed unexpectedly\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&listener_output.stdout),
        String::from_utf8_lossy(&listener_output.stderr)
    );
}

#[test]
fn tcp_listener_empty_scripted_line_closes_cleanly() {
    let _guard = smoke_test_guard();
    let listener = spawn_netcat(&[
        "--send",
        "",
        "--exit-after-send",
        "tcp",
        "listen",
        "--bind",
        "127.0.0.1:0",
    ]);
    let listener_addr = listener.wait_for_socket_addr("TCP listening on ");

    let mut client = std::net::TcpStream::connect(listener_addr).expect("connect TCP client");
    client
        .set_read_timeout(Some(WAIT_TIMEOUT))
        .expect("set client read timeout");
    let mut received = Vec::new();
    client
        .read_to_end(&mut received)
        .expect("read empty scripted response to end");

    let listener_output = listener.wait_for_output();

    assert!(
        listener_output.status.success(),
        "listener failed on empty scripted send\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&listener_output.stdout),
        String::from_utf8_lossy(&listener_output.stderr)
    );
    assert!(
        received.is_empty(),
        "expected no bytes from empty scripted TCP send, got {:?}",
        received
    );
}

#[test]
fn udp_bind_replies_to_the_last_sender_without_explicit_target() {
    let _guard = smoke_test_guard();
    let mut listener = spawn_netcat(&["udp", "bind", "--bind", "127.0.0.1:0"]);
    let listener_addr = listener.wait_for_socket_addr("UDP bound ");

    let mut sender = spawn_netcat(&["udp", "sendto", "--target", &listener_addr.to_string()]);

    sender.send_line("ping");
    listener.wait_for_stdout("ping");

    listener.send_line("pong");
    sender.wait_for_stdout("pong");

    let sender_output = sender.kill_and_collect();
    let listener_output = listener.kill_and_collect();

    let sender_stdout = String::from_utf8_lossy(&sender_output.stdout);
    assert!(
        sender_stdout.contains("pong"),
        "sender stdout: {sender_stdout}"
    );

    let listener_stdout = String::from_utf8_lossy(&listener_output.stdout);
    assert!(
        listener_stdout.contains("ping"),
        "listener stdout: {listener_stdout}"
    );
}

struct SpawnedNetcat {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
}

impl SpawnedNetcat {
    fn send_line(&mut self, line: &str) {
        use std::io::Write;

        let stdin = self.stdin.as_mut().expect("live child stdin pipe");
        writeln!(stdin, "{line}").expect("write child stdin");
        stdin.flush().expect("flush child stdin");
    }

    fn wait_for_stdout(&self, pattern: &str) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let stdout = self.stdout.lock().expect("stdout lock");
            if String::from_utf8_lossy(&stdout).contains(pattern) {
                return;
            }
            drop(stdout);

            if Instant::now() >= deadline {
                panic!("timed out waiting for stdout pattern: {pattern}");
            }

            thread::sleep(Duration::from_millis(20));
        }
    }

    #[allow(dead_code)] // for later
    fn wait_for_stderr(&self, pattern: &str) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let stderr = self.stderr.lock().expect("stderr lock");
            if String::from_utf8_lossy(&stderr).contains(pattern) {
                return;
            }
            drop(stderr);

            if Instant::now() >= deadline {
                panic!("timed out waiting for stderr pattern: {pattern}");
            }

            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_socket_addr(&self, marker: &str) -> SocketAddr {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(addr) = self.find_socket_addr(marker) {
                return addr;
            }

            if Instant::now() >= deadline {
                panic!("timed out waiting for stderr socket address after marker: {marker}");
            }

            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_output(mut self) -> Output {
        self.stdin.take();
        let mut child = self.child.take().expect("live child process");
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            match child.try_wait().expect("poll child process") {
                Some(_) => {
                    let mut output = child.wait_with_output().expect("collect child output");
                    output.stdout = self.join_stdout();
                    output.stderr = self.join_stderr();
                    return output;
                }
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    panic!("timed out waiting for netcat example process");
                }
                None => thread::sleep(Duration::from_millis(20)),
            }
        }
    }

    fn is_running(&mut self) -> bool {
        let child = self.child.as_mut().expect("live child process");
        child.try_wait().expect("poll child process").is_none()
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

    fn stderr_string(&self) -> String {
        let stderr = self.stderr.lock().expect("stderr lock");
        String::from_utf8_lossy(&stderr).into_owned()
    }
}

fn closed_loopback_tcp_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
    let addr = listener.local_addr().expect("temporary listener address");
    drop(listener);
    addr
}

fn spawn_netcat(args: &[&str]) -> SpawnedNetcat {
    let mut command = Command::new(env!("CARGO_BIN_EXE_netcat"));
    command.args(args);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn netcat example");
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
    SpawnedNetcat {
        child: Some(child),
        stdin: Some(stdin),
        stdout,
        stderr,
        stdout_thread: Some(stdout_thread),
        stderr_thread: Some(stderr_thread),
    }
}
