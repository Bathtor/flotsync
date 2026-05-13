use flotsync_io::{
    config_keys,
    test_support::{ReservedSocketKind, ReservedSocketLease, reserve_sockets},
};
use std::{
    io::{ErrorKind, Read},
    net::SocketAddr,
    process::{Child, ChildStdin, Command, ExitStatus, Output, Stdio},
    sync::{Arc, Mutex},
    thread,
    thread::JoinHandle,
    time::{Duration, Instant},
};

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

#[test]
fn udp_bind_pair_exchanges_one_scripted_line() {
    let mut receiver =
        spawn_netcat_with_reserved_bind(ReservedSocketKind::UdpSocket, &["udp", "bind"]);
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
    let mut receiver =
        spawn_netcat_with_reserved_bind(ReservedSocketKind::UdpSocket, &["udp", "bind"]);
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
    let mut receiver =
        spawn_netcat_with_reserved_bind(ReservedSocketKind::UdpSocket, &["udp", "bind"]);
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
    let mut server =
        spawn_netcat_with_reserved_bind(ReservedSocketKind::UdpSocket, &["udp", "bind"]);
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
    let mut listener =
        spawn_netcat_with_reserved_bind(ReservedSocketKind::TcpListener, &["tcp", "listen"]);
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
    let mut listener =
        spawn_netcat_with_reserved_bind(ReservedSocketKind::TcpListener, &["tcp", "listen"]);
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
    let mut listener = spawn_netcat_with_reserved_bind(
        ReservedSocketKind::TcpListener,
        &["--send", "", "--exit-after-send", "tcp", "listen"],
    );
    let listener_addr = listener.wait_for_socket_addr("TCP listening on ");

    let mut client = std::net::TcpStream::connect(listener_addr).expect("connect TCP client");
    client
        .set_read_timeout(Some(WAIT_TIMEOUT))
        .expect("set client read timeout");
    let mut received = Vec::new();
    match client.read_to_end(&mut received) {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::ConnectionReset && received.is_empty() => {}
        Err(error) => panic!("read empty scripted response to end: {error}"),
    }

    let listener_output = listener.wait_for_output();

    assert!(
        listener_output.status.success(),
        "listener failed on empty scripted send\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&listener_output.stdout),
        String::from_utf8_lossy(&listener_output.stderr)
    );
    assert!(
        received.is_empty(),
        "expected no bytes from empty scripted TCP send, got {received:?}"
    );
}

#[test]
fn udp_bind_replies_to_the_last_sender_without_explicit_target() {
    let mut listener =
        spawn_netcat_with_reserved_bind(ReservedSocketKind::UdpSocket, &["udp", "bind"]);
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
    socket_lease: Option<ReservedSocketLease>,
    binding_released: bool,
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

    fn wait_for_stdout(&mut self, pattern: &str) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let stdout = self.stdout.lock().expect("stdout lock");
            if String::from_utf8_lossy(&stdout).contains(pattern) {
                return;
            }
            drop(stdout);

            if let Some(status) = self
                .child
                .as_mut()
                .expect("live netcat child process")
                .try_wait()
                .expect("poll netcat child process")
            {
                self.panic_with_child_diagnostics(
                    format!("netcat exited before stdout contained pattern: {pattern}"),
                    Some(status),
                );
            }

            if Instant::now() >= deadline {
                self.panic_with_child_diagnostics(
                    format!("timed out waiting for stdout pattern: {pattern}"),
                    None,
                );
            }

            thread::sleep(Duration::from_millis(20));
        }
    }

    #[allow(dead_code)] // for later
    fn wait_for_stderr(&mut self, pattern: &str) {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let stderr = self.stderr.lock().expect("stderr lock");
            if String::from_utf8_lossy(&stderr).contains(pattern) {
                return;
            }
            drop(stderr);

            if let Some(status) = self
                .child
                .as_mut()
                .expect("live netcat child process")
                .try_wait()
                .expect("poll netcat child process")
            {
                self.panic_with_child_diagnostics(
                    format!("netcat exited before stderr contained pattern: {pattern}"),
                    Some(status),
                );
            }

            if Instant::now() >= deadline {
                self.panic_with_child_diagnostics(
                    format!("timed out waiting for stderr pattern: {pattern}"),
                    None,
                );
            }

            thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for_socket_addr(&mut self, marker: &str) -> SocketAddr {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(addr) = self.find_socket_addr(marker) {
                self.release_reserved_binding();
                return addr;
            }

            if let Some(status) = self
                .child
                .as_mut()
                .expect("live netcat child process")
                .try_wait()
                .expect("poll netcat child process")
            {
                self.panic_with_child_diagnostics(
                    format!("netcat exited before reporting socket address after marker: {marker}"),
                    Some(status),
                );
            }

            if Instant::now() >= deadline {
                self.panic_with_child_diagnostics(
                    format!("timed out waiting for stderr socket address after marker: {marker}"),
                    None,
                );
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
                    self.restore_reserved_binding();
                    output.stdout = self.join_stdout();
                    output.stderr = self.join_stderr();
                    return output;
                }
                None if Instant::now() >= deadline => {
                    let _ = child.kill();
                    let mut output = child
                        .wait_with_output()
                        .expect("collect killed netcat child output");
                    output.stdout = self.join_stdout();
                    output.stderr = self.join_stderr();
                    panic!(
                        "timed out waiting for netcat example process\nexit status: {}\nstdout:\n{}\nstderr:\n{}",
                        format_exit_status(output.status),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
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
        self.kill_and_collect_in_place()
    }

    fn kill_and_collect_in_place(&mut self) -> Output {
        self.stdin.take();
        let mut child = self.child.take().expect("live child process");
        let _ = child.kill();
        let mut output = child
            .wait_with_output()
            .expect("collect killed child output");
        self.restore_reserved_binding();
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

    fn collect_exited_output(&mut self) -> (Vec<u8>, Vec<u8>) {
        self.stdin.take();
        let mut child = self.child.take().expect("live child process");
        let _status = child.wait().expect("wait for exited child process");
        self.restore_reserved_binding();
        let stdout = self.join_stdout();
        let stderr = self.join_stderr();
        (stdout, stderr)
    }

    fn release_reserved_binding(&mut self) {
        if self.binding_released {
            return;
        }
        let Some(socket_lease) = self.socket_lease.as_mut() else {
            return;
        };
        socket_lease.release_binding(0);
        self.binding_released = true;
    }

    fn restore_reserved_binding(&mut self) {
        if !self.binding_released {
            return;
        }
        let Some(socket_lease) = self.socket_lease.as_mut() else {
            return;
        };
        socket_lease
            .rebind_binding(0)
            .expect("rebind reserved netcat socket");
        self.binding_released = false;
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Call sites build owned diagnostic context strings for this panic path."
    )]
    fn panic_with_child_diagnostics(
        &mut self,
        context: String,
        observed_status: Option<ExitStatus>,
    ) -> ! {
        let (status, stdout, stderr) = if let Some(status) = observed_status {
            let (stdout, stderr) = self.collect_exited_output();
            (status, stdout, stderr)
        } else {
            let output = self.kill_and_collect_in_place();
            (output.status, output.stdout, output.stderr)
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

fn closed_loopback_tcp_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind temporary listener");
    let addr = listener.local_addr().expect("temporary listener address");
    drop(listener);
    addr
}

fn spawn_netcat(args: &[&str]) -> SpawnedNetcat {
    spawn_netcat_owned(args.iter().map(|arg| (*arg).to_owned()).collect(), None)
}

fn spawn_netcat_with_reserved_bind(kind: ReservedSocketKind, args: &[&str]) -> SpawnedNetcat {
    let socket_lease = reserve_sockets(&[kind]);
    let bind_addr = socket_lease.addr(0).to_string();
    let mut command_args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    command_args.splice(
        0..0,
        ["--kompact-config".to_owned(), bind_reuse_config_string()],
    );
    command_args.extend(["--bind".to_owned(), bind_addr]);
    spawn_netcat_owned(command_args, Some(socket_lease))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "The helper owns command arguments and optional leases for spawned-process lifetime management."
)]
fn spawn_netcat_owned(
    command_args: Vec<String>,
    socket_lease: Option<ReservedSocketLease>,
) -> SpawnedNetcat {
    let mut command = Command::new(env!("CARGO_BIN_EXE_netcat"));
    command.args(&command_args);
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
        socket_lease,
        binding_released: false,
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
