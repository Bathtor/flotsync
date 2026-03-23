#!/usr/bin/env python3

from __future__ import annotations

import argparse
import socket
import sys
from dataclasses import dataclass


ROOT_BODY = b"hello from flotsync_io\n"
NOT_FOUND_BODY = b"not found\n"
CASE_NAMES = ("get_root", "head_root", "post_echo", "get_missing")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a small h11-based smoke suite against the flotsync_io HTTP example server."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument(
        "--port",
        required=True,
        type=int,
        help="Server port printed by the http_server example",
    )
    parser.add_argument(
        "--body",
        default="hello",
        help="Request body to use for the post_echo case",
    )
    parser.add_argument(
        "--case",
        action="append",
        choices=CASE_NAMES,
        help="Run only the named case. Repeat to run multiple cases. Defaults to all cases.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print raw h11 events after each case summary",
    )
    return parser.parse_args()


class HarnessError(Exception):
    def __init__(self, stage: str, error: OSError):
        self.stage = stage
        self.error = error
        super().__init__(f"{stage}: {error}")


@dataclass(frozen=True)
class Case:
    name: str
    method: str
    target: str
    request_headers: list[tuple[str, str]]
    request_body: bytes
    expected_status: int
    expected_reason: str
    expected_headers: dict[str, str]
    expected_body: bytes


def load_h11():
    try:
        import h11
    except ModuleNotFoundError:
        print(
            "FAIL: missing Python dependency 'h11'. Install it with: python3 -m pip install h11"
        )
        raise
    return h11


def build_cases(host: str, echo_body: bytes) -> list[Case]:
    return [
        Case(
            name="get_root",
            method="GET",
            target="/",
            request_headers=[("Host", host)],
            request_body=b"",
            expected_status=200,
            expected_reason="OK",
            expected_headers={
                "connection": "close",
                "content-length": str(len(ROOT_BODY)),
                "content-type": "text/plain; charset=utf-8",
            },
            expected_body=ROOT_BODY,
        ),
        Case(
            name="head_root",
            method="HEAD",
            target="/",
            request_headers=[("Host", host)],
            request_body=b"",
            expected_status=200,
            expected_reason="OK",
            expected_headers={
                "connection": "close",
                "content-length": str(len(ROOT_BODY)),
                "content-type": "text/plain; charset=utf-8",
            },
            expected_body=b"",
        ),
        Case(
            name="post_echo",
            method="POST",
            target="/echo",
            request_headers=[
                ("Host", host),
                ("Content-Length", str(len(echo_body))),
            ],
            request_body=echo_body,
            expected_status=200,
            expected_reason="OK",
            expected_headers={
                "connection": "close",
                "content-length": str(len(echo_body)),
                "content-type": "application/octet-stream",
            },
            expected_body=echo_body,
        ),
        Case(
            name="get_missing",
            method="GET",
            target="/missing",
            request_headers=[("Host", host)],
            request_body=b"",
            expected_status=404,
            expected_reason="Not Found",
            expected_headers={
                "connection": "close",
                "content-length": str(len(NOT_FOUND_BODY)),
                "content-type": "text/plain; charset=utf-8",
            },
            expected_body=NOT_FOUND_BODY,
        ),
    ]


def request_label(case: Case) -> str:
    label = f"SEND {case.method} {case.target}"
    if case.request_body:
        label += f" body={len(case.request_body)}"
    return label


def run_request(
    h11,
    host: str,
    port: int,
    case: Case,
) -> tuple[object | None, bytes, list[str]]:
    conn = h11.Connection(our_role=h11.CLIENT)
    events: list[str] = []
    response = None
    response_body = bytearray()

    try:
        sock = socket.create_connection((host, port))
    except OSError as error:
        raise HarnessError("connect", error) from error
    try:
        try:
            sock.sendall(
                conn.send(
                    h11.Request(
                        method=case.method,
                        target=case.target,
                        headers=case.request_headers,
                    )
                )
            )
            if case.request_body:
                sock.sendall(conn.send(h11.Data(data=case.request_body)))
            sock.sendall(conn.send(h11.EndOfMessage()))
        except OSError as error:
            raise HarnessError("send", error) from error

        while True:
            event = conn.next_event()
            if event is h11.NEED_DATA:
                try:
                    data = sock.recv(4096)
                except OSError as error:
                    raise HarnessError("receive", error) from error
                conn.receive_data(data)
                if not data:
                    break
                continue

            events.append(repr(event))
            if isinstance(event, h11.Response):
                response = event
            elif isinstance(event, h11.Data):
                response_body.extend(event.data)
            elif isinstance(event, h11.EndOfMessage):
                break
    finally:
        sock.close()

    return response, bytes(response_body), events


def headers_dict(response) -> dict[str, str]:
    headers: dict[str, str] = {}
    for name, value in response.headers:
        headers[name.decode("ascii").lower()] = value.decode("ascii")
    return headers


def print_step(label: str, passed: bool, detail: str = "") -> None:
    status = "ok" if passed else "FAIL"
    suffix = f" {detail}" if detail else ""
    print(f"{label} ... {status}{suffix}")


def print_failure_detail(label: str, expected: str, actual: str) -> None:
    print(f"  {label}")
    print(f"    expected: {expected}")
    print(f"    actual:   {actual}")


def print_case_summary(
    case: Case,
    host: str,
    port: int,
    response,
    response_body: bytes,
) -> int:
    print(f"CASE {case.name}")
    print_step(f"CONNECT {host}:{port}", True)
    print_step(request_label(case), True)

    if response is None:
        print_step("RECV response", False, "no HTTP response received")
        print("RESULT FAIL")
        return 1

    headers = headers_dict(response)
    status_code = response.status_code
    reason = response.reason.decode("ascii", errors="replace")
    server = headers.get("server")
    overall_ok = True

    expected_status = f"{case.expected_status} {case.expected_reason}"
    status_ok = (
        status_code == case.expected_status and reason == case.expected_reason
    )
    print_step(
        f"CHECK status {expected_status}",
        status_ok,
        f"(got {status_code} {reason})" if not status_ok else "",
    )
    if not status_ok:
        overall_ok = False
        print_failure_detail("status", expected_status, f"{status_code} {reason}")

    for header_name, expected_value in sorted(case.expected_headers.items()):
        actual_value = headers.get(header_name)
        header_ok = actual_value == expected_value
        print_step(
            f"CHECK header {header_name}={expected_value}",
            header_ok,
            f"(got {actual_value!r})" if not header_ok else "",
        )
        if not header_ok:
            overall_ok = False
            print_failure_detail(
                f"{header_name} header",
                expected_value,
                repr(actual_value),
            )

    body_ok = response_body == case.expected_body
    print_step("CHECK echoed body", body_ok)
    if not body_ok:
        overall_ok = False
        print_failure_detail("body", repr(case.expected_body), repr(response_body))

    if not overall_ok:
        print("RESPONSE HEADERS")
        for name, value in sorted(headers.items()):
            print(f"  {name}: {value}")
        if server is not None:
            print(f"SERVER HEADER {server}")
        print("RESULT FAIL")
        return 1

    print("RESULT PASS")
    return 0


def run_case(h11, host: str, port: int, case: Case, verbose: bool) -> int:
    try:
        response, response_body, events = run_request(h11, host, port, case)
    except HarnessError as error:
        print(f"CASE {case.name}")
        if error.stage == "connect":
            print_step(f"CONNECT {host}:{port}", False, str(error.error))
        elif error.stage == "send":
            print_step(f"CONNECT {host}:{port}", True)
            print_step(request_label(case), False, str(error.error))
        else:
            print_step(f"CONNECT {host}:{port}", True)
            print_step(request_label(case), True)
            print_step("RECV response", False, str(error.error))
        print("RESULT FAIL")
        print()
        return 1

    exit_code = print_case_summary(case, host, port, response, response_body)
    if verbose:
        print("RAW EVENTS")
        for event in events:
            print(f"  {event}")
    print()
    return exit_code


def main() -> int:
    args = parse_args()
    echo_body = args.body.encode("utf-8")
    selected_cases = set(args.case or CASE_NAMES)

    try:
        h11 = load_h11()
    except ModuleNotFoundError:
        return 1

    exit_code = 0
    for case in build_cases(args.host, echo_body):
        if case.name not in selected_cases:
            continue
        case_exit_code = run_case(h11, args.host, args.port, case, args.verbose)
        exit_code = max(exit_code, case_exit_code)

    print("SUITE PASS" if exit_code == 0 else "SUITE FAIL")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
