#!/usr/bin/env python3
"""Black-box tests of the actual C++ binaries; Python is only a test driver."""
import argparse
import concurrent.futures
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import tempfile
import threading
import time
import uuid


def encode(fields):
    data = [f.encode() if isinstance(f, str) else f for f in fields]
    return struct.pack("!I", len(data)) + b"".join(struct.pack("!I", len(f)) + f for f in data)


def decode(payload):
    count, = struct.unpack_from("!I", payload)
    pos, fields = 4, []
    for _ in range(count):
        length, = struct.unpack_from("!I", payload, pos)
        pos += 4
        fields.append(payload[pos:pos + length])
        pos += length
    assert pos == len(payload)
    return fields


def frame(fields):
    payload = encode(fields)
    return b"P2P1" + struct.pack("!I", len(payload)) + payload


def exact(sock, length):
    out = b""
    while len(out) < length:
        part = sock.recv(length - len(out))
        if not part:
            raise ConnectionError("Unexpected EOF")
        out += part
    return out


def receive(sock):
    header = exact(sock, 8)
    assert header[:4] == b"P2P1"
    return decode(exact(sock, struct.unpack("!I", header[4:])[0]))


def rpc(port, fields, fragmented=False):
    with socket.create_connection(("127.0.0.1", port), timeout=8) as sock:
        data = frame(fields)
        if fragmented:
            for byte in data:
                sock.sendall(bytes([byte]))
        else:
            sock.sendall(data)
        return receive(sock)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually(fn, timeout=12):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except (OSError, ConnectionError, AssertionError) as exc:
            last = exc
        time.sleep(0.08)
    raise AssertionError(f"Condition not reached within {timeout}s: {last}")


class Proxy:
    """Disconnectable bidirectional link; partitions affect tracker traffic only."""
    def __init__(self, target):
        self.target = target
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.enabled = threading.Event()
        self.enabled.set()
        self.closed = threading.Event()
        self.connections = set()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.closed.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self.relay, args=(client,), daemon=True).start()

    def relay(self, client):
        remote = None
        try:
            if not self.enabled.is_set():
                return
            remote = socket.create_connection(("127.0.0.1", self.target), timeout=1)
            with self.lock:
                self.connections.update((client, remote))
            while self.enabled.is_set() and not self.closed.is_set():
                ready, _, _ = select.select([client, remote], [], [], 0.1)
                for source in ready:
                    data = source.recv(65536)
                    if not data:
                        return
                    (remote if source is client else client).sendall(data)
        except OSError:
            pass
        finally:
            with self.lock:
                self.connections.discard(client)
                self.connections.discard(remote)
            client.close()
            if remote:
                remote.close()

    def partition(self):
        self.enabled.clear()
        with self.lock:
            for sock in self.connections:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self):
        self.partition()
        self.closed.set()
        self.listener.close()
        self.thread.join(timeout=2)


class Cluster:
    def __init__(self, directory, binaries, proxies=False):
        self.directory = Path(directory)
        self.binaries = binaries
        self.ports = [free_port(), free_port()]
        while self.ports[1] == self.ports[0]:
            self.ports[1] = free_port()
        self.processes = [None, None]
        self.outputs = []
        self.proxies = [Proxy(p) for p in self.ports] if proxies else []
        self.configs = []
        for i in range(2):
            ports = self.ports.copy()
            if proxies:
                ports[1 - i] = self.proxies[1 - i].port
            path = self.directory / f"tracker{i + 1}.txt"
            path.write_text("".join(f"127.0.0.1 {p}\n" for p in ports))
            self.configs.append(path)
        self.env = dict(os.environ, P2P_CLUSTER_KEY="integration-test-key")

    def start(self, index):
        path = self.directory / f"tracker{index + 1}.log"
        output = path.open("ab")
        self.outputs.append(output)
        self.processes[index] = subprocess.Popen(
            [str(self.binaries / "tracker.out"), str(self.configs[index]), str(index + 1)],
            stdin=subprocess.PIPE, stdout=output, stderr=output, env=self.env)
        def ready():
            proc = self.processes[index]
            if proc.poll() is not None:
                raise AssertionError(path.read_text())
            return rpc(self.ports[index], ["PING"])[0] == b"PONG"
        eventually(ready)

    def stop(self, index, crash=False):
        proc = self.processes[index]
        if proc is None:
            return
        if proc.poll() is None:
            if crash:
                proc.kill()
            else:
                proc.stdin.write(b"quit\n")
                proc.stdin.flush()
            try:
                proc.wait(timeout=12)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
                raise AssertionError("Tracker shutdown hung")
        if not crash:
            assert proc.returncode == 0, (self.directory / f"tracker{index + 1}.log").read_text()
        proc.stdin.close()
        self.processes[index] = None

    def command(self, index, command, *args, token="", request_id=None, expected="OK", fragmented=False):
        request_id = request_id or uuid.uuid4().hex
        fields = ["CLIENT", request_id, token, command, *args]
        deadline = time.monotonic() + 15
        while True:
            response = [part.decode() for part in rpc(self.ports[index], fields, fragmented)]
            if response[0] != "RETRY_LATER":
                break
            assert time.monotonic() < deadline, "Tracker remained busy"
            time.sleep(0.025)
        assert response[0] == expected, (command, expected, response)
        return response

    def login(self, index, user, password="pass"):
        return self.command(index, "login", user, password, "127.0.0.1:6100")[3]

    def close(self):
        for i in range(2):
            self.stop(i)
        for proxy in self.proxies:
            proxy.close()
        for output in self.outputs:
            output.close()
        for path in self.directory.glob("*.log"):
            content = path.read_text()
            assert "ERROR: AddressSanitizer" not in content, content
            assert "ERROR: LeakSanitizer" not in content, content
            assert "runtime error:" not in content, content
            assert "WARNING: ThreadSanitizer" not in content, content


def core_tests(cluster):
    c = cluster
    c.start(0)
    c.start(1)
    c.command(0, "create_user", "alice", "pass", fragmented=True)
    c.command(1, "create_user", "bob", "pass")
    c.command(0, "create_user", "charlie", "pass")
    c.command(1, "create_user", "alice", "pass", expected="ALREADY_EXISTS")
    c.command(0, "login", "alice", "wrong", "127.0.0.1:6100", expected="AUTH_FAILED")
    c.command(0, "login", "missing", "pass", "127.0.0.1:6100", expected="AUTH_FAILED")
    c.command(0, "create_group", "x", expected="UNAUTHENTICATED")
    c.command(0, "list_groups", expected="UNAUTHENTICATED")
    c.command(0, "create_user", "bad id", "pass", expected="INVALID_ARGUMENT")
    c.command(0, "login", "alice", "pass", "invalid", expected="INVALID_ARGUMENT")
    a, b, outsider = c.login(1, "alice"), c.login(0, "bob"), c.login(1, "charlie")
    c.command(0, "login", "alice", "pass", "127.0.0.1:6100", token=a, expected="ALREADY_LOGGED_IN")
    c.command(0, "create_group", "systems", token=a)
    c.command(1, "create_group", "systems", token=b, expected="ALREADY_EXISTS")
    assert c.command(1, "list_groups", token=b)[4:] == ["systems"]
    c.command(1, "join_group", "missing", token=b, expected="NOT_FOUND")
    c.command(1, "join_group", "systems", token=b)
    c.command(0, "join_group", "systems", token=b, expected="ALREADY_PENDING")
    c.command(1, "list_requests", "systems", token=b, expected="FORBIDDEN")
    c.command(1, "accept_request", "systems", "bob", token=outsider, expected="FORBIDDEN")
    assert c.command(0, "list_requests", "systems", token=a)[4:] == ["bob"]
    c.command(1, "accept_request", "systems", "bob", token=a)
    assert c.command(0, "list_requests", "systems", token=a)[4:] == []
    c.command(0, "join_group", "systems", token=b, expected="ALREADY_MEMBER")
    c.command(1, "leave_group", "systems", token=a)
    c.command(0, "list_requests", "systems", token=a, expected="FORBIDDEN")
    c.command(0, "list_requests", "systems", token=b)
    c.command(0, "leave_group", "systems", token=outsider, expected="NOT_MEMBER")
    c.command(1, "leave_group", "systems", token=b)
    assert c.command(1, "list_groups", token=b)[4:] == []
    c.command(0, "logout", token=b)
    c.command(1, "list_groups", token=b, expected="UNAUTHENTICATED")
    b = c.login(1, "bob")
    newer_b = c.login(0, "bob")
    c.command(1, "list_groups", token=b, expected="UNAUTHENTICATED")
    c.command(1, "list_groups", token=newer_b)
    print("PASS: all user/group commands, authentication, permissions, replicated sessions", flush=True)

    duplicate = uuid.uuid4().hex
    c.command(0, "create_group", "once", token=a, request_id=duplicate)
    c.command(1, "create_group", "once", token=a, request_id=duplicate)
    c.command(1, "create_group", "different", token=a, request_id=duplicate, expected="REQUEST_ID_CONFLICT")
    # Lose the response at the client transport, then retry on the other tracker.
    lost = uuid.uuid4().hex
    def applied_without_response():
        with socket.create_connection(("127.0.0.1", c.ports[0])) as sock:
            sock.sendall(frame(["CLIENT", lost, a, "create_group", "lost-response"]))
        events = rpc(c.ports[0], ["SYNC", "integration-test-key"])[1:]
        return any(decode(decode(event)[3])[0] == lost.encode() for event in events)
    eventually(applied_without_response)
    c.command(1, "create_group", "lost-response", token=a, request_id=lost)
    print("PASS: idempotent retries and lost-response recovery", flush=True)

    def create(i):
        return c.command(i % 2, "create_group", f"parallel-{i}", token=a)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(create, range(18)))
    groups = c.command(1, "list_groups", token=a)[4:]
    assert sum(g.startswith("parallel-") for g in groups) == 18
    # Exactly one winner for a concurrent group-name collision.
    def collide(i):
        fields = ["CLIENT", uuid.uuid4().hex, a, "create_group", "collision"]
        while True:
            result = rpc(c.ports[i % 2], fields)
            if result[0] != b"RETRY_LATER":
                return result[0]
            time.sleep(0.03)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(collide, range(4)))
    assert results.count(b"OK") == 1, results
    assert results.count(b"ALREADY_EXISTS") == 3, results
    print("PASS: concurrent requests through both trackers", flush=True)

    c.stop(0, crash=True)
    assert c.command(1, "create_group", "during-primary-outage", token=a)[2] == "degraded"
    c.start(0)
    assert "during-primary-outage" in c.command(0, "list_groups", token=a)[4:]
    c.stop(1, crash=True)
    c.command(0, "create_group", "during-secondary-outage", token=a)
    c.start(1)
    assert "during-secondary-outage" in c.command(1, "list_groups", token=a)[4:]
    c.stop(0)
    c.stop(1)
    # A torn final append is safely discarded on restart.
    wal = Path(str(c.configs[0]) + ".tracker1.wal")
    with wal.open("ab") as stream:
        stream.write(b"WAL")
    c.start(1)  # Tracker 2 can restart alone, retaining sessions and metadata.
    assert "during-secondary-outage" in c.command(1, "list_groups", token=a)[4:]
    c.start(0)
    assert c.command(0, "list_groups", token=a)[4:] == c.command(1, "list_groups", token=a)[4:]
    print("PASS: either tracker fails, both restart, journal recovery, torn-tail repair", flush=True)

    malformed = [b"BAD!" + struct.pack("!I", 4) + b"\0" * 4,
                 b"P2P1" + struct.pack("!I", 17 * 1024 * 1024),
                 b"P2P1" + struct.pack("!I", 4) + struct.pack("!I", 100000),
                 b"P2P1" + struct.pack("!I", 8) + struct.pack("!II", 0, 99)]
    for data in malformed:
        with socket.create_connection(("127.0.0.1", c.ports[0]), timeout=4) as sock:
            sock.sendall(data)
            assert receive(sock)[0] == b"ERROR"
    # EOF in a frame and a client that stops sending must not crash the server.
    with socket.create_connection(("127.0.0.1", c.ports[0]), timeout=4) as sock:
        sock.sendall(b"P2")
        sock.shutdown(socket.SHUT_WR)
        assert receive(sock)[0] == b"ERROR"
    with socket.create_connection(("127.0.0.1", c.ports[0]), timeout=4) as sock:
        sock.sendall(b"P2")
        assert receive(sock)[0] == b"ERROR"
    assert rpc(c.ports[0], ["SYNC", "wrong-key"])[0] == b"FORBIDDEN"
    c.command(0, "unknown", token=a, expected="UNKNOWN_COMMAND")
    c.command(0, "accept_request", "only-one-arg", token=a, expected="INVALID_ARGUMENT")
    for _ in range(40):
        assert rpc(c.ports[1], ["PING"])[0] == b"PONG"
    print("PASS: partial TCP I/O, malformed frames, deadlines, replication authentication", flush=True)

    # Exercise the actual CLI, including quotes, local parsing, EOF logout and failover.
    endpoint = f"127.0.0.1:{free_port()}"
    upload = c.directory / "file with spaces.txt"
    upload.write_bytes(b"abc")
    script = 'create_user cli_user "space password"\nlogin cli_user "space password"\ncreate_group cli_group\nlist_groups\nlogout\nlogin cli_user "space password"\n'
    script += f'upload_file cli_group "{upload}"\n'
    result = subprocess.run([str(c.binaries / "client.out"), endpoint, str(c.configs[0])],
                            input=script, text=True, capture_output=True, timeout=30, env=c.env)
    assert result.returncode == 0 and "ERROR" not in result.stdout, result
    assert "File prepared; publication is not implemented yet." in result.stdout, result.stdout
    assert "Pieces: 1" in result.stdout and "a9993e364706816aba3e25717850c26c9cd0d89d" in result.stdout
    assert "runtime error:" not in result.stderr and "Sanitizer" not in result.stderr, result
    assert "cli_group" in result.stdout and result.stdout.count("OK: Logged out") == 2, result.stdout
    c.stop(0)
    result = subprocess.run([str(c.binaries / "client.out"), endpoint, str(c.configs[0])],
                            input='login cli_user "space password"\nlist_groups\nquit\n',
                            text=True, capture_output=True, timeout=30, env=c.env)
    assert result.returncode == 0 and "cli_group" in result.stdout and "ERROR" not in result.stdout, result
    assert "runtime error:" not in result.stderr and "Sanitizer" not in result.stderr, result
    c.start(0)
    print("PASS: real client CLI, quoted passwords, EOF logout, automatic tracker failover", flush=True)


def ownership_tests(c):
    c.start(0)
    c.start(1)
    for user in ("owner", "zara", "amy"):
        c.command(0, "create_user", user, "pass")
    owner, early, late = (c.login(0, user) for user in ("owner", "zara", "amy"))

    def populate(group):
        c.command(0, "create_group", group, token=owner)
        # Request and alphabetical order deliberately differ from acceptance order.
        c.command(1, "join_group", group, token=late)
        c.command(0, "join_group", group, token=early)
        c.command(1, "accept_request", group, "zara", token=owner)
        c.command(0, "accept_request", group, "amy", token=owner)

    populate("succession")
    # Recover join order from existing command history before the first transfer.
    c.stop(0)
    c.stop(1)
    c.start(0)
    c.start(1)
    c.stop(0, crash=True)
    leave_id = uuid.uuid4().hex
    result = c.command(1, "leave_group", "succession", token=owner, request_id=leave_id)
    assert result[1] == "Left group; new owner: zara" and result[2] == "degraded", result
    c.start(0)
    c.command(0, "leave_group", "succession", token=owner, request_id=leave_id)
    for tracker in (0, 1):
        c.command(tracker, "list_requests", "succession", token=early)
        c.command(tracker, "list_requests", "succession", token=late, expected="FORBIDDEN")
        c.command(tracker, "list_requests", "succession", token=owner, expected="FORBIDDEN")
    c.command(0, "join_group", "succession", token=owner)
    c.command(1, "accept_request", "succession", "owner", token=owner, expected="FORBIDDEN")
    assert c.command(0, "list_requests", "succession", token=early)[4:] == ["owner"]
    c.command(1, "accept_request", "succession", "owner", token=early)
    assert c.command(0, "leave_group", "succession", token=early)[1] == "Left group; new owner: amy"
    assert c.command(1, "leave_group", "succession", token=late)[1] == "Left group; new owner: owner"
    c.command(0, "leave_group", "succession", token=owner)
    assert c.command(1, "list_groups", token=owner)[4:] == []

    populate("rejoin")
    c.command(0, "leave_group", "rejoin", token=early)
    c.command(1, "join_group", "rejoin", token=early)
    c.command(0, "accept_request", "rejoin", "zara", token=owner)
    # A rejoining member goes to the end; even a duplicate acceptance cannot move it.
    c.command(1, "accept_request", "rejoin", "amy", token=owner, expected="NOT_FOUND")
    for proxy in c.proxies:
        proxy.partition()
    result = c.command(1, "leave_group", "rejoin", token=owner)
    assert result[1] == "Left group; new owner: amy" and result[2] == "degraded", result
    c.command(1, "join_group", "rejoin", token=owner)
    # The isolated primary still has the old owner until histories are exchanged.
    c.command(0, "list_requests", "rejoin", token=owner)
    for proxy in c.proxies:
        proxy.enabled.set()
    def converged():
        return (rpc(c.ports[0], ["SYNC", "integration-test-key"])[1:] ==
                rpc(c.ports[1], ["SYNC", "integration-test-key"])[1:])
    eventually(converged)
    c.stop(0)
    c.stop(1)
    c.start(0)
    c.start(1)
    for tracker in (0, 1):
        assert c.command(tracker, "list_requests", "rejoin", token=late)[4:] == ["owner"]
        c.command(tracker, "list_requests", "rejoin", token=owner, expected="FORBIDDEN")
    assert c.command(0, "leave_group", "rejoin", token=late)[1] == "Left group; new owner: zara"
    assert c.command(1, "list_requests", "rejoin", token=early)[4:] == ["owner"]
    # Pending requests do not become owners and do not keep an empty group alive.
    c.command(0, "leave_group", "rejoin", token=early)
    assert c.command(1, "list_groups", token=owner)[4:] == []
    c.command(1, "create_group", "rejoin", token=owner)
    assert c.command(0, "list_requests", "rejoin", token=owner)[4:] == []
    print("PASS: oldest-member succession, rejoin order, permissions, failover, partition and restart", flush=True)


def partition_tests(c):
    c.start(0)
    c.start(1)
    c.command(0, "create_user", "owner", "pass")
    token = c.login(0, "owner")
    # Fully stop traffic in BOTH directions, while client ports remain reachable.
    for proxy in c.proxies:
        proxy.partition()
    time.sleep(0.2)
    c.command(0, "create_group", "left", token=token)
    c.command(1, "create_group", "right", token=token)
    c.command(0, "create_user", "same_user", "left-password")
    c.command(1, "create_user", "same_user", "right-password")
    left = c.login(0, "same_user", "left-password")
    right = c.login(1, "same_user", "right-password")
    c.command(0, "create_group", "contested", token=left)
    c.command(1, "create_group", "contested", token=right)
    for proxy in c.proxies:
        proxy.enabled.set()
    def converged():
        left_events = rpc(c.ports[0], ["SYNC", "integration-test-key"])[1:]
        right_events = rpc(c.ports[1], ["SYNC", "integration-test-key"])[1:]
        return left_events == right_events
    eventually(converged)
    assert c.command(0, "list_groups", token=token)[4:] == ["contested", "left", "right"]
    assert c.command(1, "list_groups", token=token)[4:] == ["contested", "left", "right"]
    winner = c.login(1, "same_user", "left-password")
    c.command(0, "login", "same_user", "right-password", "127.0.0.1:6100", expected="AUTH_FAILED")
    c.command(1, "list_groups", token=right, expected="UNAUTHENTICATED")
    c.command(0, "list_requests", "contested", token=winner)
    c.command(1, "leave_group", "contested", token=winner)
    c.stop(0)
    c.stop(1)
    c.start(0)
    c.start(1)
    assert c.command(1, "list_groups", token=token)[4:] == ["left", "right"]
    print("PASS: real network partition, union recovery, deterministic conflicts, replay after restart", flush=True)


def startup_tests(binaries, directory):
    root = Path(directory)
    config = root / "invalid.txt"
    config.write_text("127.0.0.1 12345\n")
    for binary, args in [("tracker.out", [str(config), "1"]), ("tracker.out", [str(config), "3"]),
                         ("client.out", ["bad-endpoint", str(config)])]:
        result = subprocess.run([str(binaries / binary), *args], capture_output=True, text=True, timeout=5)
        assert result.returncode != 0, result
    config.write_text(f"127.0.0.1 {free_port()}\n127.0.0.1 {free_port()}\n")
    journal = root / "corrupt.wal"
    journal.write_bytes(b"NOT_A_VALID_HEADER")
    result = subprocess.run([str(binaries / "tracker.out"), str(config), "1"], capture_output=True, text=True,
                            env=dict(os.environ, P2P_STATE_FILE=str(journal)), timeout=5)
    assert result.returncode != 0 and "Corrupt journal" in result.stderr, result
    print("PASS: invalid startup/configuration and corrupt journal rejection", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", default=".")
    args = parser.parse_args()
    binaries = Path(args.bin_dir).resolve()
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="p2p-interim-tests-") as directory:
        root = Path(directory)
        for name, fn, proxies in [("core", core_tests, False), ("ownership", ownership_tests, True),
                                  ("partition", partition_tests, True)]:
            path = root / name
            path.mkdir()
            cluster = Cluster(path, binaries, proxies)
            try:
                fn(cluster)
            except Exception:
                for log in path.glob("*.log"):
                    print(f"--- {log.name} ---\n{log.read_text()}", flush=True)
                raise
            finally:
                cluster.close()
        startup_tests(binaries, root)
    print(f"All interim integration tests passed ({time.monotonic() - started:.1f}s).", flush=True)


if __name__ == "__main__":
    main()
