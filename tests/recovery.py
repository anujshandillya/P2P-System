"""Deterministic faulty peers exercise retries, worker limits and partial seeding."""
import hashlib
from pathlib import Path
import socket
import socketserver
import struct
import sys
import tempfile
import threading
import time
from integration import Cluster, free_port, rpc, exact, decode, frame, eventually
from publication import metadata
from transfer import Client

class Peer:
    def __init__(self, files, corrupt=False, delay=0.04):
        self.files, self.corrupt, self.delay = files, corrupt, delay
        self.requests = []
        self.active = 0
        self.maximum = 0
        self.lock = threading.Lock()
        self.abort = False
        self.parity = None
        self.gate = threading.Event()
        self.gate.set()
        owner = self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    header = exact(self.request, 8)
                    fields = [part.decode() for part in decode(exact(self.request, struct.unpack('!I', header[4:])[0]))]
                    kind, group, name, digest, token, *extra = fields
                    data = owner.files[name]
                    if kind == 'BITFIELD':
                        bits = ''.join('1' if owner.parity is None or i % 2 == owner.parity else '0' for i in range((len(data)+524287)//524288))
                        self.request.sendall(frame(['OK', digest, bits]))
                        return
                    with owner.lock:
                        owner.active += 1
                        owner.maximum = max(owner.maximum, owner.active)
                    try:
                        index = int(extra[0])
                        owner.requests.append((name, index, time.monotonic()))
                        if index >= 4: owner.gate.wait(timeout=10)
                        time.sleep(owner.delay)
                        if owner.abort: return
                        piece = data[index*524288:(index+1)*524288]
                        if owner.corrupt: piece = bytes([piece[0] ^ 1]) + piece[1:]
                        self.request.sendall(frame(['OK', digest, str(index), piece]))
                    finally:
                        with owner.lock: owner.active -= 1
                except (OSError, EOFError, KeyError, ValueError): pass
        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True
        self.server = Server(('127.0.0.1', 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()


def run(c):
    c.start(0); c.start(1)
    files = {'one.bin': bytes(range(256)) * (2048*50), 'two.bin': b'z' * (524288*20+1)}
    peers = [Peer(files, corrupt=True), Peer(files, delay=0.15)]
    for peer in peers: peer.gate.clear()
    clients = []
    try:
        tokens = []
        for i, peer in enumerate(peers):
            name = f'seed{i}'; c.command(0, 'create_user', name, 'pass')
            token = c.command(0, 'login', name, 'pass', f'127.0.0.1:{peer.port}')[3]; tokens.append(token)
        c.command(0, 'create_group', 'g', token=tokens[0])
        c.command(0, 'join_group', 'g', token=tokens[1]); c.command(0, 'accept_request', 'g', 'seed1', token=tokens[0])
        for token in tokens:
            for name, data in files.items(): c.command(0, 'upload_file', 'g', metadata(name, data), token=token)
            for i in [0, 1]: c.command(i, 'heartbeat', token=token)
        for name in ['download', 'watch']:
            c.command(0, 'create_user', name, 'pass')
            client = Client(c); clients.append(client)
            client.send(f'login {name} pass'); client.send('join_group g')
            c.command(0, 'accept_request', 'g', name, token=tokens[0])
        client, watcher = clients
        out = c.directory / 'out'; out.mkdir()
        client.send(f'download_file g one.bin "{out}"', 'Download queued')
        client.send(f'download_file g two.bin "{out}"', 'Download queued')
        client.send(f'download_file g one.bin "{out}"', 'already has an active download')
        # Query the real downloader while it is still missing pieces.
        watch_token = c.login(0, 'watch')
        def partial():
            result = rpc(client.port, ['BITFIELD', 'g', 'one.bin', hashlib.sha1(files['one.bin']).hexdigest(), watch_token])
            return len(result) == 3 and b'1' in result[2] and b'0' in result[2]
        eventually(partial, timeout=12)
        # A corrupt source then disappears; the healthy source must finish both jobs.
        for peer in peers: peer.gate.set()
        peers[0].abort = True
        for name, data in files.items():
            eventually(lambda name=name: (out / name).exists(), timeout=45)
            assert (out / name).read_bytes() == data
        assert peers[0].requests and peers[1].requests
        assert peers[1].maximum > 1 and peers[1].maximum <= 4
        first_file_finished = max(t for name, _, t in peers[1].requests if name == 'one.bin')
        assert any(name == 'two.bin' and t < first_file_finished for name, _, t in peers[1].requests)
        print('PASS: corrupt pieces rejected, dropped peers retried, partial seeding, multi-peer jobs, four-worker bound and fairness', flush=True)
        # Force the final whole-file check to disagree with individually valid pieces.
        bad = bytearray(metadata('bad.bin', b'abc'))
        expected = hashlib.sha1(b'abc').hexdigest().encode()
        where = bad.find(expected); bad[where:where+40] = b'0'*40
        peers[1].files['bad.bin'] = b'abc'
        c.command(0, 'upload_file', 'g', bytes(bad), token=tokens[1])
        c.command(0, 'heartbeat', token=tokens[1])
        client.send(f'download_file g bad.bin "{out}"', 'Download queued')
        time.sleep(2)
        client.send('show_downloads', 'Whole-file SHA1 mismatch')
        assert not (out / 'bad.bin').exists()
        # No sources: job waits, and logout cancels it without exposing a final file.
        c.command(0, 'upload_file', 'g', metadata('unavailable.bin', b'abc'), token=tokens[1])
        c.command(0, 'stop_share', 'g', 'unavailable.bin', token=tokens[1])
        client.send(f'download_file g unavailable.bin "{out}"', 'Download queued')
        client.send('logout', 'Logged out')
        client.send('show_downloads', 'Session ended')
        assert not (out / 'unavailable.bin').exists()
        print('PASS: final hash mismatch fails safely; logout cancels unavailable downloads', flush=True)
    finally:
        for peer in peers: peer.gate.set()
        for client in clients: client.close()
        for peer in peers: peer.close()

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='p2p-recovery-') as directory:
        c = Cluster(directory, Path(sys.argv[1] if len(sys.argv) > 1 else '.').resolve())
        try: run(c)
        finally: c.close()
