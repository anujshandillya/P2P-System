import hashlib
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
from integration import Cluster, free_port, rpc, eventually

class Client:
    def __init__(self, c):
        self.port = free_port()
        self.lines = queue.Queue()
        self.output = []
        self.process = subprocess.Popen([str(c.binaries / 'client.out'), f'127.0.0.1:{self.port}', str(c.configs[0])],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=c.env)
        def read():
            for line in self.process.stdout:
                self.output.append(line)
                self.lines.put(line)
        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()
    def send(self, command, expected='OK:', timeout=25):
        self.process.stdin.write(command + '\n'); self.process.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try: line = self.lines.get(timeout=0.1)
            except queue.Empty: continue
            if expected in line: return line
            if line.startswith('ERROR'): raise AssertionError((command, line))
        raise AssertionError((command, expected, self.output))
    def close(self):
        if self.process.poll() is None:
            self.process.stdin.write('quit\n'); self.process.stdin.flush()
            try: self.process.wait(timeout=30)
            except subprocess.TimeoutExpired: self.process.kill(); self.process.wait(); raise
        self.reader.join(timeout=2)
        errors = self.process.stderr.read()
        assert self.process.returncode == 0 and 'Sanitizer' not in errors and 'runtime error:' not in errors, errors


def peer_tests(c):
    c.start(0); c.start(1)
    for name in ['seed', 'reader', 'stranger']: c.command(0, 'create_user', name, 'pass')
    seed = Client(c)
    try:
        seed.send('login seed pass')
        seed.send('create_group files')
        token = c.login(0, 'reader')
        stranger = c.login(0, 'stranger')
        c.command(0, 'join_group', 'files', token=token)
        seed.send('accept_request files reader')
        data = bytes(range(256)) * 4097
        path = c.directory / 'source file.bin'; path.write_bytes(data)
        digest = hashlib.sha1(data).hexdigest()
        seed.send(f'upload_file files "{path}"', 'File published')
        eventually(lambda: len(c.command(0, 'discover', 'files', path.name, token=token)) == 6)
        request = ['BITFIELD', 'files', path.name, digest, token]
        assert rpc(seed.port, request) == [b'OK', digest.encode(), b'111']
        response = rpc(seed.port, ['PIECE', 'files', path.name, digest, token, '2'])
        assert response == [b'OK', digest.encode(), b'2', data[2*524288:]]
        assert rpc(seed.port, ['PIECE', 'files', path.name, digest, token, '3'])[0] == b'ERROR'
        assert rpc(seed.port, ['BITFIELD', 'files', path.name, digest, stranger])[0] == b'ERROR'
        seed.send(f'stop_share files "{path.name}"', 'Sharing stopped')
        assert rpc(seed.port, request)[0] == b'ERROR'
        seed.send(f'upload_file files "{path}"', 'File published')
        path.write_bytes(b'changed')
        assert rpc(seed.port, ['PIECE', 'files', path.name, digest, token, '0'])[0] == b'ERROR'
        assert rpc(seed.port, request)[0] == b'ERROR'
        print('PASS: real peer serving, discovery heartbeat, permissions, stop-share and changed-source rejection', flush=True)
    finally: seed.close()

def download_tests(c):
    c.start(0); c.start(1)
    for name in ['a', 'b', 'c']: c.command(0, 'create_user', name, 'pass')
    clients = [Client(c) for _ in range(3)]
    try:
        for client, name in zip(clients, ['a', 'b', 'c']): client.send(f'login {name} pass')
        a, b, d = clients
        a.send('create_group g')
        for client, name in [(b, 'b'), (d, 'c')]:
            client.send('join_group g'); a.send(f'accept_request g {name}')
        destinations = [c.directory / 'b', c.directory / 'c']
        for path in destinations: path.mkdir()
        data = bytes(range(256)) * 18001
        path = c.directory / 'shared file.bin'; path.write_bytes(data)
        empty = c.directory / 'empty'; empty.write_bytes(b'')
        a.send(f'upload_file g "{path}"', 'File published')
        a.send(f'upload_file g "{empty}"', 'File published')
        b.send(f'download_file g "{path.name}" "{destinations[0]}"', 'Download queued')
        b.send(f'download_file g empty "{destinations[0]}"', 'Download queued')
        eventually(lambda: (destinations[0] / path.name).exists(), timeout=30)
        eventually(lambda: (destinations[0] / 'empty').exists(), timeout=30)
        assert (destinations[0] / path.name).read_bytes() == data
        assert (destinations[0] / 'empty').read_bytes() == b''
        b.send('show_downloads', '[C] [g] shared file.bin')
        b.send(f'download_file g "{path.name}" "{destinations[0]}"', 'Destination already exists')
        # Completed downloader becomes an independently discoverable seeder.
        time.sleep(3)
        a.send(f'stop_share g "{path.name}"', 'Sharing stopped')
        d.send(f'download_file g "{path.name}" "{destinations[1]}"', 'Download queued')
        eventually(lambda: (destinations[1] / path.name).exists(), timeout=30)
        assert (destinations[1] / path.name).read_bytes() == data
        print('PASS: concurrent downloads, empty file, final verification, safe destinations and downloaded-file seeding', flush=True)
    finally:
        for client in clients: client.close()

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='p2p-transfer-') as directory:
        c = Cluster(directory, Path(sys.argv[1] if len(sys.argv) > 1 else '.').resolve())
        try:
            peer_tests(c)
            c.close()
            nested = Path(directory) / "downloads"; nested.mkdir()
            c = Cluster(nested, c.binaries)
            download_tests(c)
        finally: c.close()
