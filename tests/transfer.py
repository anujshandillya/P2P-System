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

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='p2p-transfer-') as directory:
        c = Cluster(directory, Path(sys.argv[1] if len(sys.argv) > 1 else '.').resolve())
        try: peer_tests(c)
        finally: c.close()
