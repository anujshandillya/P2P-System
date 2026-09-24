"""Optional 1 GiB end-to-end test; samples client RSS and streams verification."""
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from integration import Cluster
from transfer import Client

with tempfile.TemporaryDirectory(prefix='p2p-large-') as directory:
    c = Cluster(directory, Path(sys.argv[1] if len(sys.argv) > 1 else '.').resolve())
    clients = []
    try:
        c.start(0); c.start(1)
        for name in ['seed', 'download']: c.command(0, 'create_user', name, 'pass')
        clients = [Client(c), Client(c)]
        a, b = clients
        a.send('login seed pass'); b.send('login download pass')
        a.send('create_group large'); b.send('join_group large'); a.send('accept_request large download')
        path = Path(directory) / 'one-gib.bin'
        with path.open('wb') as stream: stream.truncate(1024**3)
        out = Path(directory) / 'destination'; out.mkdir()
        started = time.monotonic()
        a.send(f'upload_file large "{path}"', 'File published', timeout=120)
        hashing = time.monotonic() - started
        b.send(f'download_file large "{path.name}" "{out}"', 'Download queued')
        # Wait for at least one verified piece, then remove both trackers. Already
        # authorized direct transfers must continue through this outage.
        time.sleep(0.5)
        b.send('show_downloads', '[D] [large]')
        c.stop(0); c.stop(1)
        deadline = time.monotonic() + 180
        peaks = [0, 0]
        while not (out / path.name).exists():
            assert time.monotonic() < deadline, b.output
            for i, client in enumerate(clients):
                rss = subprocess.check_output(['ps', '-o', 'rss=', '-p', str(client.process.pid)], text=True).strip()
                peaks[i] = max(peaks[i], int(rss))
            time.sleep(0.2)
        transfer = time.monotonic() - started - hashing
        def digest(path):
            result = hashlib.sha1()
            with path.open('rb') as stream:
                for block in iter(lambda: stream.read(1024*1024), b''): result.update(block)
            return result.hexdigest()
        assert (out / path.name).stat().st_size == 1024**3
        assert digest(path) == digest(out / path.name)
        b.send('show_downloads', '[C] [large] one-gib.bin')
        c.start(0); c.start(1)
        print(f'PASS: both trackers unavailable during transfer; 1 GiB byte-content hash verified; preparation={hashing:.2f}s transfer+verification={transfer:.2f}s peak sampled RSS KiB={peaks}', flush=True)
    finally:
        for client in clients: client.close()
        c.close()
