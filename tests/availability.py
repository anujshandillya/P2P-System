import hashlib
from pathlib import Path
import sys
import tempfile
import time
from integration import Cluster, eventually, rpc
from publication import metadata
from transfer import Client
from recovery import Peer


def run(c):
    c.start(0); c.start(1)
    files = {'partial.bin': b'P' * (524288*8+1), 'race.bin': b'R' * (524288*12)}
    peers = [Peer(files, delay=0.06), Peer(files, delay=0.06)]
    clients = []
    try:
        tokens = []
        for i, peer in enumerate(peers):
            name = f'p{i}'; c.command(0, 'create_user', name, 'pass')
            token = c.command(0, 'login', name, 'pass', f'127.0.0.1:{peer.port}')[3]; tokens.append(token)
            # Deliberately make each initial piece assignment choose the wrong peer.
            peer.parity = 1-i
        c.command(0, 'create_group', 'g', token=tokens[0])
        c.command(0, 'join_group', 'g', token=tokens[1]); c.command(0, 'accept_request', 'g', 'p1', token=tokens[0])
        for token in tokens:
            for name, data in files.items(): c.command(0, 'upload_file', 'g', metadata(name, data), token=token)
            for i in [0, 1]: c.command(i, 'heartbeat', token=token)
        c.command(0, 'create_user', 'd', 'pass')
        client = Client(c); clients.append(client)
        client.send('login d pass'); client.send('join_group g')
        c.command(0, 'accept_request', 'g', 'd', token=tokens[0])
        out = c.directory / 'out'; out.mkdir()
        # Tracker 1 fails before discovery and piece transfer.
        c.stop(0)
        client.send(f'download_file g partial.bin "{out}"', 'Download queued')
        eventually(lambda: (out/'partial.bin').exists(), timeout=30)
        assert (out/'partial.bin').read_bytes() == files['partial.bin']
        assert all(peer.requests for peer in peers)
        c.start(0)
        for token in tokens: c.command(0, 'heartbeat', token=token)
        # An unrelated file appearing during a transfer must survive finalization.
        for peer in peers: peer.gate.clear()
        client.send(f'download_file g race.bin "{out}"', 'Download queued')
        (out/'race.bin').write_bytes(b'keep me')
        for peer in peers: peer.gate.set()
        time.sleep(3)
        client.send('show_downloads', 'Cannot finalize download')
        assert (out/'race.bin').read_bytes() == b'keep me'
        # An idle crashed source drops out after its heartbeat lease.
        for token in tokens: c.command(0, 'heartbeat', token=token)
        before = c.command(0, 'discover', 'g', 'partial.bin', token=tokens[0])
        assert len(before) >= 7
        time.sleep(16)
        after = c.command(0, 'discover', 'g', 'partial.bin', token=tokens[0])
        assert all(f'127.0.0.1:{peer.port}' not in after[5:] for peer in peers)
        print('PASS: complementary partial peers, tracker failover, destination creation race and stale-peer expiry', flush=True)
    finally:
        for peer in peers: peer.gate.set()
        for client in clients: client.close()
        for peer in peers: peer.close()

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='p2p-availability-') as directory:
        c = Cluster(directory, Path(sys.argv[1] if len(sys.argv)>1 else '.').resolve())
        try: run(c)
        finally: c.close()
