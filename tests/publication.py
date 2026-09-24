import hashlib
from pathlib import Path
import sys
import tempfile
import uuid
from integration import Cluster, frame, eventually
import struct


def packed(fields):
    values = [x.encode() if isinstance(x, str) else x for x in fields]
    return struct.pack('!I', len(values)) + b''.join(struct.pack('!I', len(x)) + x for x in values)


def metadata(name, data):
    return packed([name, str(len(data)), '524288', hashlib.sha1(data).hexdigest(),
                   *[hashlib.sha1(data[i:i+524288]).hexdigest() for i in range(0, len(data), 524288)]])


def run(c):
    c.start(0); c.start(1)
    for name in ['alice', 'bob', 'outsider']:
        c.command(0, 'create_user', name, 'pass')
    a, b, other = [c.login(0, name) for name in ['alice', 'bob', 'outsider']]
    c.command(0, 'create_group', 'g', token=a)
    c.command(1, 'join_group', 'g', token=b)
    c.command(0, 'accept_request', 'g', 'bob', token=a)
    meta = metadata('shared file.bin', b'abc')
    c.command(0, 'upload_file', 'g', meta, token=other, expected='FORBIDDEN')
    c.command(0, 'upload_file', 'g', b'bad', token=a, expected='INVALID_ARGUMENT')
    request_id = uuid.uuid4().hex
    c.command(0, 'upload_file', 'g', meta, token=a, request_id=request_id)
    c.command(1, 'upload_file', 'g', meta, token=a, request_id=request_id)
    assert c.command(1, 'list_files', 'g', token=b)[4:] == ['shared file.bin']
    assert len(c.command(1, 'discover', 'g', 'shared file.bin', token=b)) == 5
    for tracker in [0, 1]: c.command(tracker, 'heartbeat', token=a)
    assert len(c.command(1, 'discover', 'g', 'shared file.bin', token=b)) == 6
    c.command(1, 'upload_file', 'g', metadata('shared file.bin', b'changed'), token=b, expected='CONFLICT')
    c.command(1, 'upload_file', 'g', meta, token=b)
    c.command(0, 'heartbeat', token=b)
    assert len(c.command(0, 'discover', 'g', 'shared file.bin', token=a)) == 7
    c.command(1, 'stop_share', 'g', 'shared file.bin', token=a)
    assert len(c.command(0, 'discover', 'g', 'shared file.bin', token=a)) == 6
    c.command(0, 'authorize', 'g', 'shared file.bin', hashlib.sha1(b'abc').hexdigest(), b, token=a)
    c.command(0, 'logout', token=b)
    assert c.command(1, 'list_files', 'g', token=a)[4:] == []
    c.command(0, 'authorize', 'g', 'shared file.bin', hashlib.sha1(b'abc').hexdigest(), b, token=a, expected='FORBIDDEN')
    c.command(0, 'upload_file', 'g', meta, token=a)
    c.stop(0); c.stop(1); c.start(0); c.start(1)
    assert c.command(0, 'list_files', 'g', token=a)[4:] == ['shared file.bin']
    assert len(c.command(0, 'discover', 'g', 'shared file.bin', token=a)) == 5
    a = c.login(0, 'alice')
    assert c.command(1, 'list_files', 'g', token=a)[4:] == []
    # A full 1 GiB manifest must fit the request, journal and replay limits.
    digest = hashlib.sha1(b'x').hexdigest()
    large = packed(['large', str(1024**3), '524288', digest, *([digest] * 2048)])
    c.command(0, 'upload_file', 'g', large, token=a)
    c.stop(0); c.start(0)
    assert 'large' in c.command(0, 'list_files', 'g', token=a)[4:]
    c.command(0, 'leave_group', 'g', token=a)
    b = c.login(1, 'bob')
    assert c.command(1, 'list_files', 'g', token=b)[4:] == []
    print('PASS: publication, conflicts, permissions, multiple sources, revocation, restart and 1 GiB metadata')

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='p2p-publication-') as directory:
        c = Cluster(directory, Path(sys.argv[1] if len(sys.argv) > 1 else '.').resolve())
        try: run(c)
        finally: c.close()
