"""Compare C++ whole-file and piece digests with an independent implementation."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile

probe = str(Path(sys.argv[1]).resolve())
piece = 512 * 1024
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    for size in [0, 1, 55, 56, 63, 64, 65, piece - 1, piece, piece + 1, 3 * piece + 17]:
        data = (bytes(range(256)) * ((size + 255) // 256))[:size]
        path = root / 'file with spaces.bin'
        path.write_bytes(data)
        result = subprocess.run([probe, str(path)], text=True, capture_output=True, check=True)
        expected = [path.name, str(size), hashlib.sha1(data).hexdigest()]
        expected += [hashlib.sha1(data[i:i+piece]).hexdigest() for i in range(0, size, piece)]
        assert result.stdout.splitlines() == expected, (size, result.stdout)
    oversized = root / 'oversized'
    with oversized.open('wb') as stream:
        stream.truncate(1024**3 + 1)
    fifo = root / 'fifo'
    os.mkfifo(fifo)
    for path in [root, root / 'missing', oversized, fifo]:
        result = subprocess.run([probe, str(path)], capture_output=True, timeout=3)
        assert result.returncode == 1, path
print('File inspection: all hashes, piece boundaries, and invalid-path checks passed')
