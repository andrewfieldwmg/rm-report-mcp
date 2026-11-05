"""Build a deterministic package only from the files tested by a passing replay."""

import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import tarfile

from contract import verify_replay, verify_schemas


def package(receipt_path, output, version):
    if not re.fullmatch(r'(?:[0-9a-f]{40}|local-[a-z0-9-]{1,50})', version):
        raise ValueError('Version must be a commit SHA or a labelled local version')
    receipt = json.loads(receipt_path.read_text(encoding='utf8'))
    if receipt['status'] != 'passed' or receipt['dataset'] != 'synthetic' or receipt['broker_stopped'] is not True:
        raise ValueError('A passing bounded replay receipt is required')
    verify_replay(receipt['observed'])
    verify_schemas(receipt.get('schemas', {}))
    files = {name: Path(__file__).with_name(name).read_bytes() for name in ('contract.py', 'pipeline.py', 'run_local.py')}
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in files.items()}
    if hashes != receipt['source_sha256']:
        raise ValueError('Package source does not match the tested files')
    manifest = {'version': version, 'source_sha256': hashes, 'spark': receipt['spark_version'],
                'kafka': receipt['kafka_release'], 'jar_sha256': receipt['jar_sha256']}
    files['manifest.json'] = (json.dumps(manifest, indent=2, sort_keys=True) + '\n').encode()
    with output.open('xb') as raw, gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0) as zipped:
        with tarfile.open(mode='w', fileobj=zipped) as tar:
            for name, content in sorted(files.items()):
                entry = tarfile.TarInfo(name)
                entry.mode, entry.size = 0o644, len(content)
                tar.addfile(entry, io.BytesIO(content))
    print(json.dumps({'version': version, 'package_sha256': hashlib.sha256(output.read_bytes()).hexdigest()}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--version', required=True)
    args = parser.parse_args()
    package(args.receipt, args.output, args.version)
