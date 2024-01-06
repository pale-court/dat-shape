from atomicwrites import atomic_write
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath

from . import bundles
from . import manifests

import json
import os
import requests
import sys
import zstandard as zstd

poe_root_url = 'http://10.0.5.106'
poe_data_url = f'{poe_root_url}/poe-data/'
poe_meta_url = f'{poe_root_url}/poe-meta/'

proj_dir = Path('/home/inya/code/dat-shape')
output_dir = Path('/mnt/inya/dat-meta')

shape_path = proj_dir / 'shape.json'
global_path = output_dir / 'global.json'

@dataclass
class Options:
    push_to_git: bool
    profile_run: bool

def run():
    opts = Options(push_to_git=False, profile_run=False)
    shape = json.loads(shape_path.read_text())

    global_meta = {
        'builds': {},
    }
    if global_path.exists():
        global_meta = json.loads(global_path.read_text())

    public_url = f'{poe_meta_url}builds/public'
    r = requests.get(public_url)
    if r.status_code != 200:
        print(f'could not fetch public builds: {r.status_code}', file=sys.stderr)
        sys.exit(1)

    builds = json.loads(r.text)

    builds_to_process = []
    for build_key in sorted(builds, key=lambda x: int(x)):
        b = builds[build_key]
        if (
            build_key not in global_meta['builds']
            or (last := global_meta['builds'][build_key])['shape_revision'] < shape['revision']
            or last.get('game_version') != b.get('version')
            ):
            print(last, b)
            builds_to_process.append(build_key)

        global_meta['builds'][build_key] = {
            'shape_revision': shape['revision'],
            'time_updated': b['time_updated'],
            'game_version': b['version'],
        }

    test_build_id = '5540437'
    if opts.profile_run and len(builds_to_process) == 0:
        builds_to_process.append(test_build_id)

    for build_id in builds_to_process:
        if int(build_id) < 1465491:
            # builds older than this do not have DAT64 files
            continue

        has_bundles = int(build_id) >= 5528345

        build = builds[build_id]
        print(build)
        manifest_id = build['manifests']['238961']['gid']

        mf = None
        for mf_url in [f'{poe_data_url}idxz/238961/{manifest_id}/bundled', f'{poe_data_url}idxz/238961/{manifest_id}/loose']:
            r = requests.get(mf_url)
            if r.status_code == 200:
                mf = manifests.parse(r.content)
                break

        if not mf:
            print(f'While processing {manifest_id}: HTTP status {r.status_code}')
            continue

        dat64_by_bundle = {}
        def dat64_filter(path: str):
            return path.endswith('.dat64') and path.count('/') == 1

        dat64_recs = {path: mf.by_path[path] for path in mf.by_path if dat64_filter(path)}
        if len(dat64_recs) == 0:
            continue

        build_output = {
            "files": {}
        }

        build_output_path = output_dir / 'builds'
        build_output_path.mkdir(parents=True, exist_ok=True)

        for path, rec in dat64_recs.items():
            path = PurePosixPath(path)
            file_url =  f"{poe_data_url}cad/{rec['sha256']}"
            r = requests.get(file_url)
            fdata = r.content
            info = DatInfo(fdata, path.stem)
            build_output['files'][str(path)] = info.as_dict()

        output_path = build_output_path / f'build-{build_id}.json'
        with atomic_write(output_path) as fh:
            json.dump(build_output, fh, sort_keys=True, indent=2)
            output_path.unlink(missing_ok=True)
        output_path.chmod(0o644)

    with atomic_write(global_path) as fh:
        json.dump(global_meta, fh, sort_keys=True, indent=2)
        global_path.unlink(missing_ok=True)
    global_path.chmod(0o644)

    if opts.push_to_git:
        os.chdir(output_dir)
        os.system('git add -A')
        os.system('git commit -m "autocommit"')
        os.system('git push -u origin main')


class DatInfo:
    def __init__(self, data, name):
        fh = BytesIO(data)
        self.row_count, = bundles.readf(fh, '<I')
        self.var_offset = data.index(b'\xBB\xbb\xBB\xbb\xBB\xbb\xBB\xbb')
        self.var_size = len(data) - self.var_offset
        self.fixed_size = self.var_offset - 4
        self.row_width = self.fixed_size // self.row_count if self.row_count > 0 else 0
        self.ids = None

    def __str__(self):
        return f'row_count={self.row_count}, row_width={self.row_width}, var_size={self.var_size}'
    
    def as_dict(self):
        ret = {
            "fixed_size": self.fixed_size,
            "row_count": self.row_count,
            "row_width": self.row_width,
            "var_offset": self.var_offset,
            "var_size": self.var_size,
        }
        if self.ids:
            ret["ids"] = self.ids

        return ret

if __name__ == '__main__':
    run()
