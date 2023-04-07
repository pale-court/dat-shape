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
poe_data_url = f'{poe_root_url}/poe-data'
poe_index_url = f'{poe_root_url}/poe-index'
poe_meta_url = f'{poe_root_url}/poe-meta'

proj_dir = Path('/home/zao/code/dat-shape')
output_dir = Path('/home/zao/dat-meta')

shape_path = proj_dir / 'shape.json'
global_path = output_dir / 'global.json'

@dataclass
class Options:
    push_to_git: bool
    profile_run: bool

def run():
    opts = Options(push_to_git=False, profile_run=False)
    shape = json.loads(shape_path.read_text())
    old_global = None
    if global_path.exists():
        old_global = json.loads(global_path.read_text())

    public_url = f'{poe_meta_url}/builds/public'
    r = requests.get(public_url)
    if r.status_code != 200:
        print(f'could not fetch public builds: {r.status_code}', file=sys.stderr)
        sys.exit(1)

    builds = json.loads(r.text)

    global_meta = {
        'builds': {},
    }
    builds_to_process = []
    for build_key in sorted(builds, key=lambda x: int(x)):
        if old_global:
            if build_key not in old_global['builds'] or old_global['builds'][build_key]['shape_revision'] < shape['revision']:
                builds_to_process.append(build_key)

        b = builds[build_key]
        global_meta['builds'][build_key] = {
            'shape_revision': shape['revision'],
            'time_updated': b['time_updated'],
            'game_version': b['version'],
        }

    test_build_id = '5540437'
    if opts.profile_run and len(builds_to_process) == 0:
        builds_to_process.append(test_build_id)

    for build_id in builds_to_process:
        build = builds[build_id]
        print(build)
        manifest_id = build['manifests']['238961']
        loose_url = f'{poe_index_url}/238961/{manifest_id}-loose.ndjson.zst'
        r = requests.get(loose_url)
        if r.status_code == 200:
            mf = manifests.parse(r.content)

        idxbin_entry = mf.by_path['Bundles2/_.index.bin']
        hash = idxbin_entry['sha256']
        comp = idxbin_entry['comp']
        idxbin_url = f'{poe_data_url}/{hash[:2]}/{hash}.bin{".zst" if comp else ""}'
        r = requests.get(idxbin_url)
        index_payload = r.content
        if comp:
            with zstd.ZstdDecompressor().stream_reader(index_payload, read_across_frames=True) as dfh:
                index_payload = dfh.read()
        index = bundles.BundleIndex(index_payload)
        index_files_by_phash = {rec.path_hash: rec for rec in index.files}
        dat64_by_bundle = {}
        def dat64_filter(path : str):
            return path.endswith('.dat64') and path.count('/') == 1
            pp = PurePosixPath(path)
            return pp.suffix == ".dat64" and len(pp.parents) == 2

        for phash, path in bundles.enumerate_path_table(index, dat64_filter):
            rec = index_files_by_phash[phash]
            entry = {"path": PurePosixPath(path), "file_record": rec}
            if (bid := rec.bundle_index) in dat64_by_bundle:
                dat64_by_bundle[bid].append(entry)
            else:
                dat64_by_bundle[bid] = [entry]

        build_output = {
            "files": {}
        }

        # iterate for each bundle
        for bid in dat64_by_bundle:
            bid_files = dat64_by_bundle[bid]
            bundle_rec = index.bundles[bid]
            bundle_mf_entry = mf.by_path[str(bundle_rec.bin_path())]
            bhash = bundle_mf_entry['sha256']
            bcomp = bundle_mf_entry['comp']
            bundle_url = f'{poe_data_url}/{bhash[:2]}/{bhash}.bin{".zst" if bcomp else ""}'
            r = requests.get(bundle_url)
            index_payload = r.content
            if bcomp:
                with zstd.ZstdDecompressor().stream_reader(index_payload, read_across_frames=True) as dfh:
                    index_payload = dfh.read()
            bundle = bundles.CompressedBundle(BytesIO(index_payload))
            bundle_data = bundle.decompress_all()
            
            # slice out each file
            for file in bid_files:
                frec = file['file_record']
                file_end = frec.file_offset + frec.file_size
                fdata = bundle_data[frec.file_offset:file_end]
                
                # process and shape it
                info = DatInfo(fdata, file['path'].stem)
                # print(f'{str(file["path"])}: {info!s}')
                build_output['files'][str(file['path'])] = info.as_dict()

            output_path = output_dir / f'build-{build_id}.json'
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
        os.system('git add *.json')
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