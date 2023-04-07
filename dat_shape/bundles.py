from collections.abc import Callable
from dataclasses import dataclass
from io import BytesIO
from pathlib import PurePosixPath
from typing import Dict, Optional
from zstandard import ZstdCompressor, ZstdDecompressor

from . import poe_util

import codecs
import ndjson
import ooz
import struct

class BundleIndex:
    def __init__(self, index_data):
        zfh = BytesIO(index_data)
        # with index_path.open('rb') as zfh:
        index_bundle = CompressedBundle(zfh)
        fh = BytesIO(index_bundle.decompress_all())

        self.bundles = []
        bundle_count, = readf(fh, '<I')
        for _ in range(bundle_count):
            bnamelen, = readf(fh, '<I')
            bnameraw = fh.read(bnamelen)
            bname = bnameraw.decode('UTF-8')
            bunclen = readf(fh, '<I')
            self.bundles.append(BundleRecord(
                name=bname, uncompressed_size=bunclen))

        self.files = []
        file_count, = readf(fh, '<I')
        for _ in range(file_count):
            path_hash, bundle_index, file_offset, file_size = readf(
                fh, '<QIII')
            self.files.append(FileRecord(
                path_hash=path_hash, bundle_index=bundle_index, file_offset=file_offset, file_size=file_size))

        self.path_reps = []
        path_rep_count, = readf(fh, '<I')
        for _ in range(path_rep_count):
            hash, offset, size, recursive_size = readf(fh, '<QIII')
            self.path_reps.append(
                PathRep(hash=hash, offset=offset, size=size, recursive_size=recursive_size))
        self.path_comp = fh.read()

def readf(fh: BytesIO, fmt: str):
    return struct.unpack(fmt, fh.read(struct.calcsize(fmt)))


def readfi(fh: BytesIO, fmt: str, count: int):
    return struct.iter_unpack(fmt, fh.read(struct.calcsize(fmt) * count))


@dataclass
class BundleRecord:
    name: str
    uncompressed_size: int

    def bin_path(self):
        return PurePosixPath(f'Bundles2/{self.name}.bundle.bin')


@dataclass
class FileRecord:
    path_hash: int
    bundle_index: int
    file_offset: int
    file_size: int


@dataclass
class PathRep:
    hash: int
    offset: int
    size: int
    recursive_size: int


class CompressedBundle:
    def __init__(self, fh):
        self.fh = fh
        self.uncompressed_size, self.total_payload_size, head_payload_size = readf(
            fh, '<III')
        first_file_encode, unk10, uncompressed_size2, total_payload_size2, block_count, self.uncompressed_block_granularity = readf(fh,
                                                                                                                                    '<IIQQII')
        fh.seek(4*4, 1)
        self.block_sizes = list(
            map(lambda x: x[0], readfi(fh, '<I', block_count)))
        self.data_start = fh.tell()

    def decompress_all(self):
        ret = bytearray()
        self.fh.seek(self.data_start)
        for i, bsize in enumerate(self.block_sizes):
            if i+1 != len(self.block_sizes):
                usize = self.uncompressed_block_granularity
            else:
                usize = self.uncompressed_size - i * self.uncompressed_block_granularity
            ret.extend(ooz.decompress(self.fh.read(bsize), usize))
        return ret
    
def _cut_ntmbs(slice):
    for i, b in enumerate(slice):
        if b == 0:
            return slice[:i].tobytes(), slice[i+1:]
    raise ValueError


def generate_path_hash_table(index: BundleIndex):
    ret: Dict[int, str] = {}

    path_data = CompressedBundle(BytesIO(index.path_comp)).decompress_all()
    path_view = memoryview(path_data)
    running_string_size = 0
    for rep in index.path_reps:
        slice = path_view[rep.offset:rep.offset+rep.size]
        base_phase = False
        bases = []
        while len(slice):
            cmd, = struct.unpack('<I', slice[:4])
            slice = slice[4:]

            # toggle phase on zero command word
            if cmd == 0:
                base_phase = not base_phase
                if base_phase:
                    bases.clear()
                continue

            # otherwise build a base or emit a string
            s, slice = _cut_ntmbs(slice)
            if cmd <= len(bases):
                s = bases[cmd-1] + s.decode('UTF-8')
            else:
                s = s.decode('UTF-8')

            if base_phase:
                bases.append(s)
            else:
                running_string_size += len(s)
                hash = poe_util.hash_file_path(s)
                ret[hash] = s

    return ret

def enumerate_path_table(index: BundleIndex, filter: Optional[Callable[[str], bool]] = None):
    path_data = CompressedBundle(BytesIO(index.path_comp)).decompress_all()
    path_view = memoryview(path_data)
    for rep in index.path_reps:
        slice = path_view[rep.offset:rep.offset+rep.size]
        base_phase = False
        bases = []
        while len(slice):
            cmd, = struct.unpack('<I', slice[:4])
            slice = slice[4:]

            # toggle phase on zero command word
            if cmd == 0:
                base_phase = not base_phase
                if base_phase:
                    bases.clear()
                continue

            # otherwise build a base or emit a string
            s, slice = _cut_ntmbs(slice)
            if cmd <= len(bases):
                s = bases[cmd-1] + s.decode('UTF-8')
            else:
                s = s.decode('UTF-8')

            if base_phase:
                bases.append(s)
            elif not filter or filter(s):
                hash = poe_util.hash_file_path(s)
                yield (hash, s)
