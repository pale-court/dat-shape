from io import BytesIO

import codecs
import ndjson
import pathlib
import zstandard as zstd

class Manifest:
    def __init__(self):
        self.by_path = {}
        self.by_path_hash = {}
        self.by_content_hash = {}

def parse(payload):
    mf = Manifest()
    with zstd.ZstdDecompressor().stream_reader(payload, read_across_frames=True) as dfh:
        for rec in ndjson.reader(codecs.getreader('UTF-8')(dfh)):
            mf.by_path[rec['path']] = rec
            mf.by_path_hash[rec['phash']] = rec
            if (chash := rec['sha256']) in mf.by_content_hash:
                mf.by_content_hash[chash].append(rec)
            else:
                mf.by_content_hash[chash] = [rec]
    
    return mf