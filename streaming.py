import uuid
import time
import os

import numpy as np

from streamz_ext import Stream
from streamz import filenames

from databroker import Broker


def gen_docs_from_file(fn):
    with open(fn, 'r') as f:
        header = [next(f) for x in range(17)]
    data = np.loadtxt(fn, skiprows=17)
    data_keys = ['energy', 'i0', 'it', 'ir', 'iff', 'pba2_adc7']

    start_uid = str(uuid.uuid4())
    new_start = {'uid': start_uid,
                 'time': time.time(),
                 'parent_uids': header[7].split(': ')[-1]}
    yield 'start', new_start

    descriptor_uid = str(uuid.uuid4())
    new_descriptor = dict(
        uid=descriptor_uid,
        time=time.time(),
        run_start=start_uid,
        name='primary',
        data_keys={k: {'source': 'analysis',
                       'dtype': str(type(xx)),
                       'shape': getattr(xx, 'shape', [])
                       } for k, xx in zip(data_keys, data.T[1:])})
    yield 'descriptor', new_descriptor

    new_event = dict(uid=str(uuid.uuid4()),
                     time=time.time(),
                     timestamps={k: time.time() for k in data_keys},
                     descriptor=descriptor_uid,
                     filled={k: True for k in data_keys},
                     data={k: v for k, v in zip(data_keys, data.T[1:])},
                     seq_num=1)
    yield 'event', new_event

    new_stop = dict(uid=str(uuid.uuid4()),
                    time=time.time(),
                    run_start=start_uid)
    yield 'stop', new_stop


if __name__ == '__main__':

    root = '/hi'
    an_db = Broker.named('iss-analysis')

    source = filenames(os.path.join(root, '*.txt'))
    source.starsink(an_db.insert)
