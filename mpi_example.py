# import os
import sys
import enum
import traceback
import numpy as np
# import joblib
# import os
# import os.path
import pandas as pd
from pathlib import Path
# from dask.distributed import LocalCluster, Client
from mpi4py import MPI


filepath = 'test.h5'

class Status(enum.Enum):
    Requesting_work = 0
    Work_assigned = 1
    Work_done = 2
    Work_failure = 3


class WorkObject(object):
    def __init__(self, number):
        self._number = number

    @property
    def number(self):
        return self._number

    def __eq__(self, other):
        return self.number == other.number

    def __hash__(self):
        return hash(self.number)



def do_work(work_object):

    result = np.square(work_object.number)

    result_dict = {
        'number': [work_object.number],
        'square': [result]
    }

    data_frame = pd.DataFrame(result_dict, columns=columns)

    return Status.Work_done, data_frame


if __name__ == '__main__':

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        status = MPI.Status()

        # Queues made of Work Object class
        waiting_queue = set()
        running_queue = set()
        finished_queue = set()
        failure_queue = set()

        hdf5_store = pd.HDFStore(filepath)

        ## Populate Waiting Queue with work objects

        for i in np.arange(100, dtype=np.int64):
            work_object = WorkObject(i)
            waiting_queue.add(work_object)

        if list(hdf5_store.keys()):
            for index, row in hdf5_store['data'].iterrows():
                work_object = WorkObject(
                    row['number']
                )

                waiting_queue.discard(work_object)

        for worker in range(1, size):
            if len(waiting_queue) == 0:
                sys.stdout.write('Waiting Queue Empty\n')
                break
            item = waiting_queue.pop()
            work_type = {
                'job': 'work',
                'item': item
            }
            comm.send(work_type, dest=worker, tag=1)
            running_queue.add(item)

        sys.stdout.write('Finished First Phase\n')

        count = 0

        while len(running_queue) != 0 or len(waiting_queue) != 0:
            try:
                status_dict = comm.recv(
                    source=MPI.ANY_SOURCE,
                    tag=2,
                    status=status
                )
            except Exception:
                traceback.print_exc()
                sys.stdout.write('Failed to get\n')
                sys.exit(1)

            sender = status.Get_source()
            jobstatus = status_dict['status']
            item = status_dict['item']
            sys.stdout.write(
                'Sender: {} item: {} Status: {}\n'.format(
                    sender, item.number, jobstatus.value
                )
            )
            running_queue.discard(item)
            if jobstatus == Status.Work_done:
                sys.stdout.write('Success: {}\n'.format(item.number))
                finished_queue.add(item)
                if count == 0:
                    sys.stdout.write('First Time\n')
                    status_dict['data_frame'].to_hdf(
                        filepath, 'data', format='t'
                    )
                    count += 1
                else:
                    sys.stdout.write('Second Time\n')
                    hdf5_store.append(
                        'data',
                        status_dict['data_frame'],
                        format='t',
                        data_columns=True
                    )
            else:
                sys.stdout.write('Failure: {}\n'.format(item.number))
                failure_queue.add(item)

            if len(waiting_queue) != 0:
                new_item = waiting_queue.pop()
                work_type = {
                    'job': 'work',
                    'item': new_item
                }
                comm.send(work_type, dest=sender, tag=1)

        for worker in range(1, size):
            work_type = {
                'job': 'stopwork'
            }
            comm.send(work_type, dest=worker, tag=1)

    if rank > 0:

        columns = [
            'number',
            'square'
        ]

        while 1:
            work_type = comm.recv(source=0, tag=1)

            if work_type['job'] != 'work':
                break

            item = work_type['item']

            sys.stdout.write('Recieved {}\n'.format(item.number))

            status, data_frame = do_work(item)

            comm.send(
                {'status': status, 'item': item, 'data_frame': data_frame},
                dest=0, tag=2
            )
