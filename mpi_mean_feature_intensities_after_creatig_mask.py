# import os
import sys
import enum
import imageio
import skimage.color
import traceback
import numpy as np
# import joblib
# import os
# import os.path
import operator
import sunpy.io
import sunpy.map
import pandas as pd
from pathlib import Path
# from dask.distributed import LocalCluster, Client
from mpi4py import MPI
from skimage.morphology import closing, square
from utils import prepare_get_corresponding_images, \
    do_aiaprep, do_align, set_nan_to_non_sun, \
    get_date, do_thresholding, do_area_filtering


filepath = Path('/Volumes/Harsh 9599771751/HMIWorkAndData/intensity_results/results.h5')
list_of_directories = [Path('/Volumes/Harsh 9599771751/HMIWorkAndData/data/2014.01.01')]

# plage_en_masks_dict = None
# active_network_masks_dict = None
# sunspot_masks_dict = None


class Status(enum.Enum):
    Requesting_work = 0
    Work_assigned = 1
    Work_done = 2
    Work_failure = 3


def save_model(fout, model):
    pass


def populate_files(list_of_directories):

    hmi_files = list()
    aia_files = list()
    vis_files = list()

    for a_directory in list_of_directories:
        everything = a_directory.glob('**/*')

        aia_files.extend(
            [
                x for x in everything if x.is_file() and
                x.name.endswith('.fits') and x.name.startswith('aia')
            ]
        )

        everything = a_directory.glob('**/*')

        hmi_files.extend(
            [
                x for x in everything if x.is_file() and
                x.name.endswith('.fits') and x.name.startswith('hmi.m_720s')
            ]
        )

        everything = a_directory.glob('**/*')

        vis_files.extend(
            [
                x for x in everything if x.is_file() and
                x.name.endswith('.fits') and x.name.startswith('hmi.ic')
            ]
        )

    sys.stdout.write(
        'len(hmi_files): {}, len(aia_files): {}, len(vis_files): {}\n'.format(
            len(hmi_files), len(aia_files), len(vis_files)
        )
    )
    return hmi_files, aia_files, vis_files


class WorkObject(object):
    def __init__(self, hmi_file, aia_file, vis_file, julian_day):
        self._hmi_file = hmi_file
        self._aia_file = aia_file
        self._vis_file = vis_file
        self._julian_day = julian_day

    @property
    def hmi_file(self):
        return self._hmi_file

    @property
    def aia_file(self):
        return self._aia_file

    @property
    def vis_file(self):
        return self._vis_file

    @property
    def julian_day(self):
        return self._julian_day

    def __eq__(self, other):
        return self.julian_day == other.julian_day

    def __hash__(self):
        return hash(self.julian_day)


def get_plage_active_network_intensity(
    vis_data,
    vis_header,
    aia_file,
    sunspot_mask
):
    fill_nans = False
    aia_data, aia_header = sunpy.io.fits.read(
        aia_file
    )[1]
    aia_data, aia_header = do_aiaprep(
        aia_data, aia_header,
        fill_nans=fill_nans
    )

    aia_data, aia_header = do_align(
        vis_data, vis_header,
        aia_data, aia_header,
        fill_nans=fill_nans
    )

    aia_data, aia_total_pixels = set_nan_to_non_sun(
        aia_data,
        aia_header,
        factor=0.96,
        fill_nans=fill_nans,
        return_total_pixels=True
    )

    plage_mask = do_thresholding(
        aia_data,
        aia_header,
        k=1.71,
        op=operator.ge,
        value_1=1.0,
        radius_factor=0.96,
        k2=None,
        op2=None,
        value_2=None
    )

    plage_mask = closing(plage_mask, square(3))

    plage_mask = do_area_filtering(plage_mask)

    rr, cc = np.where(sunspot_mask == 1.0)

    plage_mask[rr, cc] = 0.0

    active_network_mask = do_thresholding(
        aia_data,
        aia_header,
        k=2,
        op=operator.ge,
        k2=6,
        op2=operator.ge,
        radius_factor=0.96,
        value_1=1.0,
        value_2=0.0
    )

    active_network_mask[rr, cc] = 0

    rr, cc = np.where(plage_mask == 1.0)

    active_network_mask[rr, cc] = 0

    plage_intensity = np.nansum(
        np.multiply(
            plage_mask,
            aia_data
        )
    )

    active_network_intensity = np.nansum(
        np.multiply(
            active_network_mask,
            aia_data
        )
    )

    no_of_pixel_plage_en = np.nansum(plage_mask)

    no_of_pixels_active_networks = np.nansum(active_network_mask)

    return no_of_pixel_plage_en, no_of_pixels_active_networks, \
        plage_intensity, active_network_intensity


def get_sunspot_intensity_and_total_pixels(
    vis_file
):
    fill_nans = False

    vis_data, vis_header = sunpy.io.fits.read(
        vis_file
    )[1]

    vis_data, vis_header = do_aiaprep(
        vis_data, vis_header,
        fill_nans=fill_nans
    )

    vis_data, vis_total_pixels = set_nan_to_non_sun(
        vis_data,
        vis_header,
        factor=0.96,
        fill_nans=fill_nans,
        return_total_pixels=True
    )

    sunspot_mask, invalid_result = do_thresholding(
        vis_data,
        vis_header,
        -5,
        operator.le,
        1.0,
        radius_factor=0.96,
        k2=None,
        op2=None,
        value_2=None
    )

    if invalid_result:
        sys.stdout.write(
            'Invalid Result for {}'.format(
                vis_file
            )
        )
        return False, None, None, \
            None, None, None, None

    no_pixel_sunspot = np.nansum(sunspot_mask)

    sunspot_intensity = np.nansum(
        np.multiply(
            sunspot_mask,
            vis_data
        )
    )

    return True, no_pixel_sunspot, vis_total_pixels, \
        sunspot_intensity, vis_data, vis_header, sunspot_mask


def do_work(work_object):

    s, a, b, c, d, e, f = get_sunspot_intensity_and_total_pixels(
        work_object.vis_file
    )

    if not s:
        return Status.Work_failure, None

    no_pixel_sunspot, vis_total_pixels = a, b

    sunspot_intensity, vis_data, vis_header = c, d, e

    sunspot_mask = f

    a, b, c, d = get_plage_active_network_intensity(
        vis_data,
        vis_header,
        work_object.aia_file,
        sunspot_mask
    )

    no_of_pixel_plage_en, no_of_pixels_active_networks = a, b

    plage_intensity, active_network_intensity = c, d

    data = {
        'date': [get_date(work_object.hmi_file).strftime('%Y-%m-%d')],
        'julian_day': [work_object.julian_day],
        'hmi_filename': [work_object.hmi_file.name],
        'hmi_ic_filename': [work_object.vis_file.name],
        'aia_filename': [work_object.aia_file.name],
        'no_of_pixel_sunspot': [no_pixel_sunspot],
        'sunspot_intensity': [sunspot_intensity],
        'no_of_pixel_plage_en': [no_of_pixel_plage_en],
        'plage_en_intensity': [plage_intensity],
        'no_of_pixels_active_networks': [no_of_pixels_active_networks],
        'active_networks_intensity': [active_network_intensity],
        'no_of_pixels_background': [0],
        'background_intensity': [0],
        'total_pixels': [vis_total_pixels]
    }

    data_frame = pd.DataFrame(
        data, columns=columns
    )

    return Status.Work_done, data_frame


if __name__ == '__main__':

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        status = MPI.Status()
        waiting_queue = set()
        running_queue = set()
        finished_queue = set()
        failure_queue = set()

        hdf5_store = pd.HDFStore(filepath)

        hmi_files, aia_files, vis_files = populate_files(list_of_directories)

        get_corresponding_images = prepare_get_corresponding_images(
            aia_files, vis_files, return_julian_day=True
        )

        for hmi_file in hmi_files:

            a, b, c, d = get_corresponding_images(
                hmi_file
            )
            aia_file, vis_file, julian_day, status_ci = a, b, c, d

            work_object = WorkObject(hmi_file, aia_file, vis_file, julian_day)
            waiting_queue.add(work_object)

        if list(hdf5_store.keys()):
            for index, row in hdf5_store['data'].iterrows():
                work_object = WorkObject(
                    row['hmi_filename'],
                    row['aia_filename'],
                    row['hmi_ic_filename'],
                    row['julian_day']
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
                    sender, item.julian_day, jobstatus.value
                )
            )
            running_queue.discard(item)
            if jobstatus == Status.Work_done:
                sys.stdout.write('Success: {}\n'.format(item.julian_day))
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
                sys.stdout.write('Failure: {}\n'.format(item.julian_day))
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
            'date',
            'julian_day',
            'hmi_filename',
            'hmi_ic_filename',
            'aia_filename',
            'no_of_pixel_sunspot',
            'sunspot_intensity',
            'no_of_pixel_plage_en',
            'plage_en_intensity',
            'no_of_pixels_active_networks',
            'active_networks_intensity',
            'no_of_pixels_background',
            'background_intensity',
            'total_intensity_aia',
            'total_pixels'
        ]

        # global plage_en_masks_dict, active_network_masks_dict, \
        #     sunspot_masks_dict

        while 1:
            work_type = comm.recv(source=0, tag=1)

            if work_type['job'] != 'work':
                break

            item = work_type['item']

            sys.stdout.write('Recieved {}\n'.format(item.julian_day))

            status, data_frame = do_work(item)

            comm.send(
                {'status': status, 'item': item, 'data_frame': data_frame},
                dest=0, tag=2
            )
