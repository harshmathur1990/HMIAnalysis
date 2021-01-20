# import os
import sys
import enum
# import imageio
# import skimage.color
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
from utils import prepare_get_corresponding_aia_images, \
    do_aiaprep, do_align, set_nan_to_non_sun, \
    get_dateime, do_thresholding, do_area_filtering, \
    do_limb_darkening_correction


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

        vis_files.extend(
            [
                x for x in everything if x.is_file() and
                x.name.endswith('.fits') and x.name.startswith('hmi.ic')
            ]
        )

    sys.stdout.write(
        'len(aia_files): {}, len(vis_files): {}\n'.format(
            len(aia_files), len(vis_files)
        )
    )

    return aia_files, vis_files


class WorkObject(object):
    def __init__(self, aia_file, vis_file, julian_day, julian_day_diff):
        self._aia_file = aia_file
        self._vis_file = vis_file
        self._julian_day = julian_day
        self._julian_day_diff = julian_day_diff

    @property
    def aia_file(self):
        return self._aia_file

    @property
    def vis_file(self):
        return self._vis_file

    @property
    def julian_day(self):
        return self._julian_day

    @property
    def julian_day_diff(self):
        return self._julian_day_diff

    def __eq__(self, other):
        return self.julian_day == other.julian_day

    def __hash__(self):
        return hash(self.julian_day)


def get_plage_active_network_intensity(
    work_object,
    vis_data,
    vis_header,
    aia_file,
    sunspot_mask
):

    sys.stdout.write(
        'AIA 1600 Processing on julian day: {}\n'.format(
            work_object.julian_day
        )
    )

    fill_nans = True
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

    limb_corrected_aia_data = do_limb_darkening_correction(
        aia_data,
        aia_header,
        radius_factor=0.96
    )

    limb_corrected_aia_data, aia_total_pixels = set_nan_to_non_sun(
        limb_corrected_aia_data,
        aia_header,
        factor=0.96,
        fill_nans=fill_nans,
        return_total_pixels=True
    )

    plage_mask, invalid_result = do_thresholding(
        limb_corrected_aia_data,
        aia_header,
        k=1.71,
        op=operator.ge,
        value_1=1.0,
        radius_factor=0.96,
        k2=None,
        op2=None,
        value_2=None
    )

    if invalid_result:
        sys.stdout.write(
            'Invalid Plage Thresholding result for {}'.format(
                aia_file
            )
        )
        return False, None, None, None, None, None, None

    plage_mask = closing(plage_mask, square(3))

    plage_mask = do_area_filtering(plage_mask)

    rr, cc = np.where(sunspot_mask == 1.0)

    plage_mask[rr, cc] = 0.0

    active_network_mask, invalid_result = do_thresholding(
        limb_corrected_aia_data,
        aia_header,
        k=1.65,
        op=operator.ge,
        k2=1.71,
        op2=operator.ge,
        radius_factor=0.96,
        value_1=1.0,
        value_2=0.0
    )

    if invalid_result:
        sys.stdout.write(
            'Invalid Active Networks Thresholding result for {}'.format(
                aia_file
            )
        )
        return False, None, None, None, None, None, None

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

    sunspot_aia_intensity = np.nansum(
        np.multiply(
            sunspot_mask,
            aia_data
        )
    )

    sunspot_mask[np.where(np.isnan(sunspot_mask))] = 0.0

    plage_mask[np.where(np.isnan(plage_mask))] = 0.0

    magnetic_mask = np.add(
        sunspot_mask,
        plage_mask
    )

    magnetic_squarred_intensity = np.nansum(
        np.square(
            np.multiply(
                magnetic_mask,
                aia_data
            )
        )
    )

    active_network_squarred_intensity = np.nansum(
        np.square(
            np.multiply(
                active_network_mask,
                aia_data
            )
        )
    )

    total_intensity_aia = np.nansum(
        aia_data
    )

    no_of_pixel_plage_en = np.nansum(plage_mask)

    no_of_pixels_active_networks = np.nansum(active_network_mask)

    return True, no_of_pixel_plage_en, no_of_pixels_active_networks, \
        plage_intensity, active_network_intensity, \
        total_intensity_aia, sunspot_aia_intensity, \
        magnetic_squarred_intensity, active_network_squarred_intensity


def get_sunspot_intensity_and_total_pixels(
    work_object,
    vis_file
):

    sys.stdout.write(
        'Visible Processing on julian day: {}\n'.format(
            work_object.julian_day
        )
    )

    fill_nans = True

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
            'Invalid Thresholding result for {}'.format(
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

    sys.stdout.write(
        'Working on julian day: {}\n'.format(
            work_object.julian_day
        )
    )

    s1, a, b, c, d, e, f = get_sunspot_intensity_and_total_pixels(
        work_object,
        work_object.vis_file
    )

    if not s1:
        return Status.Work_failure, None

    no_pixel_sunspot, vis_total_pixels = a, b

    sunspot_intensity, vis_data, vis_header = c, d, e

    sunspot_mask = f

    s2, a, b, c, d, e, f, g, h = get_plage_active_network_intensity(
        work_object,
        vis_data,
        vis_header,
        work_object.aia_file,
        sunspot_mask
    )

    if not s2:
        return Status.Work_failure, None

    no_of_pixel_plage_en, no_of_pixels_active_networks = a, b

    plage_intensity, active_network_intensity = c, d

    total_intensity_aia = e

    sunspot_aia_intensity = f

    magnetic_squarred_intensity = g

    active_network_squarred_intensity = h

    data = {
        'date': [get_dateime(work_object.vis_file).strftime('%Y-%m-%dT%H:%M:%S')],
        'julian_day': [work_object.julian_day],
        'julian_day_diff': [work_object.julian_day_diff],
        'hmi_ic_filename': [work_object.vis_file.name],
        'aia_filename': [work_object.aia_file.name],
        'no_of_pixel_sunspot': [no_pixel_sunspot],
        'sunspot_intensity': [sunspot_intensity],
        'sunspot_aia_intensity': [sunspot_aia_intensity],
        'no_of_pixel_plage_en': [no_of_pixel_plage_en],
        'plage_en_intensity': [plage_intensity],
        'no_of_pixels_active_networks': [no_of_pixels_active_networks],
        'active_networks_intensity': [active_network_intensity],
        'magnetic_squarred_intensity': [magnetic_squarred_intensity],
        'active_network_squarred_intensity': [active_network_squarred_intensity],
        'no_of_pixels_background': [0],
        'background_intensity': [0],
        'total_intensity_aia': total_intensity_aia,
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

        aia_files, vis_files = populate_files(list_of_directories)

        get_corresponding_images = prepare_get_corresponding_aia_images(
            aia_files, return_julian_day=True
        )

        for vis_file in vis_files:

            a, b, c, d = get_corresponding_images(
                vis_file
            )
            aia_file, juldiff, julian_day, status_ci = a, b, c, d

            if not status_ci:
                continue

            work_object = WorkObject(aia_file, vis_file, julian_day, juldiff)
            waiting_queue.add(work_object)

        count = 0

        if list(hdf5_store.keys()):
            for index, row in hdf5_store['data'].iterrows():
                work_object = WorkObject(
                    row['aia_filename'],
                    row['hmi_ic_filename'],
                    row['julian_day'],
                    row['julian_day_diff']
                )

                waiting_queue.discard(work_object)

                count = 1

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
                # np.savetxt(sys.stdout.buffer, np.array(status_dict['data_frame']))
                # sys.stdout.write('\n')
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
            'julian_day_diff',
            'hmi_ic_filename',
            'aia_filename',
            'no_of_pixel_sunspot',
            'sunspot_intensity',
            'sunspot_aia_intensity',
            'no_of_pixel_plage_en',
            'plage_en_intensity',
            'no_of_pixels_active_networks',
            'active_networks_intensity',
            'magnetic_squarred_intensity',
            'active_network_squarred_intensity',
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
