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
import sunpy.io
import sunpy.map
import pandas as pd
from pathlib import Path
# from dask.distributed import LocalCluster, Client
from mpi4py import MPI
from utils import prepare_get_corresponding_images, \
    do_aiaprep, do_align, set_nan_to_non_sun, get_date


filepath = Path('/Volumes/Harsh 9599771751/HMIWorkAndData/intensity_results/results.h5')
list_of_directories = [Path('/Volumes/Harsh 9599771751/HMIWorkAndData/data/2014.01.01')]
list_of_mask_directories = [Path('/Volumes/Harsh 9599771751/HMIWorkAndData/HMIAnalysis/mask/2014.01.01')]

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


def populate_masks(list_of_mask_directories):
    plage_en_masks = list()

    active_network_masks = list()

    sunspot_masks = list()

    for a_directory in list_of_mask_directories:
        everything = a_directory.glob('**/*')

        plage_en_masks.extend(
            [
                x for x in everything if x.is_file() and
                x.name.endswith('.png') and x.name.startswith('plages')
            ]
        )

        everything = a_directory.glob('**/*')
        active_network_masks.extend(
            [
                x for x in everything if x.is_file() and
                x.name.endswith('.png') and x.name.startswith('active_networks')
            ]
        )

        everything = a_directory.glob('**/*')
        sunspot_masks.extend(
            [
                x for x in everything if x.is_file() and
                x.name.endswith('.png') and x.name.startswith('hmi.ic')
            ]
        )

    plage_en_masks_dict = dict()
    active_network_masks_dict = dict()
    sunspot_masks_dict = dict()

    for plage_en_mask in plage_en_masks:
        plage_en_masks_dict[plage_en_mask.name[7:-4]] = plage_en_mask

    for active_network_mask in active_network_masks:
        active_network_masks_dict[active_network_mask.name[16:-4]] = active_network_mask

    for sunspot_mask in sunspot_masks:
        sunspot_masks_dict[sunspot_mask.name[:-4]] = sunspot_mask

    sys.stdout.write(
        'len(plage_en_masks_dict): {}, len(active_network_masks_dict): {}, len(sunspot_masks_dict): {}\n'.format(
            len(plage_en_masks_dict.keys()), len(active_network_masks_dict.keys()), len(sunspot_masks_dict.keys())
        )
    )
    return plage_en_masks_dict, active_network_masks_dict, sunspot_masks_dict


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


def do_work(work_object):

    if work_object.aia_file.name not in plage_en_masks_dict:
        sys.stdout.write(
            'Plage Mask not found for {}\n'.format(
                work_object.aia_file.name
            )
        )
        return Status.Work_failure, None

    if work_object.aia_file.name not in active_network_masks_dict:
        sys.stdout.write(
            'Active Networks Mask not found for {}\n'.format(
                work_object.aia_file.name
            )
        )
        return Status.Work_failure, None

    if work_object.vis_file.name not in sunspot_masks_dict:
        sys.stdout.write(
            'Sunspot Mask not found for {}\n'.format(
                work_object.aia_file.name
            )
        )
        return Status.Work_failure, None

    fill_nans = False

    aia_data, aia_header = sunpy.io.fits.read(
        work_object.aia_file
    )[1]

    sys.stdout.write('Read {}\n'.format(work_object.aia_file))
    aia_data, aia_header = do_aiaprep(
        aia_data, aia_header,
        fill_nans=fill_nans
    )

    sys.stdout.write('Did AIAPrep {}\n'.format(work_object.aia_file))

    hmi_data, hmi_header = sunpy.io.fits.read(
        work_object.hmi_file
    )[1]

    sys.stdout.write('Read {}\n'.format(work_object.hmi_file))
    hmi_data, hmi_header = do_aiaprep(
        hmi_data, hmi_header,
        fill_nans=fill_nans
    )

    sys.stdout.write('Did AIAPrep {}\n'.format(work_object.hmi_file))
    vis_data, vis_header = sunpy.io.fits.read(
        work_object.vis_file
    )[1]

    sys.stdout.write('Read {}\n'.format(work_object.vis_file))
    vis_data, vis_header = do_aiaprep(
        vis_data, vis_header,
        fill_nans=fill_nans
    )

    sys.stdout.write('Did AIAPrep {}\n'.format(work_object.vis_file))
    aia_data, aia_header = do_align(
        hmi_data, hmi_header,
        aia_data, aia_header,
        fill_nans=fill_nans
    )

    sys.stdout.write('Did Align {}\n'.format(work_object.aia_file))

    hmi_data, hmi_total_pixels = set_nan_to_non_sun(
        hmi_data,
        hmi_header,
        factor=0.96,
        fill_nans=fill_nans,
        return_total_pixels=True
    )

    aia_data, aia_total_pixels = set_nan_to_non_sun(
        aia_data,
        aia_header,
        factor=0.96,
        fill_nans=fill_nans,
        return_total_pixels=True
    )

    vis_data, vis_total_pixels = set_nan_to_non_sun(
        vis_data,
        vis_header,
        factor=0.96,
        fill_nans=fill_nans,
        return_total_pixels=True
    )

    sys.stdout.write('Cropped Files\n')

    plage_mask_file = plage_en_masks_dict[work_object.aia_file.name]

    active_network_mask_file = active_network_masks_dict[
        work_object.aia_file.name
    ]

    sunspot_mask_file = sunspot_masks_dict[work_object.vis_file.name]

    plage_mask = skimage.color.rgb2gray(
        imageio.imread(
            plage_mask_file
        )
    )

    active_network_mask = skimage.color.rgb2gray(
        imageio.imread(
            active_network_mask_file
        )
    )

    sunspot_mask = skimage.color.rgb2gray(
        imageio.imread(
            sunspot_mask_file
        )
    )

    sys.stdout.write('Read and made gray masks\n')
    no_of_pixel_plage_en = np.nansum(plage_mask)

    no_of_pixels_active_networks = np.nansum(active_network_mask)

    no_pixel_sunspot = np.nansum(sunspot_mask)

    # no_of_pixel_background = hmi_total_pixels - (
    #     no_of_pixel_plage_en,
    #     no_of_pixels_active_networks,
    #     no_pixel_sunspot
    # )

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

    sunspot_intensity = np.nansum(
        np.multiply(
            sunspot_mask,
            vis_data
        )
    )

    sys.stdout.write('Calculated Everything\n')

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
        'total_pixels': [hmi_total_pixels]
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

            aia_file, vis_file, julian_day, status_ci = get_corresponding_images(
                hmi_file
            )

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
                if not list(hdf5_store.keys()):
                    status_dict['data_frame'].to_hdf(
                        filepath, 'data', format='t'
                    )
                else:
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
            'total_pixels'
        ]

        # global plage_en_masks_dict, active_network_masks_dict, \
        #     sunspot_masks_dict

        a, b, c = populate_masks(
            list_of_mask_directories
        )

        plage_en_masks_dict = a
        active_network_masks_dict, sunspot_masks_dict = b, c

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
