# -*- coding: utf-8 -*-
import os
import traceback
import drms
import sys
import numpy as np
import skimage.transform
import scipy.signal
import sunpy.map
import sunpy.instr.aia
from decor import retry
from skimage.draw import circle
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import sunpy.physics.differential_rotation
import skimage
import sunpy.time
import sunpy.io
import time
from pathlib import Path
from skimage.measure import label, regionprops
# from multiprocessing import Semaphore


Base = declarative_base()
try:
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    day = int(sys.argv[3])
    divisor = int(sys.argv[7])
    remainder = int(sys.argv[8])
    engine = create_engine(
        'sqlite:///hmi_{}_{}_{}_{}_{}.db'.format(
            year,
            month,
            day,
            divisor,
            remainder
        )
    )
except Exception:
    sys.stdout.write('Falling back to hmi.db\n')
    engine = create_engine(
        'sqlite:///hmi.db'
    )
# c = drms.Client(email='harsh.mathur@iiap.res.in', verbose=True)

# try:
#     data_present = False
#     try:
#         argument = int(sys.argv[5])
#         if argument > 0:
#             data_present = True
#     except Exception:
#         data_present = False
#     if not data_present:
#         c = drms.Client(email='harsh.mathur@iiap.res.in', verbose=True)
#     else:
#         c = None
# except Exception:
#     err = traceback.format_exc()
#     sys.stdout.write('Failed to Create the Drms Client\n')
#     sys.stdout.write(err)
#     os._exit(1)


# sem = Semaphore(value=1)


def do_area_filtering(mask):
    area_per_pixel = (0.6 / 60) * (0.6 / 60)

    pixel_in_onetenth_arcminute = 0.1 / area_per_pixel

    label_image = label(mask)
    regions = regionprops(label_image)
    for region in regions:
        if region.area < pixel_in_onetenth_arcminute:
            for coords in region.coords:
                mask[coords[0]][coords[1]] = 0.0
    return mask


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        sys.stdout.write(
            '{} : {} ms\n'.format(
                method.__name__, (te - ts) * 1000
            )
        )
        return result
    return timed


def nth_repl(s, sub, repl, nth):
    find = s.find(sub)
    # if find is not p1 we have found at least one match for the substring
    i = find != -1
    # loop util we find the nth or we find no match
    while find != -1 and i != nth:
        # find + 1 means we start at the last match start index + 1
        find = s.find(sub, find + 1)
        i += 1
    # if i  is equal to nth we found nth matches so replace
    if i == nth:
        return s[:find] + repl + s[find + len(sub):]
    return s


@retry((Exception,))
def get_images(
    date_object,
    series='',
    cadence='',
    segment='image',
    wavelength=None
):
    """
    :param date_object:
    :param series: series name to fetch images
    :param cadence: 1d@1h
    :param segment: default: image, options are continuum
    magnetogram, dopplergram
    :param wavelength: default: None
    :return: list of file names
    """

    if wavelength:
        request_string = '{}[{}/{}][{}]{{{}}}'.format(
            series,
            date_object.strftime('%Y.%m.%d_TAI'),
            cadence,
            wavelength,
            segment
        )
    else:
        request_string = '{}[{}/{}]{{{}}}'.format(
            series,
            date_object.strftime('%Y.%m.%d_TAI'),
            cadence,
            segment
        )

    try:
        # sys.stdout.write(
        # 'Value of Semaphore before making export Request: {}\n'.format(sem)
        # )
        # sem.acquire()
        sys.stdout.write(
            'Creating Export Request: {}\n'.format(request_string)
        )
        # sys.stdout.write(
        # 'Value of Semaphore while making export Request: {}\n'.format(sem)
        # )
        r = c.export(request_string, protocol='fits')
        r.wait()
    except Exception as e:
        # sem.release()
        err = traceback.format_exc()
        sys.stdout.write('Error for Export Request: {}\n'.format(
            request_string)
        )
        sys.stdout.write(err)
        raise e
    else:
        pass
        # sem.release()
        # sys.stdout.write(
        # 'Value of Semaphore after making export Request: {}\n'.format(sem)
        # )

    if r.status != 0:
        sys.stdout.write('Error for Export Request: {} Status:{}\n'.format(
            request_string, r.status))
        os._exit(1)

    files_info = list()
    for id in np.arange(len(r.urls.record)):

        filename = r.urls.iloc[id]['filename']

        from dto import File
        file = File(
            id=id,
            filename=filename,
            r=r,
            date_object=date_object
        )

        files_info.append(file)

    return files_info


def apply_mask(image, mask):
    '''
    sets the pixels as zero according to binary mask
    :param image:
    :param mask:
    :return:
    '''

    ulta_mask = -1 * (mask - 1)

    ulta_mask[ulta_mask != 1.0] = 0.0

    im = np.multiply(
        ulta_mask,
        image
    )

    return im


def set_nan_to_non_sun(
    image,
    header,
    factor=1.0,
    fill_nans=True,
    return_total_pixels=False
):
    if not factor:
        factor = 1.0

    radius = header['R_SUN']

    center_x = header['CRPIX1']

    center_y = header['CRPIX2']

    # sys.stdout.write(
    #     'Center X: {}, Center Y: {} Radius: {}\n'.format(
    #         center_x, center_y, radius
    #     )
    # )

    rr, cc = circle(center_x - 1, center_y - 1, radius * factor)

    mask = np.zeros_like(image)

    if fill_nans:
        mask[mask == 0.0] = np.nan

    mask[rr, cc] = 1.0

    # mask[mask == 0.0] = 0.0

    im = np.multiply(
        mask,
        image
    )

    if return_total_pixels:
        return im, np.nansum(mask)

    return im


def do_thresholding(
    image, header, k, op, value_1, radius_factor=0.96,
    k2=None, op2=None, value_2=None
):

    mean = np.nanmean(image)
    std = np.nanstd(image)

    invalid_result = False
    if np.isnan(mean) or np.isinf(mean) or np.isnan(std) or np.isinf(std):
        invalid_result = True
        return None, invalid_result

    threshold = mean + (k * std)

    result = np.zeros(shape=image.shape)

    result[op(image, threshold)] = value_1

    if k2 and op2:
        threshold_2 = mean + (k2 * std)

        result[op2(image, threshold_2)] = value_2

    result = set_nan_to_non_sun(result, header, factor=radius_factor)

    return result, invalid_result


def do_limb_darkening_correction(
    image, header, radius_factor=1.0, kernel_size=105, clip_limit=0.01
):

    image[np.where(image < 0)] = 0

    small_image = skimage.transform.resize(
        image,
        output_shape=(512, 512),
        order=3,
        preserve_range=True
    )

    small_image[np.isnan(small_image)] = 0.0

    # Slow, 20 secs per call, 30% time of the program
    small_median = scipy.signal.medfilt2d(small_image, kernel_size)

    large_median = skimage.transform.resize(
        small_median,
        output_shape=image.shape,
        order=3,
        preserve_range=True
    )

    large_median = set_nan_to_non_sun(
        large_median,
        header,
        factor=radius_factor
    )

    large_median /= np.nanmax(large_median)

    result = np.divide(image, large_median)

    result[np.isinf(result)] = 0.0

    result[np.isnan(result)] = 0.0

    # max_value = np.nanmax(result)

    # result = result / max_value

    # result = skimage.exposure.equalize_adapthist(result, clip_limit=clip_limit)

    # result *= max_value

    return set_nan_to_non_sun(
        result,
        header,
        factor=radius_factor
    )


def do_aiaprep(data, header, radius_factor=1.0, fill_nans=True):
    header['HGLN_OBS'] = 0

    aiamap = sunpy.map.Map(
        data,
        header
    )

    # Slow, 7 secs per call, 36% of the program
    aiamap_afterprep = sunpy.instr.aia.aiaprep(aiamap=aiamap)

    result = set_nan_to_non_sun(
        aiamap_afterprep.data,
        aiamap_afterprep.meta,
        factor=radius_factor,
        fill_nans=fill_nans
    )

    if 'exptime' in aiamap_afterprep.meta:
        result = result / aiamap_afterprep.meta['exptime']

    return result, aiamap_afterprep.meta


def do_align(
    hmi_data,
    hmi_header,
    aia_data,
    aia_header,
    radius_factor=1.0,
    fill_nans=True
):

    hmiprep_map = sunpy.map.Map(hmi_data, hmi_header)

    aia_map = sunpy.map.Map(aia_data, aia_header)

    aia_map_rotated = sunpy.physics.differential_rotation.differential_rotate(
        aia_map,
        observer=hmiprep_map.coordinate_frame.observer
    )

    result = set_nan_to_non_sun(
        aia_map_rotated.data,
        aia_map_rotated.meta,
        factor=radius_factor,
        fill_nans=fill_nans
    )

    return result, aia_map_rotated.meta


# @timeit
def parse_time_from_sunpy(header):
    return sunpy.time.parse_time(header['T_OBS'])


# @timeit
def get_julian_day_from_astropy_time(astropy_time):
    return astropy_time.jd


# @timeit
def get_julian_day(file_dto):

    if isinstance(file_dto, Path):
        header = sunpy.io.read_file_header(file_dto, filetype='fits')[1]
    else:
        header = file_dto.read_headers('data')

    # header = file_hdu.header

    astropy_time = parse_time_from_sunpy(header)

    return get_julian_day_from_astropy_time(astropy_time)


# @timeit
def get_date(file_dto):
    if isinstance(file_dto, path):
        header = sunpy.io.read_file_header(file_dto, filetype='fits')[1]
    else:
        header = file_dto.read_headers('data')

    # header = file_hdu.header

    time = sunpy.time.parse_time(header['T_OBS'])

    return time.datetime.date()


def get_dateime(file_dto):
    if isinstance(file_dto, Path):
        header = sunpy.io.read_file_header(file_dto, filetype='fits')[1]
    else:
        header = file_dto.read_headers('data')

    # header = file_hdu.header

    time = sunpy.time.parse_time(header['T_OBS'])

    return time.datetime


def prepare_get_corresponding_images(
    aia_images, vis_images, return_julian_day=False
):

    aia_ordered_list = list()

    vis_ordered_list = list()

    for aia_image in aia_images:
        aia_ordered_list.append(get_julian_day(aia_image))

    for vis_image in vis_images:
        vis_ordered_list.append(get_julian_day(vis_image))

    aia_ordered_list = np.array(aia_ordered_list)

    vis_ordered_list = np.array(vis_ordered_list)

    def get_corresponding_images(hmi_image):

        julian_day_hmi = get_julian_day(hmi_image)

        aia_subtract_array = np.abs(aia_ordered_list - julian_day_hmi)

        vis_subtract_array = np.abs(vis_ordered_list - julian_day_hmi)

        aia_argmin = np.argmin(aia_subtract_array)

        vis_argmin = np.argmin(vis_subtract_array)

        if aia_subtract_array[aia_argmin] < 0.5 and \
                vis_subtract_array[vis_argmin] < 0.5:

            if return_julian_day:
                return aia_images[aia_argmin], \
                    vis_images[vis_argmin], julian_day_hmi, True
            else:
                return aia_images[aia_argmin], vis_images[vis_argmin], True

        if return_julian_day:
            return None, None, None, False
        else:
            return None, None, False

    return get_corresponding_images


def prepare_get_corresponding_aia_images(
    aia_images, return_julian_day=False
):

    aia_ordered_list = list()

    for aia_image in aia_images:
        aia_ordered_list.append(get_julian_day(aia_image))

    aia_ordered_list = np.array(aia_ordered_list)

    def get_corresponding_images(hmi_image):

        julian_day_hmi = get_julian_day(hmi_image)

        aia_subtract_array = np.abs(aia_ordered_list - julian_day_hmi)

        aia_argmin = np.argmin(aia_subtract_array)

        if aia_subtract_array[aia_argmin] < 0.01041666666:

            if return_julian_day:
                return aia_images[aia_argmin], \
                    aia_subtract_array[aia_argmin], julian_day_hmi, True
            else:
                return aia_images[aia_argmin], \
                    aia_subtract_array[aia_argmin], True

        if return_julian_day:
            return None, None, None, False
        else:
            return None, None, False

    return get_corresponding_images


def initialize():
    directory_names = [
        'data',
        'aiaprep',
        'crop_hmi_afterprep',
        'aligned_data',
        'ldr',
        'mask',
        'souvik'
    ]

    for directory in directory_names:
        if not os.path.isdir(directory):
            os.mkdir(directory)
