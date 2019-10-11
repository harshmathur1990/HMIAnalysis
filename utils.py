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
# from multiprocessing import Semaphore


Base = declarative_base()
engine = create_engine('sqlite:///hmi.db', echo=True)


try:
    c = drms.Client(email='harsh.mathur@iiap.res.in', verbose=True)
except Exception:
    err = traceback.format_exc()
    sys.stdout.write('Failed to Create the Drms Client\n')
    sys.stdout.write(err)
    os._exit(1)


# sem = Semaphore(value=1)


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


def set_nan_to_non_sun(image, header, factor=1.0):
    if not factor:
        factor = 1.0

    radius = header['R_SUN']

    center_x = header['CRPIX1']

    center_y = header['CRPIX2']

    sys.stdout.write(
        'Center X: {}, Center Y: {} Radius: {}\n'.format(
            center_x, center_y, radius
        )
    )

    rr, cc = circle(center_x - 1, center_y - 1, radius * factor)

    mask = np.zeros_like(image)

    mask[rr, cc] = 1.0

    # mask[mask == 0.0] = 0.0

    im = np.multiply(
        mask,
        image
    )

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

    threshold = mean + (k * std)

    result = np.zeros(shape=image.shape)

    result[op(image, threshold)] = value_1

    if k2 and op2:
        threshold_2 = mean + (k2 * std)

        result[op2(image, threshold_2)] = value_2

    result = set_nan_to_non_sun(result, header, factor=radius_factor)

    return result, invalid_result


def do_limb_darkening_correction(
    image, header, radius_factor=1.0, kernel_size=105
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

    result = np.divide(image, large_median)

    result[np.isinf(result)] = 0.0

    result[np.isnan(result)] = 0.0

    result = result / np.max(result)

    return skimage.exposure.equalize_adapthist(result, clip_limit=0.02)


def do_aiaprep(data, header, radius_factor=1.0):
    header['HGLN_OBS'] = 0

    aiamap = sunpy.map.Map(
        data,
        header
    )

    # Slow, 7 secs per call, 36% of the program
    aiamap_afterprep = sunpy.instr.aia.aiaprep(aiamap=aiamap)

    result = set_nan_to_non_sun(
        aiamap_afterprep.data,
        aiamap_afterprep.meta, factor=radius_factor)

    result = result / aiamap_afterprep.meta['exptime']

    return result, aiamap_afterprep.meta


def do_align(
    hmi_file,
    aia_data,
    aia_header,
    radius_factor=1.0
):

    hmi_prep_hdu = hmi_file.get_fits_hdu(
        'aiaprep',
    )

    hmiprep_map = sunpy.map.Map(hmi_prep_hdu.data, hmi_prep_hdu.header)

    aia_map = sunpy.map.Map(aia_data, aia_header)

    aia_map_rotated = sunpy.physics.differential_rotation.differential_rotate(
        aia_map,
        observer=hmiprep_map.coordinate_frame.observer
    )

    result = set_nan_to_non_sun(
        aia_map_rotated.data,
        aia_map_rotated.meta,
        factor=radius_factor
    )

    return result, aia_map_rotated.meta


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
