# -*- coding: utf-8 -*-
import os
import traceback
import drms
import sys
import numpy as np
# from multiprocessing import Semaphore
from decor import retry
from skimage.draw import circle
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base


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

    ulta_mask[ulta_mask != 1.0] = np.nan

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

    rr, cc = circle(center_x, center_y, radius * factor)

    mask = np.zeros_like(image)

    mask[rr, cc] = 1.0

    mask[mask == 0.0] = np.nan

    im = np.multiply(
        mask,
        image
    )

    return im
