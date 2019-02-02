# -*- coding: utf-8 -*-
import os
import traceback
import drms
import sys
import numpy as np
from dto import File

c = drms.Client(email='harsh.mathur@iiap.res.in', verbose=True)


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

    sys.stdout.write('Creating Export Request: {}\n'.format(request_string))

    try:
        r = c.export(request_string, protocol='fits')
        r.wait()
    except Exception:
        err = traceback.format_exc()
        sys.stdout.write(err)
        sys.stdout.write('Error for Export Request: {} Status:{}\n'.format(
            request_string, r.status))
        os._exit(1)

    if r.status != 0:
        sys.stdout.write('Error for Export Request: {} Status:{}\n'.format(
            request_string, r.status))
        os._exit(1)

    files_info = list()
    for id, record in enumerate(r.urls.record):
        if series.startswith('hmi'):
            intermediate = record.replace('[]', '.').replace(
                '][', '.').replace('[', '.').replace(']', '.')
            another_intermediate = nth_repl(intermediate, '.', '', 3)
            removed_date_dots = nth_repl(another_intermediate, '.', '', 3)
            if segment == 'continuum':
                filename = removed_date_dots.replace(':', '').replace(
                    '{', '').replace('}', '') + 'continuum.fits'
            else:
                filename = removed_date_dots.replace(':', '').replace(
                    '{', '').replace('}', '') + 'magnetogram.fits'
        else:
            filename = record.replace('[]', '.') \
                .replace('][', '.') \
                .replace('[', '.') \
                .replace(']', '.') \
                .replace(':', '') \
                .replace('-', '') \
                + 'fits'

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

    im = image.copy()
    im[mask == 1.0] = 0.0

    return im


def running_mean(
    images_list,
    previous_operation,
    operation_name='running_mean',
    window_size=1,
    suffix=None
):
    '''
    :param images_list:
    :param window_size: running mean size
    :return:
    '''
    start = 0
    end = window_size

    resultant_images = list()

    while end <= len(images_list):

        if images_list[start].is_exist_in_directory(
            operation_name, suffix=suffix
        ):
            resultant_images.append(images_list[start])
            start += window_size
            end += window_size
            continue

        image = np.zeros(shape=(4096, 4096))

        for i in range(start, end):
            curr_image = images_list[i].get_fits_hdu(
                previous_operation.operation_name)
            curr_image.data[np.isnan(curr_image.data)] = 0.0

            image = np.add(image, curr_image.data)

        image = np.divide(image, end - start)

        image = set_nan_to_non_sun(image, curr_image.header, factor=0.97)

        images_list[start].save(operation_name, image,
                                curr_image.header, suffix=suffix)

        resultant_images.append(images_list[start])

        start += window_size
        end += window_size

    return resultant_images


def set_nan_to_non_sun(image, header, factor=1.0):
    if not factor:
        factor = 1.0

    radius = header['R_SUN']

    center_x = header['CRPIX1']

    center_y = header['CRPIX2']

    Y, X = np.ogrid[:image.shape[0], :image.shape[1]]

    dist_from_center = np.sqrt((X - center_x) ** 2 + (Y - center_y) ** 2)

    mask = dist_from_center <= (factor * radius)

    im = image.copy()

    im[~mask] = np.nan

    return im
