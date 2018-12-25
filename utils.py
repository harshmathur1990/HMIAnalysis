# -*- coding: utf-8 -*-
import traceback
import os
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
        return s[:find]+repl+s[find + len(sub):]
    return s


def get_images(date_object, series='', cadence='', segment='image', wavelength=None):
    """
    :param date_object:
    :param series: series name to fetch images
    :param cadence: 1d@1h
    :param segment: default: image, options are continuum, magnetogram, dopplergram
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

    r = c.export(request_string, protocol='fits')

    r.wait()

    if r.status != 0:
        sys.stdout.write('Error for Export Request: {} Status:{}\n'.format(request_string. r.status))
        sys.exit(1)

    files_info = list()
    for id, record in enumerate(r.urls.record):
        if series.startswith('hmi'):
            intermediate = record.replace('[]', '.').replace('][', '.').replace('[', '.').replace(']', '.')
            another_intermediate = nth_repl(intermediate, '.', '', 3)
            removed_date_dots = nth_repl(another_intermediate, '.', '', 3)
            if segment == 'continuum':
                filename = removed_date_dots.replace(':', '').replace('{', '').replace('}', '') + 'continuum.fits'
            else:
                filename = removed_date_dots.replace(':', '').replace('{', '').replace('}', '') + 'magnetogram.fits'
        else:
            filename = record.replace('[]', '.')\
                           .replace('][', '.')\
                           .replace('[', '.')\
                           .replace(']', '.')\
                           .replace(':', '')\
                           .replace('-','')\
                       +'fits'

        file = File(
            id=id,
            filename=filename,
            r=r
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

    for i in range(image.shape[0]):
        for j in range(image.shape[1]):
            if mask[i][j] == 1:
                image[i][j] = 0
            elif np.isnan(mask[i][j]):
                image[i][j] = np.nan

    return image


def delete_images(images, folder_list):

    def delete_file(file_path):
        try:
            if os.path.exists(file_path) and os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception:
            sys.stderr.write(traceback.format_exc())

    for image in images:
        pre_path, path_to_file = os.path.split(image)

        for folder in folder_list:
            file_path = os.path.join(folder, path_to_file)

            delete_file(file_path)


def running_mean(images_list, previous_operation, operation_name='running_mean', window_size=1):
    '''
    :param images_list:
    :param window_size: running mean size
    :return:
    '''
    start = 0
    end = window_size

    resultant_images = list()

    while end <= len(images_list):
        image = np.zeros(shape=(4096, 4096))
        im_0 = images_list[0].get_fits_hdu(previous_operation)
        shape_j = im_0.data.shape[0]
        shape_k = im_0.data.shape[1]


        for j in range(0, shape_j):
            for k in range(0, shape_k):
                sum = 0.0
                for i in range(start, end):
                    curr_image = images_list[i].get_fits_hdu(previous_operation).data
                    sum += curr_image[j][k]

                image[j][k] = sum/(end-start)

        images_list[start].save(operation_name, image, curr_image.header)
        start += window_size
        end += window_size


        resultant_images.append(images_list[start])

    return resultant_images
