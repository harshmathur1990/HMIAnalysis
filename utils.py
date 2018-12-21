# -*- coding: utf-8 -*-
import traceback
import os
import drms
import sys
import numpy as np
import skimage.transform
import scipy.signal
import operator
from astropy.io import fits
from sunpy.map import Map
from sunpy.instr.aia import aiaprep
from skimage.draw import circle
from skimage.morphology import closing, square

from dto import File, DataList

c = drms.Client(email='harsh.mathur@iiap.res.in', verbose=True)


def get_fits_array(fits_path):
    fits_image = fits.open(fits_path)

    rv_fits_hdu = None

    for fits_hdu in fits_image:
        fits_hdu.verify('fix')
        if fits_hdu.data is not None:
            rv_fits_hdu = fits_hdu
            break

    return rv_fits_hdu


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


def do_limb_darkening_correction(image, header):
    radius = header['R_SUN']

    center_x = header['CRPIX1']

    center_y = header['CRPIX2']

    small_image = skimage.transform.resize(
        image,
        output_shape=(512, 512),
        order=3,
        preserve_range=True
    )

    small_median = scipy.signal.medfilt2d(small_image, 105)

    large_median = skimage.transform.resize(
        small_median,
        output_shape=image.shape,
        order=3,
        preserve_range=True
    )

    result = np.zeros(shape=image.shape, dtype=float)

    result[:] = np.nan

    for i in range(image.shape[0]):
        for j in range(image.shape[1]):
            if ((i-center_x)**2 + (j-center_y)**2) <= (0.97 * radius)**2:
                result[i][j] = float(image[i][j])/large_median[i][j]

    return result


def do_aiaprep(image):
    fits_array = get_fits_array(image)

    fits_array.header['HGLN_OBS'] = 0

    map = Map(
        fits_array.data,
        fits_array.header
    )

    aiamap_afterprep = aiaprep(aiamap=map)

    return aiamap_afterprep


def get_aiaprep_image(image):

    folder, path_to_file = os.path.split(image)
    sys.stdout.write('Checking if {} AIAPrep image exists...\n'.format(path_to_file))

    if not os.path.exists('aiaprep/' + path_to_file):

        aiamap_afterprep = do_aiaprep(image)

        radius = aiamap_afterprep.meta['R_SUN']

        center_x = aiamap_afterprep.meta['CRPIX1']

        center_y = aiamap_afterprep.meta['CRPIX2']

        new_data = np.zeros(shape=aiamap_afterprep.data.shape)

        new_data[:] = np.nan

        xx,cc = circle(center_x, center_y, radius)

        for x,y in zip(xx,cc):
            new_data[x][y] = aiamap_afterprep.data[x][y]

        new_map = Map(new_data, aiamap_afterprep.meta)

        new_map.save('aiaprep/' + path_to_file, filetype='fits')


    else:
        sys.stdout.write('{} AIAPrep image exists, skipping...\n'.format(path_to_file))

    return 'aiaprep/' + path_to_file


def get_ldr_image(image):

    folder, path_to_file = os.path.split(image)
    sys.stdout.write('Checking if {} LDR image exists...\n'.format(path_to_file))
    if not os.path.exists('ldr/'+path_to_file):
        sys.stdout.write('{} LDR image does not exist, creating...\n'.format(path_to_file))
        image = get_aiaprep_image(image)
        final_image = get_fits_array(
            fits_path=image
        )

        ldr_image = do_limb_darkening_correction(
            final_image.data,
            final_image.header
        )
        hdu = fits.PrimaryHDU(ldr_image)
        hdu.header = final_image.header
        hdu.verify('fix')
        hdul = fits.HDUList([hdu])
        hdul.writeto('ldr/'+path_to_file, output_verify='ignore')
    else:
        sys.stdout.write('{} LDR image exists, skipping...\n'.format(path_to_file))

    return 'ldr/'+path_to_file


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
                filename = removed_date_dots.replace(':', '').replace('{', '').replace('}', '') + 'fits'
        else:
            filename = record.replace('[]', '.').replace('][', '.')\
                           .replace('[', '.').replace(']', '.')\
                           .replace('-','').replace(':', '').replace('{', '')\
                           .replace('}', '') + 'fits'
        sys.stdout.write('Checking if {} exists on disk\n'.format(filename))

        file = File(
            id=id,
            filename=filename
        )

        files_info.append(file)

    return DataList(r=r, files_info=files_info)


def calculate_mask(image, k, header, op=operator.le):
    '''
    Calculates image mask depending on k parameter
    :param image:
    :param k:
    :return: bonary numpy array of same shape as image
    '''

    radius = header['R_SUN']

    center_x = header['CRPIX1']

    center_y = header['CRPIX2']

    mean = np.nanmean(image)
    std = np.nanstd(image)

    threshold = mean + (k * std)

    result = np.zeros(shape=image.shape)


    for i in range(image.shape[0]):
        for j in range(image.shape[1]):
            if (i-center_x)**2 + (j-center_y)**2 > (0.97 * radius)**2:
                result[i][j] = np.nan
            elif op(image[i][j], threshold):
                result[i][j] = 1.0
            else:
                result[i][j] = 0.0

    return result


def do_morphological_closing(image, kernel_size):
    return closing(image, square(kernel_size))


def get_mask_images(images, k, op=operator.le, do_limb_darkening_correction=False, post_processor=None):
    res_list = list()

    for image in images:
        folder, path_to_file = os.path.split(image)
        if not os.path.exists('mask/' + path_to_file):
            if do_limb_darkening_correction:
                image = get_ldr_image(image)
            else:
                image = get_aiaprep_image(image)
            fits_image = get_fits_array(image)
            mask = calculate_mask(fits_image.data, k, op=op, header=fits_image.header)
            closed_mask = do_morphological_closing(mask, 3)
            if post_processor:
                closed_mask = post_processor(closed_mask)
            hdu = fits.PrimaryHDU(closed_mask)
            hdu.header = fits_image.header
            hdu.verify('fix')
            hdul = fits.HDUList([hdu])
            hdul.writeto('mask/' + path_to_file, output_verify='ignore')
        else:
            sys.stdout.write('{} Mask exists, skipping...\n'.format(path_to_file))
        res_list.append(
            'mask/' + path_to_file
        )

    delete_images(images, ['aiaprep', 'ldr'])

    return res_list


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


def running_mean(images_list, window_size=1):
    '''
    :param images_list:
    :param window_size: running mean size
    :return:
    '''
    start = 0
    end = window_size

    resultant_images = list()

    while end <= len(images_list):
        image = np.zeros(shape=images_list[0].shape)

        if isinstance(images_list[0], str):
            im_0 = fits.open(images_list[0])[0]
            shape_j = im_0.data.shape[0]
            shape_k = im_0.data.shape[1]
        else:
            shape_j = images_list[0].shape[0]
            shape_k = images_list[0].shape[1]

        for j in range(0, shape_j):
            for k in range(0, shape_k):
                sum = 0.0
                for i in range(start, end):
                    if isinstance(images_list[i], str):
                        curr_image = fits.open(images_list[i])[0].data
                    else:
                        curr_image = images_list[i]
                    sum += curr_image[j][k]

                image[j][k] = sum/(end-start)

        start += window_size
        end += window_size

        resultant_images.append(image)

    return resultant_images
