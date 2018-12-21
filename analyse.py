# -*- coding: utf-8 -*-
import operator
import sys
import datetime
from datetime import timedelta
import os
import copy
from astropy.io import fits
from skimage.measure import label, regionprops

from utils import running_mean, get_images, \
    get_mask_images, \
    apply_mask, get_fits_array, get_aiaprep_image


def get_masks(
        images,
        k,
        do_limb_darkening_correction=False,
        op=operator.le,
        post_processor=None
    ):

    mask_images = get_mask_images(
        images,
        k=k,
        op=op,
        do_limb_darkening_correction=do_limb_darkening_correction,
        post_processor=post_processor
    )
    return mask_images


def get_vis_masks(images):


    return get_masks(
        images,
        k=-5,
        do_limb_darkening_correction=False
    )


def get_aia_masks(images):

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

    masks = get_masks(
        images,
        k=1.71,
        do_limb_darkening_correction=True,
        op=operator.ge,
        post_processor=do_area_filtering
    )

    return masks


def do_hmi_areaprep(images):
    images = [get_aiaprep_image(image) for image in images]
    return images


def mask_magnetograms(hmi_list, masks_array):

    masked_images = list()

    iterables = (hmi_list,) + tuple(masks_array)

    for hmi, *masks in zip(*iterables):

        folder, path_to_file = os.path.split(hmi_list)

        sys.stdout.write('Checking if {} Masked Magnetogrm exists...\n')

        if not os.path.exists('masked_magnetograms/'+path_to_file):

            hmi_image = get_fits_array(hmi)

            masked_image = copy.deepcopy(hmi_image.data)

            for mask in masks:
                masked_image = apply_mask(masked_image, mask)

            hdu = fits.PrimaryHDU(masked_image)
            hdu.header = hmi_image.header
            hdul = fits.HDUList([hdu])
            hdul.writeto('masked_magnetograms/' + path_to_file)
        else:
            sys.stdout.write('{} Masked Magnetogrm exists...skipping\n')

        masked_images.append(
            'masked_magnetograms/' + path_to_file
        )

    return masked_images


def mag_variations(start_date, end_date):
    '''
    Generates one image per date in the interval start date and end date
    :param start_date:
    :param end_date:
    :return:
    '''

    result_list = list()
    _date = start_date

    sys.stdout.write('Starting Process\n')
    while _date <= end_date:
        sys.stdout.write('Date {}\n'.format(_date))

        vis_images = get_images(
            _date,
            series='hmi.ic_nolimbdark_720s',
            cadence='1d@720s',
            segment='continuum'
        )

        aia_images = get_images(
            _date,
            series='aia.lev1_uv_24s',
            cadence='1d@720s',
            segment='continuum',
            wavelength=1600
        )

        hmi_images = get_images(_date, series='hmi.M_720s', cadence='1d@720s', segment='magnetogram')

        masked_hmi = mask_magnetograms(hmi_images, [vis_images, aia_images])

        hmi_list = do_hmi_areaprep(hmi_images)

        vis_masks = get_vis_masks(vis_images)
        aia_masks = get_aia_masks(aia_images)

        running_mean_hmi = running_mean(masked_hmi, window_size=10)
        running_mean_hmi = running_mean(running_mean_hmi, window_size=len(running_mean_hmi))
        result_list.append(running_mean_hmi[0])
        _date = _date + timedelta(days=1)

        # delete_files('data')
    return result_list


def analyse(start_date, end_date):
    result = mag_variations(start_date, end_date)
    return result
    # car_map = get_car_map(result)


if __name__ == '__main__':
    today = datetime.datetime.utcnow().date()
    week_before = today - timedelta(days=8)
    result = analyse(week_before, week_before)