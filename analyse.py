# -*- coding: utf-8 -*-
import operator
import sys
import datetime
from datetime import timedelta
import os
import copy
from astropy.io import fits
from skimage.measure import label, regionprops

from chains import CreateCarringtonMap, MaskingMagnetograms
from utils import running_mean, get_images, apply_mask





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
            segment='image',
            wavelength=1600
        )

        hmi_images = get_images(_date, series='hmi.M_720s', cadence='1d@720s', segment='magnetogram')

        for hmi_image, aia_image, vis_image in zip(hmi_images, aia_images, vis_images):
            hmi_chain = CreateCarringtonMap(
                'carrington',
                aia_file=aia_image,
                hmi_ic_file=vis_image
            ).set_prev(
                MaskingMagnetograms(
                    'masked_magnetograms',
                    aia_file=aia_image,
                    hmi_ic_file=vis_image
                )
            )


            previous_operation = hmi_chain.process(hmi_image)
            hmi_image.delete('aiaprep')

        running_mean_hmi = running_mean(hmi_images, previous_operation, window_size=10)
        running_mean_hmi = running_mean(running_mean_hmi, previous_operation, operation_name='mean', window_size=len(running_mean_hmi))
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