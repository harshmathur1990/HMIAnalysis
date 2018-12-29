# -*- coding: utf-8 -*-
import sys
import datetime
from datetime import timedelta
from concurrent.futures import ProcessPoolExecutor

from chains import CreateCarringtonMap, MaskingMagnetograms
from utils import running_mean, get_images


def do_work_on_images(hmi_image, aia_image, vis_image):
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

    return previous_operation


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

        future_list = list()
        with ProcessPoolExecutor(max_workers=3) as executor:

            future_list.append(
                executor.submit(
                    get_images,
                    _date,
                    'hmi.ic_nolimbdark_720s',
                    '1d@720s',
                    'continuum'
                )
            )

            future_list.append(
                executor.submit(
                    get_images,
                    _date,
                    'aia.lev1_uv_24s',
                    '1d@720s',
                    'image',
                    1600
                )
            )

            future_list.append(
                executor.submit(
                    get_images,
                    _date,
                    'hmi.M_720s',
                    '1d@720s',
                    'magnetogram'
                )
            )

        vis_images = future_list[0].result()
        aia_images = future_list[1].result()
        hmi_images = future_list[2].result()

        outer_executor = ProcessPoolExecutor(max_workers=4)

        future_list = list()
        for hmi_image, aia_image, vis_image in zip(hmi_images, aia_images, vis_images):
            future_list.append(
                outer_executor.submit(
                    do_work_on_images,
                    hmi_image,
                    aia_image,
                    vis_image
                )
            )

        for _future in future_list:
            previous_operation = _future.result()
        running_mean_hmi = running_mean(hmi_images, previous_operation, window_size=10)
        running_mean_hmi = running_mean(running_mean_hmi, previous_operation, operation_name='mean', window_size=len(running_mean_hmi))
        result_list.append(running_mean_hmi[0])
        _date = _date + timedelta(days=1)

        # delete_files('data')
    return result_list


def analyse_images(start_date, end_date):
    result = mag_variations(start_date, end_date)
    return result
    # car_map = get_car_map(result)


def run():
    today = datetime.datetime.utcnow().date() - timedelta(days=13)
    week_before = today - timedelta(days=3 * 365)
    result = analyse_images(week_before, week_before)


if __name__ == '__main__':
    run()