# -*- coding: utf-8 -*-
import sys
import os
import datetime
from datetime import timedelta
import numpy as np
import traceback
# from concurrent.futures import ProcessPoolExecutor
# from user_pools import NoDaemonPool as Pool
from chains import CreateCarringtonMap, MaskingMagnetograms, SouvikRework
from utils import running_mean, get_images, Base, engine
from model import Record


def do_work_on_images(hmi_image, aia_image, vis_image):
    sys.stdout.write(
        'Working on Files: {}:{}:{}\n'.format(
            hmi_image.filename,
            aia_image.filename,
            vis_image.filename
        )
    )
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


def do_souvik_work(hmi_image, aia_image, vis_image, date_object):
    sys.stdout.write(
        'Working on Files: {}:{}:{}\n'.format(
            hmi_image.filename,
            aia_image.filename,
            vis_image.filename
        )
    )

    hmi_chain = SouvikRework(
        operation_name='souvik',
        aia_file=aia_image,
        hmi_ic_file=vis_image,
        date_object=date_object
    )

    previous_operation = hmi_chain.process(hmi_image)
    hmi_image.delete('aiaprep')
    hmi_image.delete('souvik')

    return previous_operation


def process_for_date(
        _date,
        max_outer_executor,
        max_delete_outer_executor
):
    sys.stdout.write('Date {}\n'.format(_date))

    # future_list = list()
    # executor = Pool(3)

    # future_list.append(
    #     executor.apply_async(
    #         get_images,
    #         args=(
    #             _date,
    #             'hmi.ic_nolimbdark_720s',
    #             '1d@720s',
    #             'continuum',
    #         )
    #     )
    # )

    # future_list.append(
    #     executor.apply_async(
    #         get_images,
    #         args=(
    #             _date,
    #             'aia.lev1_uv_24s',
    #             '1d@720s',
    #             'image',
    #             1600,
    #         )
    #     )
    # )

    # future_list.append(
    #     executor.apply_async(
    #         get_images,
    #         args=(
    #             _date,
    #             'hmi.M_720s',
    #             '1d@720s',
    #             'magnetogram',
    #         )
    #     )
    # )

    # vis_images = future_list[0].get()
    # aia_images = future_list[1].get()
    # hmi_images = future_list[2].get()

    vis_images = get_images(
        _date,
        'hmi.ic_nolimbdark_720s',
        '1d@720s',
        'continuum'
    )

    aia_images = get_images(
        _date,
        'aia.lev1_uv_24s',
        '1d@720s',
        'image',
        1600
    )

    hmi_images = get_images(
        _date,
        'hmi.M_720s',
        '1d@720s',
        'magnetogram'
    )

    all_images = list()

    all_images.extend(
        vis_images
    )

    all_images.extend(
        aia_images
    )

    all_images.extend(
        hmi_images
    )

    for image in all_images:
        image.download_file()

    sys.stdout.write(
        'Got All Images List for Date {}\n'.format(_date)
    )

    # outer_executor = Pool(max_outer_executor)

    if not hmi_images[0].is_exist_in_directory('mean') or \
        not hmi_images[0].is_exist_in_directory(
            'mean', suffix='smoothed'):

        # future_list = list()
        for hmi_image, aia_image, vis_image in zip(
                hmi_images, aia_images, vis_images):

            previous_operation = do_work_on_images(
                hmi_image,
                aia_image,
                vis_image
            )
            # future_list.append(
            #     outer_executor.apply_async(
            #         do_work_on_images,
            #         args=(
            #             hmi_image,
            #             aia_image,
            #             vis_image,
            #         )
            #     )
            # )

        # for _future in future_list:
        #     previous_operation = _future.get()

        intermediate_running_mean_hmi = running_mean(
            hmi_images,
            previous_operation,
            window_size=10
        )

        running_mean(
            intermediate_running_mean_hmi,
            previous_operation,
            operation_name='mean',
            window_size=len(intermediate_running_mean_hmi),
            suffix='smoothed'
        )

        running_mean_hmi = running_mean(
            hmi_images,
            previous_operation,
            operation_name='mean',
            window_size=len(hmi_images),
        )

        sys.stdout.write('Deleting Data for {}\n'.format(_date))

        # delete_outer_executor = Pool(
        #     max_delete_outer_executor
        # )

        # delete_list = list()

        for hmi_image, aia_image, vis_image in zip(
                hmi_images, aia_images, vis_images):
            hmi_image.delete(
                previous_operation.operation_name
            )
            hmi_image.delete(
                'running_mean'
            )
            # aia_image.delete_data()
            # vis_image.delete_data()
            # hmi_image.delete_data()
            # delete_list.append(
            #     delete_outer_executor.apply_async(
            #         hmi_image.delete,
            #         args=(previous_operation.operation_name,)
            #     )
            # )
            # delete_list.append(
            #     delete_outer_executor.apply_async(
            #         hmi_image.delete,
            #         args=('running_mean',)
            #     )
            # )
            # delete_list.append(
            #     delete_outer_executor.apply_async(
            #         aia_image.delete_data
            #     )
            # )
            # delete_list.append(
            #     delete_outer_executor.apply_async(
            #         vis_image.delete_data
            #     )
            # )
            # delete_list.append(
            #     delete_outer_executor.apply_async(
            #         hmi_image.delete_data
            #     )
            # )

        # for _delete_element in delete_list:
        #     _delete_element.get()

        return running_mean_hmi[0]

    return hmi_images[0]


def mag_variations(
        start_date,
        end_date,
        max_super_outer_executor=4,
        max_outer_executor=4,
        max_delete_outer_executor=5
):
    '''
    Generates one image per date in the interval start date and end date
    :param start_date:
    :param end_date:
    :return:
    '''

    mean_list = list()
    _date = start_date

    sys.stdout.write('Starting Process\n')

    # super_outer_executor = Pool(
    # max_super_outer_executor)

    # date_element_list = list()

    while _date <= end_date:
        mean_result = process_for_date(
            _date,
            max_outer_executor,
            max_delete_outer_executor
        )
        mean_list.append(mean_result)
        # date_element_list.append(
        #     super_outer_executor.apply_async(
        #         process_for_date,
        #         args=(
        #             _date,
        #             max_outer_executor,
        #             max_delete_outer_executor,
        #         )
        #     )
        # )
        _date = _date + timedelta(days=1)

    # for date_element in date_element_list:
        # mean_result = date_element.get()
        # mean_list.append(mean_result)

    return mean_list


def analyse_images(start_date, end_date):
    mean_list = mag_variations(
        start_date, end_date, max_super_outer_executor=1)
    return mean_list
    # car_map = get_car_map(result)


def souvik_verify(start_date, no_of_years):

    _start_date = start_date

    for index in np.arange(no_of_years):

        sys.stdout.write('Startng work for Year: {}\n'.format(_start_date))

        try:
            vis_images = get_images(
                _start_date,
                'hmi.ic_nolimbdark_720s',
                '365d@24h',
                'continuum'
            )

            aia_images = get_images(
                _start_date,
                'aia.lev1_uv_24s',
                '365d@24h',
                'image',
                1600
            )

            hmi_images = get_images(
                _start_date,
                'hmi.M_720s',
                '365d@24h',
                'magnetogram'
            )

            all_images = list()

            all_images.extend(
                vis_images
            )

            all_images.extend(
                aia_images
            )

            all_images.extend(
                hmi_images
            )

            for image in all_images:
                image.download_file()

        except Exception:

            err = traceback.format_exc()
            sys.stdout.write(err)
            _start_date = _start_date.replace(
                year=_start_date.year + 1
            )

            continue

        _date = _start_date

        for hmi_image, aia_image, vis_image in zip(
            hmi_images, aia_images, vis_images
        ):

            sys.stdout.write('Startng work for Date: {}\n'.format(_date))

            # hmi_image.date = _date
            # aia_image.date = _date
            # vis_image.date = _date

            record = Record.find_by_date(_date)

            if not record:
                do_souvik_work(
                    hmi_image,
                    aia_image,
                    vis_image,
                    _date
                )
            else:
                sys.stdout.write('Data Exists for Date: {}\n'.format(_date))

            aia_image.delete_data()
            vis_image.delete_data()
            hmi_image.delete_data()

            _date = _date + timedelta(days=1)

        _start_date = _start_date.replace(
            year=_start_date.year + 1
        )


def run():
    from_date = datetime.date(year=2011, month=1, day=1)
    # to_date = datetime.date(year=2015, month=12, day=17)
    # analyse_images(from_date, to_date)
    souvik_verify(from_date, 8)


if __name__ == '__main__':
    if not os.path.exists('hmi.db'):
        Base.metadata.create_all(engine)
    run()
