# -*- coding: utf-8 -*-
import sys
import os
import datetime
from datetime import timedelta
import numpy as np
import traceback
# from concurrent.futures import ProcessPoolExecutor
# from user_pools import NoDaemonPool as Pool
from chains import SouvikRework
from utils import get_images, Base, engine
from model import Record


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


def souvik_verify(start_date, no_of_years, days=365):

    _start_date = start_date

    for index in np.arange(no_of_years):

        sys.stdout.write('Startng work for Year: {}\n'.format(_start_date))

        try:
            vis_images = get_images(
                _start_date,
                'hmi.ic_nolimbdark_720s',
                '{}d@24h'.format(days),
                'continuum'
            )

            aia_images = get_images(
                _start_date,
                'aia.lev1_uv_24s',
                '{}d@24h'.format(days),
                'image',
                1600
            )

            hmi_images = get_images(
                _start_date,
                'hmi.M_720s',
                '{}d@24h'.format(days),
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
    from_date = datetime.date(
        year=int(sys.argv[1]),
        month=int(sys.argv[2]),
        day=int(sys.argv[3]),
    )
    # to_date = datetime.date(year=2015, month=12, day=17)
    # analyse_images(from_date, to_date)
    souvik_verify(from_date, 1, days=int(sys.argv[4]))


if __name__ == '__main__':
    if not os.path.exists('hmi.db'):
        Base.metadata.create_all(engine)
    run()
