# -*- coding: utf-8 -*-
import sys
import os
import datetime
from pathlib import Path
import traceback
from dto import File
# from concurrent.futures import ProcessPoolExecutor
# from user_pools import NoDaemonPool as Pool
from chains import SouvikRework
from utils import get_images, Base, engine
from model import Record
from dotenv import load_dotenv
from utils import initialize, prepare_get_corresponding_images, \
    get_date, get_julian_day


def do_souvik_work(hmi_image, aia_image, vis_image):
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
        hmi_ic_file=vis_image
    )

    previous_operation = hmi_chain.process(hmi_image)

    return previous_operation


def get_all_images_from_server(_start_date, days):
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

        return hmi_images, vis_images, aia_images

    except Exception:

        err = traceback.format_exc()
        sys.stdout.write(err)
        sys.exit(1)


def get_data_objects_from_local(_start_date):

    base_path = Path('data')

    data_path = base_path / _start_date.strftime('%Y.%m.%d')

    everything = data_path.glob('**/*')

    hmi_files = [
        x for x in everything if x.is_file() and
        x.name.endswith('.fits') and x.name.startswith('hmi.m_720s')
    ]

    everything = data_path.glob('**/*')

    aia_files = [
        x for x in everything if x.is_file() and
        x.name.endswith('.fits') and x.name.startswith('aia')
    ]

    everything = data_path.glob('**/*')

    vis_files = [
        x for x in everything if x.is_file() and
        x.name.endswith('.fits') and x.name.startswith('hmi.ic')
    ]

    hmi_images = list()

    aia_images = list()

    vis_images = list()

    for index, hmi_file in enumerate(hmi_files):
        hmi_images.append(
            File(
                filename=hmi_file.name,
                r=None,
                id=index,
                date_object=_start_date
            )
        )

    for index, aia_file in enumerate(aia_files):
        aia_images.append(
            File(
                filename=aia_file.name,
                r=None,
                id=index,
                date_object=_start_date
            )
        )

    for index, vis_file in enumerate(vis_files):
        vis_images.append(
            File(
                filename=vis_file.name,
                r=None,
                id=index,
                date_object=_start_date
            )
        )

    return hmi_images, vis_images, aia_images


def souvik_verify(start_date, days=365, data_present=False):

    _start_date = start_date

    sys.stdout.write('Startng work for Year: {}\n'.format(_start_date))

    if not data_present:
        hmi_images, vis_images, aia_images = get_all_images_from_server(
            _start_date, days
        )

    else:
        hmi_images, vis_images, aia_images = get_data_objects_from_local(
            _start_date
        )

    download_only = int(sys.argv[6])

    if download_only == 1:
        return

    get_corresponding_images = prepare_get_corresponding_images(
        aia_images, vis_images
    )

    divisor = int(sys.argv[7])

    remainder = int(sys.argv[8])

    hmi_images.sort(key=get_julian_day)

    for index, hmi_image in enumerate(hmi_images):

        if index % divisor != remainder:
            continue

        aia_image, vis_image, status = get_corresponding_images(
            hmi_image
        )

        if not status:
            sys.stdout.write(
                'No AIA or VIS image found for filename: {}\n'.format(
                    hmi_image.filename
                )
            )
            continue

        _date = get_date(hmi_image)

        sys.stdout.write('Startng work for Date: {}\n'.format(_date))

        record = Record.find_by_date(_date)

        if not record:
            do_souvik_work(
                hmi_image,
                aia_image,
                vis_image
            )
        else:
            sys.stdout.write('Data Exists for Date: {}\n'.format(_date))

        aia_image.delete('aiaprep')
        aia_image.delete('aligned_data')
        aia_image.delete('ldr')
        aia_image.delete('mask', suffix='plages')
        aia_image.delete('mask', suffix='active_networks')
        vis_image.delete('aiaprep')
        vis_image.delete('mask')
        hmi_image.delete('aiaprep')
        hmi_image.delete('crop_hmi_afterprep')
        hmi_image.delete('souvik')


def run():
    load_dotenv(verbose=True)
    data_present = False
    try:
        argument = int(sys.argv[5])
        if argument > 0:
            data_present = True
    except Exception:
        data_present = False

    from_date = datetime.date(
        year=int(sys.argv[1]),
        month=int(sys.argv[2]),
        day=int(sys.argv[3])
    )
    # to_date = datetime.date(year=2015, month=12, day=17)
    # analyse_images(from_date, to_date)
    souvik_verify(
        from_date,
        days=int(sys.argv[4]),
        data_present=data_present
    )


if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    day = int(sys.argv[3])
    divisor = int(sys.argv[7])
    remainder = int(sys.argv[8])
    if not os.path.exists(
        'hmi_{}_{}_{}_{}_{}.db'.format(
            year,
            month,
            day,
            divisor,
            remainder
        )
    ):
        Base.metadata.create_all(engine)
    initialize()
    run()
