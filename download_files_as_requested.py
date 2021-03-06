import datetime
from utils import get_images


if __name__ == '__main__':

    _start_date = datetime.datetime(2014, 1, 1).date()

    days = 15

    vis_images = get_images(
        _start_date,
        'hmi.ic_nolimbdark_720s',
        '{}d@15m'.format(days),
        'continuum'
    )

    aia_images = get_images(
        _start_date,
        'aia.lev1_uv_24s',
        '{}d@15m'.format(days),
        'image',
        1600
    )

    for vis_image in vis_images:
        vis_image.download_file(output_dir='/home/harsh/data')
    for aia_image in aia_images:
        aia_image.download_file(output_dir='/home/harsh/data')
