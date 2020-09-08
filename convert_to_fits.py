import sys
import imageio
from pathlib import Path
import skimage.color
import sunpy.io
from astropy.io import fits


def convert_to_fits(base_path):
    if isinstance(base_path, str):
        base_path = Path(base_path)

    all_things = base_path.glob('**/*')

    all_folders = [
        x for x in all_things if x.is_dir()
    ]

    for a_dir in all_folders:
        all_files = a_dir.glob('**/*')

        png_files = [
            x for x in all_files if x.is_file() and
            x.name.endswith('.png') and
            x.name.startswith('aiaprep')
        ]

        if len(png_files) <= 0:
            sys.stdout.write('{}'.format(a_dir))
            continue
        im = imageio.imread(png_files[0])

        fits_filename = png_files[0].name + '.fits'
        sunpy.io.fits.write(
            a_dir / fits_filename,
            skimage.color.rgb2gray(im),
            dict(),
            hdu_type=fits.CompImageHDU,
            overwrite=True
        )

        # sys.stdout.write('{}\n'.format(a_dir / fits_filename))
