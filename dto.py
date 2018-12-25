import os
import sys
from astropy.io import fits
import sunpy.io.fits


class File(object):

    def __init__(self, id, filename, r, directory='data'):
        self._id = id
        self._filename = filename
        self._directory = directory
        self._r = r

    @property
    def filename(self):
        return self._filename

    @property
    def id(self):
        return self._id

    @property
    def path(self):
        return os.path.join(self._directory, self._filename)

    @property
    def request(self):
        return self._r

    def _get_path(self, directory):
        return os.path.join(directory, self._filename)

    def get_path(self, directory):
        return self._get_path(directory)

    def is_exist_in_directory(self, directory):
        return os.path.exists(self._get_path(directory))

    def get_fits_hdu(self, directory):
        path = self._get_path(directory)
        fits_image = fits.open(path)

        rv_fits_hdu = None

        for fits_hdu in fits_image:
            fits_hdu.verify('fix')
            if fits_hdu.data is not None:
                rv_fits_hdu = fits_hdu
                break

        return rv_fits_hdu

    def save(self, operation_name, data, header):
        sunpy.io.fits.write(self._get_path(operation_name), data=data, header=header)

    def delete(self, operation_name):
        if operation_name and self.filename and operation_name != 'data':
            os.remove(self._get_path(operation_name))

    def download_file(self, output_dir='data', fname_from_rec=False):

        if not os.path.exists(output_dir + '/' + self.filename):
            sys.stdout.write('{} does not exist, downloading...\n'.format(self.filename))
            self.request.download(output_dir, self.id, fname_from_rec=fname_from_rec)

        else:
            sys.stdout.write('{} exists, skipping...\n'.format(self.filename))

        return self
