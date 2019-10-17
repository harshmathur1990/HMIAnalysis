# -*- coding: utf-8 -*-
import os
import sys
import numpy as np
import sunpy.io
import sunpy.io.fits
import matplotlib.pyplot as plt
from six.moves.urllib.error import HTTPError, URLError
from decor import retry
from utils import timeit
# from utils import sem


class File(object):

    def __init__(self, id, filename, r, date_object, directory='data'):
        self._id = id
        self._filename = filename
        self._directory = directory
        self._r = r
        self._date = date_object

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

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, value):
        self._date = value

    def _get_path(self, directory, suffix=None):
        if not os.path.isdir(
                directory + '/' + self._date.strftime('%Y.%m.%d')):
            os.mkdir(directory + '/' + self._date.strftime('%Y.%m.%d'))

        filename = self._filename
        if suffix:
            filename = suffix + '_' + self._filename
        return os.path.join(
            directory, self._date.strftime('%Y.%m.%d'), filename)

    def get_path(self, directory, suffix):
        return self._get_path(directory, suffix=suffix)

    def is_exist_in_directory(self, directory, suffix=None):
        return os.path.exists(self._get_path(directory, suffix=suffix))

    @timeit
    def get_fits_hdu(self, directory, suffix=None):
        path = self._get_path(directory, suffix=suffix)

        data_header_pairs = sunpy.io.read_file(path)

        data, header = None

        if len(data_header_pairs) > 1:
            hdpair = data_header_pairs[1]
        else:
            hdpair = data_header_pairs[0]

        data, header = hdpair.data, hdpair.header

        if directory == 'data':
            _data = data.copy()
            _data -= np.nanmin(_data)
            _data[np.isnan(_data)] = 0.0
            _data[np.isinf(_data)] = 0.0
            _data = _data / np.nanmax(_data)
            _data = np.uint8(_data * 255)
            plt.imsave(
                path + '.png',
                _data,
                cmap='gray',
                format='png'
            )

        return data, header

    @timeit
    def read_headers(self, directory, suffix=None):
        path = self._get_path(directory, suffix=suffix)
        return sunpy.io.read_file_header(path)[1]

    def save(self, operation_name, data, header, suffix=None):
        filename = self._get_path(operation_name, suffix=suffix)
        sys.stdout.write('Saving {}\n'.format(filename))
        sunpy.io.fits.write(
            filename,
            data,
            header
        )
        _data = data.copy()
        _data -= np.nanmin(_data)
        _data[np.isnan(_data)] = 0.0
        _data[np.isinf(_data)] = 0.0
        _data = _data / np.nanmax(_data)
        _data = np.uint8(_data * 255)
        plt.imsave(filename + '.png', _data, cmap='gray', format='png')

    def _delete(self, operation_name, suffix=None):
        path_to_be_deleted = self._get_path(operation_name, suffix=suffix)
        if not int(os.getenv('DEBUG') or 0):
            os.remove(path_to_be_deleted)

    def delete(self, operation_name, suffix=None):
        if operation_name and self.filename and operation_name != 'data':
            path_to_be_deleted = self._get_path(operation_name, suffix=suffix)
            if os.path.exists(path_to_be_deleted):
                self._delete(operation_name, suffix=suffix)

    def delete_data(self):
        self._delete('data')

    @retry((HTTPError, URLError))
    def download_file(self, output_dir='data', fname_from_rec=False):

        date_folder = output_dir + '/' + \
            self._date.strftime('%Y.%m.%d')

        if not os.path.isdir(date_folder):
            os.mkdir(date_folder)

        if not os.path.exists(
                date_folder + '/' + self.filename
        ):
            sys.stdout.write(
                '{} does not exist, downloading...\n'.format(self.filename))
            # sys.stdout.write(
            #     'Value of Semaphore before downloading: {}\n'.format(sem)
            # )
            # sem.acquire()
            # sys.stdout.write(
            #     'Value of Semaphore while downloading: {}\n'.format(sem)
            # )
            self.request.download(
                date_folder, self.id, fname_from_rec=fname_from_rec)
            # sem.release()
            # sys.stdout.write(
            #     'Value of Semaphore after downloading: {}\n'.format(sem)
            # )

        else:
            sys.stdout.write('{} exists, skipping...\n'.format(self.filename))

        return self


class PreviousOperation(object):

    def __init__(self, file, previous_op, suffix):
        self._file = file
        self._previous_op = previous_op
        self._suffix = suffix

    def get_fits_hdu(self):
        return self._file.get_fits_hdu(
            directory=self._previous_op,
            suffix=self._suffix
        )
