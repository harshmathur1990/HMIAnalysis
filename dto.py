# -*- coding: utf-8 -*-
import os
import sys
from astropy.io import fits
import matplotlib.pyplot as plt


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

    def _get_path(self, directory, suffix=None):
        if not os.path.isdir(
                directory + '/' + self._date.strftime('%Y.%m.%d')):
            os.mkdir(directory + '/' + self._date.strftime('%Y.%m.%d'))

        filename = self._filename
        if suffix:
            filename = suffix + '_' + self._filename
        return os.path.join(directory, self._date.strftime('%Y.%m.%d'), filename)

    def get_path(self, directory, suffix):
        return self._get_path(directory, suffix=suffix)

    def is_exist_in_directory(self, directory, suffix=None):
        return os.path.exists(self._get_path(directory, suffix=suffix))

    def get_fits_hdu(self, directory):
        path = self._get_path(directory)
        fits_image = fits.open(path)

        rv_fits_hdu = None

        for fits_hdu in fits_image:
            fits_hdu.verify('silentfix')
            if fits_hdu.data is not None:
                rv_fits_hdu = fits_hdu
                break

        return rv_fits_hdu

    def _get_header_object(self, header):
        header = header.copy()

        # The comments need to be added to the header separately from the normal
        # kwargs. Find and deal with them:
        fits_header = fits.Header()
        # Check Header
        key_comments = header.pop('KEYCOMMENTS', False)

        for k, v in header.items():
            if isinstance(v, fits.header._HeaderCommentaryCards):
                if k == 'comment':
                    comments = str(v).split('\n')
                    for com in comments:
                        fits_header.add_comment(com)
                elif k == 'history':
                    hists = str(v).split('\n')
                    for hist in hists:
                        fits_header.add_history(hist)
                elif k != '':
                    fits_header.append(fits.Card(k, str(v).split('\n')))

            else:
                fits_header.append(fits.Card(k, v))

        if isinstance(key_comments, dict):
            for k, v in key_comments.items():
                # Check that the Card for the comment exists before trying to write to it.
                if k in fits_header:
                    fits_header.comments[k] = v
        elif key_comments:
            raise TypeError("KEYCOMMENTS must be a dictionary")

        return fits_header

    def save(self, operation_name, data, header, suffix=None):
        filename = self._get_path(operation_name, suffix=suffix)
        sys.stdout.write('Saving {}\n'.format(filename))
        hdu = fits.PrimaryHDU()
        header_object = self._get_header_object(header)
        comp_hdu = fits.CompImageHDU(data=data, header=header_object)
        hdul = fits.HDUList([hdu, comp_hdu])
        hdul.writeto(filename, output_verify='ignore')
        plt.imsave(filename + '.png', data, cmap='gray', format='png')

    def _delete(self, operation_name):
        path_to_be_deleted = self._get_path(operation_name)
        os.remove(path_to_be_deleted)

    def delete(self, operation_name):
        if operation_name and self.filename and operation_name != 'data':
            path_to_be_deleted = self._get_path(operation_name)
            if os.path.exists(path_to_be_deleted):
                self._delete(operation_name)

    def delete_data(self):
        self._delete('data')

    def download_file(self, output_dir='data', fname_from_rec=False):

        if not os.path.isdir(output_dir + '/' + self._date.strftime('%Y.%m.%d')):
            os.mkdir(output_dir + '/' + self._date.strftime('%Y.%m.%d'))

        if not os.path.exists(output_dir + '/' + self._date.strftime('%Y.%m.%d') + self.filename):
            sys.stdout.write(
                '{} does not exist, downloading...\n'.format(self.filename))
            self.request.download(
                output_dir + '/' + self._date.strftime('%Y.%m.%d'), self.id, fname_from_rec=fname_from_rec)

        else:
            sys.stdout.write('{} exists, skipping...\n'.format(self.filename))

        return self
