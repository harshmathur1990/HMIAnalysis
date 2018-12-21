import os
import sys


class File(object):

    def __init__(self, id, filename):
        self._id = id
        self._filename = filename

    @property
    def filename(self):
        return self._filename

    @property
    def id(self):
        return self._id


class DataList(object):

    def __init__(self, r, files_info):
        self._r = r
        self._files_info = files_info


    def __iter__(self):
        return self

    def __next__(self):
        for _file in self._files_info:
            yield _file

    @property
    def request(self):
        return self._r

    def download_file(self, file, output_dir='data'):

        assert isinstance(file, File)
        if not os.path.exists(output_dir + '/' + file.filename):
            sys.stdout.write('{} does not exist, downloading...\n'.format(file.filename))
            self.request.download(output_dir, id)
        else:
            sys.stdout.write('{} exists, skipping...\n'.format(file.filename))

        return output_dir + '/' + file.filename