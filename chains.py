from abc import abstractmethod, ABC
import copy
import numpy as np
import sys
import operator
import skimage.transform
import scipy.signal
import skimage.draw
import sunpy.map
import sunpy.instr.aia
from skimage.measure import label, regionprops
from skimage.morphology import closing, square

from utils import apply_mask, set_nan_to_non_sun


class Chain(ABC):

    def __init__(self, operation_name):
        self._operation_name = operation_name
        self._prev = None

    @property
    def operation_name(self):
        return self._operation_name

    def set_prev(self, chain):
        self._prev = chain
        return self

    def process(self, file):
        if not file.is_exist_in_directory(self._operation_name):
            sys.stdout.write('Operation: {}, File: {} performing operation\n'.format(self._operation_name, file.filename))

            previous_operation_name = None
            previous = None

            if self._prev:
                previous = self._prev.process(file)
                previous_operation_name = previous.operation_name

            _actual_result = self.actual_process(file, previous_operation_name)

            if _actual_result:
                data, header = _actual_result
                file.save(self._operation_name, data, header)

                file.delete(previous_operation_name)
        else:
            sys.stdout.write('Operation: {}, File: {} reusing result\n'.format(self._operation_name, file.filename))

        return self

    @abstractmethod
    def actual_process(self, file, previous_operation_name=None):
        pass


class Thresholding(Chain):

    def __init__(self, operation_name, k, op, post_processor=None, radius_factor=None):
        super().__init__(operation_name)
        self._k = k
        self._op = op
        self._post_processor=post_processor
        self._radius_factor = radius_factor


    def _do_thresholding(self, image, header):

        mean = np.nanmean(image)
        std = np.nanstd(image)

        threshold = mean + (self._k * std)

        result = np.zeros(shape=image.shape)

        result[self._op(image, threshold)] = 1.0

        result = set_nan_to_non_sun(result, header, factor=self._radius_factor)

        result = closing(result, square(3)) # 1.8 sec per call

        return result

    def actual_process(self, file, previous_operation_name=None):
        fits_array = file.get_fits_hdu(previous_operation_name)

        image = self._do_thresholding(fits_array.data, fits_array.header)

        if self._post_processor:
            image = self._post_processor(image)

        return image, fits_array.header


class LimbDarkeningCorrection(Chain):

    def __init__(self, operation_name, radius_factor=None):
        super().__init__(operation_name)
        self._radius_factor = radius_factor

    def _do_limb_darkening_correction(self, image, header):

        small_image = skimage.transform.resize(
            image,
            output_shape=(512, 512),
            order=3,
            preserve_range=True
        )

        small_image[np.isnan(small_image)] = 0.0

        small_median = scipy.signal.medfilt2d(small_image, 105) # Slow, 20 secs per call

        large_median = skimage.transform.resize(
            small_median,
            output_shape=image.shape,
            order=3,
            preserve_range=True
        )

        result = np.divide(image, large_median)

        result = set_nan_to_non_sun(result, header, factor=self._radius_factor)

        return result

    def actual_process(self, file, previous_operation_name=None):
        fits_array = file.get_fits_hdu(previous_operation_name)

        image = self._do_limb_darkening_correction(fits_array.data, fits_array.header)

        return image, fits_array.header


class AIAPrep(Chain):

    def __init__(self, operation_name, radius_factor=None):
        super().__init__(operation_name)
        self._radius_factor = radius_factor

    def _do_aiaprep(self, data, header):
        header['HGLN_OBS'] = 0

        aiamap = sunpy.map.Map(
            data,
            header
        )

        aiamap_afterprep = sunpy.instr.aia.aiaprep(aiamap=aiamap) # Slow, 7 secs per call

        result = set_nan_to_non_sun(aiamap_afterprep.data, aiamap_afterprep.meta, factor=self._radius_factor)

        return result, aiamap_afterprep.meta

    def actual_process(self, file, previous_operation_name=None):
        fits_array = file.get_fits_hdu(previous_operation_name)

        return self._do_aiaprep(fits_array.data, fits_array.header)


class DownloadFiles(Chain):

    def __init__(self, operation_name, fname_from_rec=False):
        super().__init__(operation_name)
        self._fname_from_rec = fname_from_rec

    def actual_process(self, file, previous_operation_name=None):
        file.download_file(fname_from_rec=self._fname_from_rec)


class MaskingMagnetograms(Chain):

    def __init__(self, operation_name, aia_file, hmi_ic_file):
        super().__init__(operation_name)
        self._aia_file = aia_file
        self._hmi_ic_file = hmi_ic_file

    def actual_process(self, file, previous_operation_name=None):

        def do_area_filtering(mask):
            area_per_pixel = (0.6 / 60) * (0.6 / 60)

            pixel_in_onetenth_arcminute = 0.1 / area_per_pixel

            label_image = label(mask)
            regions = regionprops(label_image)
            for region in regions:
                if region.area < pixel_in_onetenth_arcminute:
                    for coords in region.coords:
                        mask[coords[0]][coords[1]] = 0.0
            return mask


        aia_chain = Thresholding(
            operation_name='mask',
            k=1.71,
            op=operator.ge,
            post_processor=do_area_filtering,
            radius_factor=0.97
        ).set_prev(
            LimbDarkeningCorrection(operation_name='ldr', radius_factor=0.97).set_prev(
                AIAPrep(operation_name='aiaprep', radius_factor=1.0).set_prev(
                    DownloadFiles(operation_name='data', fname_from_rec=True)
                )
            )
        )

        hmi_ic_chain = Thresholding(
            operation_name='mask',
            k=-5,
            op=operator.le,
            radius_factor=1.0
        ).set_prev(
            AIAPrep(operation_name='aiaprep', radius_factor=1.0).set_prev(
                DownloadFiles(operation_name='data')
            )
        )

        hmi_mag_chain = AIAPrep(operation_name='aiaprep', radius_factor=1.0).set_prev(
            DownloadFiles(operation_name='data')
        )

        previous_operation_aia = aia_chain.process(self._aia_file)
        previous_operation_hmi_ic = hmi_ic_chain.process(self._hmi_ic_file)
        previous_operation_hmi_mag = hmi_mag_chain.process(file)


        aia_mask = self._aia_file.get_fits_hdu(previous_operation_aia.operation_name)
        hmi_ic_mask = self._hmi_ic_file.get_fits_hdu(previous_operation_hmi_ic.operation_name)
        hmi_mag_image = file.get_fits_hdu(previous_operation_hmi_mag.operation_name)

        masked_image = copy.deepcopy(hmi_mag_image.data)
        masked_image = apply_mask(masked_image, aia_mask.data)
        masked_image = apply_mask(masked_image, hmi_ic_mask.data)

        self._aia_file.delete('mask')
        self._hmi_ic_file.delete('mask')
        return masked_image, hmi_mag_image.header


class CreateCarringtonMap(Chain):

    def __init__(self, operation_name, aia_file, hmi_ic_file):
        super().__init__(operation_name)
        self._aia_file = aia_file
        self._hmi_ic_file = hmi_ic_file

    def actual_process(self, file, previous_operation_name=None):
        # The file is HMI Magnetogram masked image.
        # If the file exists, use it and extract the map information,
        # else create the file using the chaining.
        fits_array = file.get_fits_hdu(previous_operation_name)
        return fits_array.data, fits_array.header