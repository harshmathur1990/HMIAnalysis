# -*- coding: utf-8 -*-
from abc import abstractmethod, ABC
# from concurrent.futures import ProcessPoolExecutor
# from user_pools import NoDaemonPool as Pool
import numpy as np
import sys
import operator
import skimage.transform
import scipy.signal
import skimage.draw
import skimage.filters
import sunpy.map
import sunpy.instr.aia
from skimage.measure import label, regionprops
from skimage.morphology import closing, square
from model import Record
from utils import apply_mask, set_nan_to_non_sun


class Chain(ABC):

    def __init__(self, operation_name, suffix=None):
        self._operation_name = operation_name
        self._suffix = suffix
        self._prev = None

    @property
    def operation_name(self):
        return self._operation_name

    def set_prev(self, chain):
        self._prev = chain
        return self

    def process(self, file):
        sys.stdout.write(
            'Operation: {}, File: {} Checking for result\n'.format(
                self._operation_name, file.filename)
        )

        if not file.is_exist_in_directory(self._operation_name, self._suffix):
            sys.stdout.write(
                'Operation: {}, File: {} Result not available\n'.format(
                    self._operation_name, file.filename
                )
            )

            previous_operation_name = None
            previous = None

            if self._prev:
                previous = self._prev.process(file)
                previous_operation_name = previous.operation_name

            sys.stdout.write(
                'Operation: {}, File: {} Performing Operation\n'.format(
                    self._operation_name, file.filename
                )
            )

            _actual_result = self.actual_process(
                file,
                previous_operation_name
            )

            if _actual_result:
                data, header = _actual_result
                file.save(
                    self._operation_name,
                    data,
                    header,
                    suffix=self._suffix
                )

                file.delete(previous_operation_name)
        else:
            sys.stdout.write(
                'Operation: {}, File: {} reusing result\n'.format(
                    self._operation_name, file.filename
                )
            )

        return self

    @abstractmethod
    def actual_process(self, file=None, previous_operation_name=None):
        pass


class Thresholding(Chain):

    def __init__(
        self,
        operation_name,
        k,
        op,
        suffix=None,
        k2=None,
        op2=None,
        post_processor=None,
        radius_factor=None,
        value_1=1.0,
        value_2=0.0
    ):
        super().__init__(operation_name, suffix)
        self._k = k
        self._op = op
        self._k2 = k2
        self._op2 = op2
        self._post_processor = post_processor
        self._radius_factor = radius_factor
        self._value_1 = value_1
        self._value_2 = value_2

    def _do_thresholding(self, image, header):

        mean = np.nanmean(image)
        std = np.nanstd(image)

        threshold = mean + (self._k * std)

        result = np.zeros(shape=image.shape)

        result[self._op(image, threshold)] = self._value_1

        if self._k2 and self._op2:
            threshold_2 = mean + (self._k2 + std)

            result[self._op2(image, threshold_2)] = self._value_2

        result = set_nan_to_non_sun(result, header, factor=self._radius_factor)

        # 1.8 sec per call, 4% of the program
        result = closing(result, square(3))

        return result

    def actual_process(self, file=None, previous_operation_name=None):

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

        # Slow, 20 secs per call, 30% time of the program
        small_median = scipy.signal.medfilt2d(small_image, 105)

        large_median = skimage.transform.resize(
            small_median,
            output_shape=image.shape,
            order=3,
            preserve_range=True
        )

        result = np.divide(image, large_median)

        result = set_nan_to_non_sun(result, header, factor=self._radius_factor)

        return result

    def actual_process(self, file=None, previous_operation_name=None):
        fits_array = file.get_fits_hdu(previous_operation_name)

        image = self._do_limb_darkening_correction(
            fits_array.data, fits_array.header)

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

        # Slow, 7 secs per call, 36% of the program
        aiamap_afterprep = sunpy.instr.aia.aiaprep(aiamap=aiamap)

        result = set_nan_to_non_sun(
            aiamap_afterprep.data,
            aiamap_afterprep.meta, factor=self._radius_factor)

        return result, aiamap_afterprep.meta

    def actual_process(self, file=None, previous_operation_name=None):
        fits_array = file.get_fits_hdu(previous_operation_name)

        return self._do_aiaprep(fits_array.data, fits_array.header)


class DownloadFiles(Chain):

    def __init__(self, operation_name, fname_from_rec=False):
        super().__init__(operation_name)
        self._fname_from_rec = fname_from_rec

    def actual_process(self, file=None, previous_operation_name=None):
        file.download_file(fname_from_rec=self._fname_from_rec)


def do_area_filtering(mask):
    area_per_pixel = (0.5 / 60) * (0.5 / 60)

    pixel_in_onetenth_arcminute = 0.1 / area_per_pixel

    label_image = label(mask)
    regions = regionprops(label_image)
    for region in regions:
        if region.area < pixel_in_onetenth_arcminute:
            for coords in region.coords:
                mask[coords[0]][coords[1]] = 0.0
    return mask


def function_proxy(func, *args):
    return func(*args)


class SouvikRework(Chain):

    def __init__(self, operation_name, aia_file, hmi_ic_file):
        super().__init__(operation_name)
        self._aia_file = aia_file
        self._hmi_ic_file = hmi_ic_file

    def actual_process(self, file=None, previous_operation_name=None):

        sys.stdout.write(
            'Started The Process for MaskingMagnetograms :{}\n'.format(
                file.filename
            )
        )
        aia_chain_plages = Thresholding(
            operation_name='mask',
            suffix='plages',
            k=1.71,
            op=operator.ge,
            post_processor=do_area_filtering,
            radius_factor=0.97
        ).set_prev(
            LimbDarkeningCorrection(
                operation_name='ldr',
                radius_factor=0.97
            ).set_prev(
                AIAPrep(
                    operation_name='aiaprep', radius_factor=1.0
                ).set_prev(
                    DownloadFiles(
                        operation_name='data', fname_from_rec=True
                    )
                )
            )
        )

        aia_chain_active_networks = Thresholding(
            operation_name='mask',
            suffix='active_networks',
            k=1.65,
            op=operator.ge,
            k2=1.71,
            op2=operator.ge,
            radius_factor=0.97,
            value_1=1.0,
            value_2=0.0
        ).set_prev(
            LimbDarkeningCorrection(
                operation_name='ldr',
                radius_factor=0.97
            ).set_prev(
                AIAPrep(
                    operation_name='aiaprep', radius_factor=1.0
                ).set_prev(
                    DownloadFiles(
                        operation_name='data', fname_from_rec=True
                    )
                )
            )
        )

        hmi_ic_chain = Thresholding(
            operation_name='mask',
            suffix=None,
            k=-5,
            op=operator.le,
            radius_factor=1.0
        ).set_prev(
            AIAPrep(operation_name='aiaprep', radius_factor=1.0).set_prev(
                DownloadFiles(operation_name='data')
            )
        )

        hmi_mag_chain = AIAPrep(
            operation_name='aiaprep',
            radius_factor=1.0
        ).set_prev(
            DownloadFiles(operation_name='data')
        )

        sys.stdout.write(
            'Created The Chains for MaskingMagnetograms :{}\n'.format(
                file.filename
            )
        )

        previous_operation_aia_plages = aia_chain_plages.process(
            self._aia_file,
        )
        previous_operation_active_networks = aia_chain_active_networks.process(
            self._aia_file,
        )
        previous_operation_hmi_ic = hmi_ic_chain.process(
            self._hmi_ic_file
        )
        previous_operation_hmi_mag = hmi_mag_chain.process(
            file
        )

        '''
        future_list = list()

        executor = Pool(4)

        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    aia_chain_plages.process,
                    self._aia_file,
                    'plages'
                )
            )
        )
        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    aia_chain_active_networks.process,
                    self._aia_file,
                    'active_networks'
                )
            )
        )
        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    hmi_ic_chain.process,
                    self._hmi_ic_file,
                )
            )
        )
        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    hmi_mag_chain.process,
                    file,
                )
            )
        )

        previous_operation_aia_plages = future_list[0].get()
        previous_operation_active_networks = future_list[1].get()
        previous_operation_hmi_ic = future_list[2].get()
        previous_operation_hmi_mag = future_list[3].get()

        '''

        sys.stdout.write(
            'Performed all the chains for : {}'.format(
                file.filename
            )
        )

        aia_mask_plages = self._aia_file.get_fits_hdu(
            previous_operation_aia_plages.operation_name,
            'plages'
        )
        aia_mask_active_networks = self._aia_file.get_fits_hdu(
            previous_operation_active_networks.operation_name,
            'active_networks'
        )
        hmi_ic_mask = self._hmi_ic_file.get_fits_hdu(
            previous_operation_hmi_ic.operation_name)
        hmi_mag_image = file.get_fits_hdu(
            previous_operation_hmi_mag.operation_name)

        masked_image = hmi_mag_image.data.copy()

        total_mask = np.add(
            np.add(
                aia_mask_plages.data,
                aia_mask_active_networks.data
            ),
            hmi_ic_mask.data
        )

        total_mask[total_mask >= 1.0] = 1.0

        masked_image = apply_mask(masked_image, total_mask)

        background_field = np.nansum(masked_image)

        no_of_background_field = len(np.where(total_mask == 0.0)[0])

        def get_no_of_pixel_and_field(mask, image):

            mask = mask.copy()
            image = image.copy()
            no_of_pixels = np.nansum(mask)

            ulta_mask = -1 * (mask - 1)

            _masked_image = apply_mask(image, ulta_mask)

            return no_of_pixels, np.nansum(_masked_image)

        no_of_active_pixel, active_field = get_no_of_pixel_and_field(
            aia_mask_active_networks.data, hmi_mag_image.data
        )

        no_of_sunspot_pixel, sunspot_field = get_no_of_pixel_and_field(
            hmi_ic_mask.data, hmi_mag_image.data
        )

        no_of_plage_pixel, plage_field = get_no_of_pixel_and_field(
            aia_mask_plages.data, hmi_mag_image.data
        )

        record = Record(
            date=file.date,
            no_of_pixel_sunspot=no_of_sunspot_pixel,
            total_mag_field_sunspot=sunspot_field,
            no_of_pixel_plage=no_of_plage_pixel,
            total_mag_field_plage=plage_field,
            no_of_pixel_active=no_of_active_pixel,
            total_mag_field_active=active_field,
            no_of_pixel_background=no_of_background_field,
            total_background_field=background_field
        )

        record.save()

        self._aia_file.delete('mask', suffix='plages')
        self._aia_file.delete('mask', suffix='active_networks')
        self._hmi_ic_file.delete('mask')
        return masked_image, hmi_mag_image.header


class MaskingMagnetograms(Chain):

    def __init__(self, operation_name, aia_file, hmi_ic_file):
        super().__init__(operation_name)
        self._aia_file = aia_file
        self._hmi_ic_file = hmi_ic_file

    def actual_process(self, file=None, previous_operation_name=None):

        sys.stdout.write(
            'Started The Process for MaskingMagnetograms :{}\n'.format(
                file.filename
            )
        )
        aia_chain_plages = Thresholding(
            operation_name='mask',
            suffix='plages',
            k=1.71,
            op=operator.ge,
            post_processor=do_area_filtering,
            radius_factor=0.97
        ).set_prev(
            LimbDarkeningCorrection(
                operation_name='ldr',
                radius_factor=0.97
            ).set_prev(
                AIAPrep(
                    operation_name='aiaprep', radius_factor=1.0
                ).set_prev(
                    DownloadFiles(
                        operation_name='data', fname_from_rec=True
                    )
                )
            )
        )

        aia_chain_active_networks = Thresholding(
            operation_name='mask',
            suffix='active_networks',
            k=1.65,
            op=operator.ge,
            k2=1.71,
            op2=operator.ge,
            radius_factor=0.97,
            value_1=1.0,
            value_2=0.0
        ).set_prev(
            LimbDarkeningCorrection(
                operation_name='ldr',
                radius_factor=0.97
            ).set_prev(
                AIAPrep(
                    operation_name='aiaprep', radius_factor=1.0
                ).set_prev(
                    DownloadFiles(
                        operation_name='data', fname_from_rec=True
                    )
                )
            )
        )

        hmi_ic_chain = Thresholding(
            operation_name='mask',
            suffix=None,
            k=-5,
            op=operator.le,
            radius_factor=1.0
        ).set_prev(
            AIAPrep(operation_name='aiaprep', radius_factor=1.0).set_prev(
                DownloadFiles(operation_name='data')
            )
        )

        hmi_mag_chain = AIAPrep(
            operation_name='aiaprep',
            radius_factor=1.0
        ).set_prev(
            DownloadFiles(operation_name='data')
        )

        sys.stdout.write(
            'Created The Chains for MaskingMagnetograms :{}\n'.format(
                file.filename
            )
        )

        previous_operation_aia_plages = aia_chain_plages.process(
            self._aia_file,
        )
        previous_operation_active_networks = aia_chain_active_networks.process(
            self._aia_file,
        )
        previous_operation_hmi_ic = hmi_ic_chain.process(
            self._hmi_ic_file
        )
        previous_operation_hmi_mag = hmi_mag_chain.process(
            file
        )

        '''
        future_list = list()

        executor = Pool(4)

        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    aia_chain_plages.process,
                    self._aia_file,
                    'plages'
                )
            )
        )
        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    aia_chain_active_networks.process,
                    self._aia_file,
                    'active_networks'
                )
            )
        )
        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    hmi_ic_chain.process,
                    self._hmi_ic_file,
                )
            )
        )
        future_list.append(
            executor.apply_async(
                function_proxy,
                args=(
                    hmi_mag_chain.process,
                    file,
                )
            )
        )

        previous_operation_aia_plages = future_list[0].get()
        previous_operation_active_networks = future_list[1].get()
        previous_operation_hmi_ic = future_list[2].get()
        previous_operation_hmi_mag = future_list[3].get()

        '''

        sys.stdout.write(
            'Performed all the chains for : {}'.format(
                file.filename
            )
        )

        aia_mask_plages = self._aia_file.get_fits_hdu(
            previous_operation_aia_plages.operation_name,
            'plages'
        )
        aia_mask_active_networks = self._aia_file.get_fits_hdu(
            previous_operation_active_networks.operation_name,
            'active_networks'
        )
        hmi_ic_mask = self._hmi_ic_file.get_fits_hdu(
            previous_operation_hmi_ic.operation_name)
        hmi_mag_image = file.get_fits_hdu(
            previous_operation_hmi_mag.operation_name)

        masked_image = hmi_mag_image.data.copy()

        total_mask = np.add(
            np.add(
                aia_mask_plages.data,
                aia_mask_active_networks.data
            ),
            hmi_ic_mask.data
        )

        total_mask[total_mask >= 1.0] = 1.0

        masked_image = apply_mask(masked_image, total_mask)

        self._aia_file.delete('mask', suffix='plages')
        self._aia_file.delete('mask', suffix='active_networks')
        self._hmi_ic_file.delete('mask')
        return masked_image, hmi_mag_image.header


class CreateCarringtonMap(Chain):

    def __init__(self, operation_name, aia_file, hmi_ic_file):
        super().__init__(operation_name)
        self._aia_file = aia_file
        self._hmi_ic_file = hmi_ic_file

    def actual_process(self, file=None, previous_operation_name=None):
        # The file is HMI Magnetogram masked image.
        # If the file exists, use it and extract the map information,
        # else create the file using the chaining.
        fits_array = file.get_fits_hdu(previous_operation_name)
        return fits_array.data, fits_array.header
