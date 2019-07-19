# -*- coding: utf-8 -*-
from abc import abstractmethod, ABC
# from concurrent.futures import ProcessPoolExecutor
# from user_pools import NoDaemonPool as Pool
import numpy as np
import sys
import operator
from skimage.draw import circle
from skimage.measure import label, regionprops
from skimage.morphology import closing, square
from model import Record
from utils import apply_mask, do_thresholding, \
    do_limb_darkening_correction, do_aiaprep


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
        value_2=0.0,
        do_closing=False
    ):
        super().__init__(operation_name, suffix)
        self._k = k
        self._op = op
        self._k2 = k2
        self._op2 = op2
        self._post_processor = post_processor
        self._radius_factor = radius_factor
        self._value_1 = value_1
        self._value_2 = value_2,
        self._do_closing = do_closing

    @property
    def do_closing(self):
        return self._do_closing

    def _do_thresholding(self, image, header):

        return do_thresholding(
            image=image,
            header=header,
            k=self._k,
            op=self._op,
            value_1=self._value_1,
            radius_factor=self._radius_factor,
            k2=self._k2,
            op2=self._op2,
            value_2=self._value_2,
            # do_closing=self.do_closing
        )

    def actual_process(self, file=None, previous_operation_name=None):

        fits_array = file.get_fits_hdu(previous_operation_name)

        image, invalid_result = self._do_thresholding(
            fits_array.data, fits_array.header
        )

        if invalid_result:
            sys.stdout.write(
                'Invalid LDR Result for Filename: {}'.format(file.filename)
            )

        if self._post_processor:
            image = self._post_processor(image)

        # 1.8 sec per call, 4% of the program
        if self.do_closing:
            image = closing(image, square(3))

        return image, fits_array.header


class LimbDarkeningCorrection(Chain):

    def __init__(self, operation_name, radius_factor=None):
        super().__init__(operation_name)
        self._radius_factor = radius_factor

    def _do_limb_darkening_correction(self, image, header):

        return do_limb_darkening_correction(
            image, header, radius_factor=self._radius_factor
        )

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

        return do_aiaprep(
            data,
            header,
            radius_factor=self._radius_factor
        )

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

    def __init__(self, operation_name, aia_file, hmi_ic_file, date_object):
        super().__init__(operation_name)
        self._aia_file = aia_file
        self._hmi_ic_file = hmi_ic_file
        self._date_object = date_object

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
            radius_factor=0.96,
            do_closing=True
        ).set_prev(
            LimbDarkeningCorrection(
                operation_name='ldr',
                radius_factor=0.96
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
            radius_factor=0.96,
            value_1=1.0,
            value_2=0.0,
            do_closing=False
        ).set_prev(
            LimbDarkeningCorrection(
                operation_name='ldr',
                radius_factor=0.96
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
            k=0.4,
            op=operator.le,
            radius_factor=0.96,
            do_closing=True
        ).set_prev(
            AIAPrep(operation_name='aiaprep', radius_factor=1.0).set_prev(
                DownloadFiles(operation_name='data')
            )
        )

        hmi_mag_chain = AIAPrep(
            operation_name='aiaprep',
            radius_factor=0.96
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

        sys.stdout.write(
            'Performed all the chains for : {}\n'.format(
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

        no_of_pixels_total_field = len(
            circle(2048.5, 2048.5, hmi_mag_image.header['R_SUN'] * 0.96)[0]
        )

        # apply mask is masking the features in the mask and
        # returning the non masked elements
        masked_image = apply_mask(masked_image, total_mask)

        background_field = np.nansum(masked_image)

        no_of_background_field = no_of_pixels_total_field - np.nansum(
            total_mask
        )

        total_magnetic_field = np.nansum(hmi_mag_image.data)

        def get_no_of_pixel_and_field(mask, image):

            mask = mask.copy()
            image = image.copy()
            no_of_pixels = np.nansum(mask)

            ulta_mask = -1 * (mask - 1)

            _masked_image = apply_mask(image, ulta_mask)

            return no_of_pixels, np.nansum(_masked_image)

        mask_active_network_plage = np.add(
            aia_mask_plages.data,
            aia_mask_active_networks.data
        )

        mask_active_network_plage[mask_active_network_plage >= 1.0] = 1.0

        a, b = get_no_of_pixel_and_field(
            mask_active_network_plage, hmi_mag_image.data
        )

        no_of_pixel_plage_and_active, total_mag_field_plage_active = a, b

        no_of_sunspot_pixel, sunspot_field = get_no_of_pixel_and_field(
            hmi_ic_mask.data, hmi_mag_image.data
        )

        record = Record(
            date=self._date_object,
            no_of_pixel_sunspot=no_of_sunspot_pixel,
            total_mag_field_sunspot=sunspot_field,
            no_of_pixel_plage_and_active=no_of_pixel_plage_and_active,
            total_mag_field_plage_active=total_mag_field_plage_active,
            no_of_pixel_background=no_of_background_field,
            total_background_field=background_field,
            total_pixels=no_of_pixels_total_field,
            total_magnetic_field=total_magnetic_field
        )

        record.save()

        self._aia_file.delete('mask', suffix='plages')
        self._aia_file.delete('mask', suffix='active_networks')
        self._hmi_ic_file.delete('mask')
        return masked_image, hmi_mag_image.header
