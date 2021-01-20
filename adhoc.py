import numpy as np


def prepare_indice_Extract(date_array):
    def get_indice(date):
        return np.where(date_array == date)
