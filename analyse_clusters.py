import numpy as np
import h5py
import pandas as pd


# if __name__ == '__main__':
n = 10

f = h5py.File('out_{}.h5'.format(n), 'r')

data_frame = pd.read_hdf('../HMIResult.h5')
features = ['mmbf', 'mmapf', 'mmsf']
data = data_frame.loc[:, features].values

clusters = []
clusters_center = []
clusters_std = []
contribution = []
contribution_center = []
contribution_std = []
elements_in_one_std = []

for i in range(n):
    clusters.append(
        data[
            np.where(
                f['labels_'][()] == i
            )
        ]
    )
    clusters_center.append(
        np.mean(
            data[
                np.where(
                    f['labels_'][()] == i
                )
            ],
            0
        )
    )
    clusters_std.append(
        np.std(
            data[
                np.where(
                    f['labels_'][()] == i
                )
            ],
            0
        )
    )
    contribution.append(
        np.abs(
            data[
                np.where(
                    f['labels_'][()] == i
                )
            ]
        ) / np.sum(
            np.abs(
                data[
                    np.where(
                        f['labels_'][()] == i
                    )
                ]
            ),
            1
        )[:, np.newaxis]
    )
    contribution_center.append(
        np.mean(
            np.abs(
                data[
                    np.where(
                        f['labels_'][()] == i
                    )
                ]
            ) / np.sum(
                np.abs(
                    data[
                        np.where(
                            f['labels_'][()] == i
                        )
                    ]
                ),
                1
            )[:, np.newaxis],
            0
        )
    )
    contribution_std.append(
        np.std(
            np.abs(
                data[
                    np.where(
                        f['labels_'][()] == i
                    )
                ]
            ) / np.sum(
                np.abs(
                    data[
                        np.where(
                            f['labels_'][()] == i
                        )
                    ]
                ),
                1
            )[:, np.newaxis],
            0
        )
    )

for i in range(n):
    elements_in_one_std.append(
        np.where(
            (contribution[i][:, 0] >= (contribution_center[i][0] - contribution_std[i][0])) &
            (contribution[i][:, 0] <= (contribution_center[i][0] + contribution_std[i][0])) &
            (contribution[i][:, 1] >= (contribution_center[i][1] - contribution_std[i][1])) &
            (contribution[i][:, 1] <= (contribution_center[i][1] + contribution_std[i][1])) &
            (contribution[i][:, 2] >= (contribution_center[i][2] - contribution_std[i][2])) &
            (contribution[i][:, 2] <= (contribution_center[i][2] + contribution_std[i][2]))
        )[0]
    )
