import datetime
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta


def plot_magnetic_field(
    title,
    date_list,
    mmf_list,
    mmsf_list,
    mmbf_list,
    mmapf_list
):

    date_list = list(date_list)

    mmf_list = list(mmf_list)

    mmsf_list = list(mmsf_list)

    mmbf_list = list(mmbf_list)

    mmapf_list = list(mmapf_list)

    fig, axs = plt.subplots(4)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    x_ticks = list()

    _start_date = datetime.date(2011, 1, 1)

    for i in range(0, 9):
        x_ticks.append(
            _start_date.replace(year=_start_date.year + i)
        )

    plt.xticks(x_ticks)

    plt.gcf().autofmt_xdate()

    l1, = axs[0].plot(date_list, mmbf_list)

    l2, = axs[1].plot(date_list, mmapf_list)

    l3, = axs[2].plot(date_list, mmsf_list)

    l4, = axs[3].plot(date_list, mmf_list)

    l1.set_label('Background')

    axs[0].legend()

    l2.set_label('Plage, Enhanced and Active Networks')

    axs[1].legend()

    l3.set_label('Sunspots')

    axs[2].legend()

    l4.set_label('Mean Field')

    axs[3].legend()

    # fig.legend((l1,), ('Background'))

    # fig.legend((l2,), ('Plage, Enhanced and Active Networks'))

    # fig.legend((l3,), ('Sunspots'))

    # fig.legend((l4,), ('Mean Field'))

    fig.suptitle('{}'.format(title))

    fig.tight_layout()

    # plt.legend()

    # plt.tight_layout()

    plt.show()

    # fig1.savefig(
    #     '{} Intensity Variation.png'.format(beam_name),
    #     dpi=300,
    #     format='png'
    # )


def plot_intensity(
    title,
    date_list,
    mean_intensity_aia_list,
    mean_intensity_active_networks,
    mean_intensity_plage_en,
    mean_intensity_sunspots,
    mean_intensity_background
):

    plt.close('all')

    plt.clf()

    plt.cla()

    date_list = list(date_list)

    mean_intensity_aia_list = list(mean_intensity_aia_list)

    mean_intensity_active_networks = list(mean_intensity_active_networks)

    mean_intensity_plage_en = list(mean_intensity_plage_en)

    mean_intensity_sunspots = list(mean_intensity_sunspots)

    fig, axs = plt.subplots(5)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    x_ticks = list()

    _start_date = datetime.date(2013, 12, 31)

    for i in range(0, 16):
        x_ticks.append(
            _start_date + timedelta(days=i)
        )

    plt.xticks(x_ticks)

    plt.gcf().autofmt_xdate()

    l1, = axs[0].plot(date_list, mean_intensity_aia_list, linewidth=0.5)

    l2, = axs[1].plot(date_list, mean_intensity_active_networks, linewidth=0.5)

    l3, = axs[2].plot(date_list, mean_intensity_plage_en, linewidth=0.5)

    l4, = axs[3].plot(date_list, mean_intensity_sunspots, linewidth=0.5)

    l5, = axs[4].plot(date_list, mean_intensity_background, linewidth=0.5)

    l1.set_label('Intensity AIA divided by total pixels')

    axs[0].legend()

    l2.set_label('Intensity Active Networks divided by total pixels')

    axs[1].legend()

    l3.set_label('Intensity Plage and Enhanced Networks divided by total pixels')

    axs[2].legend()

    l4.set_label('Intensity Sunspots divided by total pixels')

    axs[3].legend()

    l5.set_label('Intensity Background divided by total pixels')

    axs[4].legend()

    # fig.legend((l1,), ('Background'))

    # fig.legend((l2,), ('Plage, Enhanced and Active Networks'))

    # fig.legend((l3,), ('Sunspots'))

    # fig.legend((l4,), ('Mean Field'))

    fig.suptitle('{}'.format(title))

    fig.tight_layout()

    # plt.legend()

    # plt.tight_layout()

    # plt.show()
    plt.savefig(
        'fifteen_days_fifteen_minute_cadence_separate_features.png',
        format='png',
        dpi=1200
    )


def plot_intensity_add_sunspot_with_plages(
    title,
    date_list,
    mean_intensity_aia_list,
    mean_intensity_active_networks,
    mean_intensity_plage_en,
    mean_intensity_sunspots,
    mean_intensity_background
):

    plt.close('all')

    plt.clf()

    plt.cla()

    date_list = list(date_list)

    mean_intensity_aia_list = list(mean_intensity_aia_list)

    mean_intensity_active_networks = list(mean_intensity_active_networks)

    mean_intensity_plage_en = np.array(
        list(mean_intensity_plage_en)
    )

    mean_intensity_sunspots = np.array(
        list(mean_intensity_sunspots)
    )

    mean_intensity_sunspots_plage_en = np.add(
        mean_intensity_plage_en,
        mean_intensity_sunspots
    )

    fig, axs = plt.subplots(4)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    x_ticks = list()

    _start_date = datetime.date(2013, 12, 31)

    for i in range(0, 16):
        x_ticks.append(
            _start_date + timedelta(days=i)
        )

    plt.xticks(x_ticks)

    plt.gcf().autofmt_xdate()

    l1, = axs[0].plot(date_list, mean_intensity_aia_list, linewidth=0.5)

    l2, = axs[1].plot(date_list, mean_intensity_active_networks, linewidth=0.5)

    l3, = axs[2].plot(
        date_list, mean_intensity_sunspots_plage_en, linewidth=0.5
    )

    l4, = axs[3].plot(date_list, mean_intensity_background, linewidth=0.5)

    l1.set_label('Intensity AIA divided by total pixels')

    axs[0].legend()

    l2.set_label('Intensity Active Networks divided by total pixels')

    axs[1].legend()

    l3.set_label(
        'Intensity Sunspots and Plage and Enhanced Networks divided by total pixels'
    )

    axs[2].legend()

    l4.set_label('Intensity background divided by total pixels')

    axs[3].legend()

    fig.suptitle('{}'.format(title))

    fig.tight_layout()

    # plt.legend()

    # plt.tight_layout()

    # plt.show()

    plt.savefig(
        'fifteen_days_fifteen_minute_cadence_sunspot_plage_en_merge.png',
        format='png',
        dpi=1200
    )



def plot_intensity_add_sunspot_with_plages(
    title,
    date_list,
    rms_magnetic,
    rms_quiet_networks,
):

    plt.close('all')

    plt.clf()

    plt.cla()

    date_list = list(date_list)

    rms_add_quiet_magnetic = np.add(rms_magnetic, rms_quiet_networks)

    rms_quiet_contribution = np.divide(rms_quiet_networks, rms_add_quiet_magnetic)

    fig, axs = plt.subplots(3)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    x_ticks = list()

    _start_date = datetime.date(2013, 12, 31)

    for i in range(0, 31, 2):
        x_ticks.append(
            _start_date + timedelta(days=i)
        )

    plt.xticks(x_ticks)

    plt.gcf().autofmt_xdate()

    l1, = axs[0].plot(date_list, rms_magnetic, linewidth=0.5)

    l2, = axs[1].plot(date_list, rms_quiet_networks, linewidth=0.5)

    l3, = axs[2].plot(
        date_list, rms_quiet_contribution, linewidth=0.5
    )

    l1.set_label('RMS Magnetic Intensity (Im)')

    axs[0].legend()

    l2.set_label('RMS Quiet Network Intensity (Iq)')

    axs[1].legend()

    l3.set_label(
        'Iq / (Iq + Im)'
    )

    axs[2].legend()

    fig.suptitle('{}'.format(title))

    fig.tight_layout()

    # plt.legend()

    # plt.tight_layout()

    # plt.show()

    plt.savefig(
        'thirty_days_fifteen_minute_cadence_sunspot_plage_en_merge.png',
        format='png',
        dpi=1200
    )


def plot_PCA(
    data_frame,
    principalComponents,
    pca
):

    fig, axs = plt.subplots(4)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

    plt.gca().xaxis.set_major_locator(mdates.DayLocator())

    x_ticks = list()

    _start_date = datetime.date(2011, 1, 1)

    for i in range(0, 9):
        x_ticks.append(
            _start_date.replace(year=_start_date.year + i)
        )

    plt.xticks(x_ticks)

    plt.gcf().autofmt_xdate()

    l1, = axs[0].plot(
        data_frame['date'],
        principalComponents.T[0]
    )

    l2, = axs[1].plot(
        data_frame['date'],
        principalComponents.T[1]
    )

    l3, = axs[2].plot(
        data_frame['date'],
        principalComponents.T[2]
    )

    l4, = axs[3].plot(
        data_frame['date'],
        data_frame['mmf']
    )

    l1.set_label(
        'PC1 - Explained Variance Ratio: {}'.format(
            pca.explained_variance_ratio_[0]
        )
    )

    axs[0].legend()

    l2.set_label(
        'PC2 - Explained Variance Ratio: {}'.format(
            pca.explained_variance_ratio_[1]
        )
    )

    axs[1].legend()

    l3.set_label(
        'PC3 - Explained Variance Ratio: {}'.format(
            pca.explained_variance_ratio_[2]
        )
    )

    axs[2].legend()

    l4.set_label('Mean Field')

    axs[3].legend()

    # fig.legend((l1,), ('Background'))

    # fig.legend((l2,), ('Plage, Enhanced and Active Networks'))

    # fig.legend((l3,), ('Sunspots'))

    # fig.legend((l4,), ('Mean Field'))

    fig.suptitle('Mean Field vs Principal Components')

    # plt.title('Mean Field vs Principal Components')

    fig.tight_layout()

    plt.legend()

    plt.tight_layout()

    plt.show()

    # fig1.savefig(
    #     '{} Intensity Variation.png'.format(beam_name),
    #     dpi=300,
    #     format='png'
    # )
