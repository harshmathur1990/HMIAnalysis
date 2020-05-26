import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


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

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

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
