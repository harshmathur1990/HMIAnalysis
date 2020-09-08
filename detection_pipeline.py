from pathlib import Path
from dateutil import parser
from model import Record
from utils import do_aiaprep
import matplotlib.pyplot as plt
import sunpy.io
import numpy as np
import imageio
from skimage.color import rgb2gray


def get_download_commands(date):
    if isinstance(date, str):
        date_object = parser.parse(date).date()
    else:
        date_object = date

    record = Record.find_by_date(
        date_object=date_object
    )

    commands = list()

    if not record:
        return commands

    remote_folder_name = date_object.strftime(
        '%Y.01.01'
    )

    local_foldername = date_object.strftime(
        '%Y-%m-%d'
    )

    commands.append(
        'mkdir -p {}'.format(
            local_foldername
        )
    )

    commands.append(
        'cd {}'.format(
            local_foldername
        )
    )

    # commands.append(
    #     'scp harsh@delphinus:/data/harsh1/data/{}/{} .'.format(
    #         remote_folder_name,
    #         record.hmi_ic_filename
    #     )
    # )

    # commands.append(
    #     'scp harsh@delphinus:/data/harsh1/HMIAnalysis/aiaprep/{}/{}.png aiaprep_{}.png'.format(
    #         remote_folder_name,
    #         record.hmi_ic_filename,
    #         record.hmi_ic_filename
    #     )
    # )

    # commands.append(
    #     'scp harsh@delphinus:/data/harsh1/HMIAnalysis/ldr/{}/{}.png .'.format(
    #         remote_folder_name,
    #         record.aia_filename
    #     )
    # )

    # commands.append(
    #     'scp harsh@delphinus:/data/harsh1/HMIAnalysis/mask/{}/plages_{}.png .'.format(
    #         remote_folder_name,
    #         record.aia_filename
    #     )
    # )

    # commands.append(
    #     'scp harsh@delphinus:/data/harsh1/HMIAnalysis/mask/{}/active_networks_{}.png .'.format(
    #         remote_folder_name,
    #         record.aia_filename
    #     )
    # )

    # commands.append(
    #     'scp harsh@delphinus:/data/harsh1/HMIAnalysis/mask/{}/{}.png .'.format(
    #         remote_folder_name,
    #         record.hmi_ic_filename
    #     )
    # )

    # commands.append(
    #     'scp harsh@delphinus:/data/harsh1/HMIAnalysis/crop_hmi_afterprep/{}/{}.png .'.format(
    #         remote_folder_name,
    #         record.hmi_filename
    #     )
    # )

    commands.append(
        'scp harsh@delphinus:/data/harsh1/HMIAnalysis/aligned_data/{}/{}.png aiaprep_{}.png'.format(
            remote_folder_name,
            record.aia_filename,
            record.aia_filename
        )
    )


    commands.append('cd ..')

    return commands


def make_download_script(filename, date_list):
    commands = list()

    for date in date_list:
        commands += get_download_commands(date)

    f = open(filename, 'w')

    f.write('\n'.join(str(line) for line in commands))

    f.close()


def make_six_images(directory, output):
    if isinstance(directory, str):
        directory = Path(directory)
    if isinstance(output, str):
        output = Path(output)

    all_files = directory.glob('**/*')

    all_files = [x for x in all_files]

    hmi_fits_file = [
        x for x in all_files
        if x.is_file() and x.name.endswith('.fits')
        and x.name.startswith('hmi.ic')
    ][0]

    data, header = sunpy.io.read_file(str(hmi_fits_file))[1]

    hmi_ic_aia_data, _ = do_aiaprep(data, header, radius_factor=0.96)

    hmi_ic_aia_data[np.isnan(hmi_ic_aia_data)] = 0.0
    hmi_ic_aia_data[np.isinf(hmi_ic_aia_data)] = 0.0

    # _data = hmi_ic_aia_data.copy()
    # _data -= np.nanmin(_data)
    # # _data[np.isnan(_data)] = 0.0
    # # _data[np.isinf(_data)] = 0.0
    # _data = _data / np.nanmax(_data)
    # _data = np.uint8(_data * 255)
    # aiaprepfilename = 'aiaprep_' + hmi_fits_file.name + '.png'
    # plt.imsave(
    #     directory / aiaprepfilename, _data, cmap='gray', format='png'
    # )
    # plt.clf()
    # plt.cla()
    # plt.close('all')

    sunspot_file = [
        x for x in all_files
        if x.is_file() and x.name.endswith('.png')
        and x.name.startswith('hmi.ic')
    ][0]

    plages_file = [
        x for x in all_files
        if x.is_file() and x.name.endswith('.png')
        and x.name.startswith('plages')
    ][0]

    active_networks_file = [
        x for x in all_files
        if x.is_file() and x.name.endswith('.png')
        and x.name.startswith('active')
    ][0]

    hmi_mag_file = [
        x for x in all_files
        if x.is_file() and x.name.endswith('.png')
        and x.name.startswith('hmi.m')
    ][0]

    aia_file = [
        x for x in all_files
        if x.is_file() and x.name.endswith('.png')
        and x.name.startswith('aia.lev1')
    ][0]

    sunspot_mask = rgb2gray(imageio.read(sunspot_file).get_data(0))
    plages_mask = rgb2gray(imageio.read(plages_file).get_data(0))
    active_networks_mask = rgb2gray(
        imageio.read(active_networks_file).get_data(0)
    )
    magnetogram = rgb2gray(imageio.read(hmi_mag_file).get_data(0))
    aia_data = rgb2gray(imageio.read(aia_file).get_data(0))

    X, Y = np.meshgrid(np.arange(4096), np.arange(4096))

    fig = plt.figure(figsize=(12, 12), dpi=450)
    ax1 = fig.add_subplot(111)
    ax1.contour(
        X, Y, sunspot_mask, 0, colors='blue', linewidths=0.03
    )
    ax1.imshow(hmi_ic_aia_data, origin='lower', cmap='gray')

    outd = output / directory.name

    outd.mkdir(exist_ok=True)

    out = outd / 'B.png'

    plt.savefig(out)

    plt.clf()

    plt.cla()

    plt.close('all')

    fig = plt.figure(figsize=(12, 12), dpi=450)
    ax1 = fig.add_subplot(111)
    ax1.contour(
        X, Y, plages_mask, 0, colors='red', linewidths=0.03
    )
    ax1.imshow(aia_data, origin='lower', cmap='gray')

    outd = output / directory.name

    outd.mkdir(exist_ok=True)

    out = outd / 'A.png'

    plt.savefig(out)

    plt.clf()

    plt.cla()

    plt.close('all')

    fig = plt.figure(figsize=(12, 12), dpi=450)
    ax1 = fig.add_subplot(111)
    ax1.imshow(magnetogram, origin='lower', cmap='gray')

    outd = output / directory.name

    outd.mkdir(exist_ok=True)

    out = outd / 'C.png'

    plt.savefig(out)

    plt.clf()

    plt.cla()

    plt.close('all')

    fig = plt.figure(figsize=(12, 12), dpi=450)
    ax1 = fig.add_subplot(111)
    ax1.contour(
        X, Y, plages_mask, 0, colors='red', linewidths=0.03
    )
    ax1.contour(
        X, Y, sunspot_mask, 0, colors='blue', linewidths=0.03
    )
    ax1.imshow(magnetogram, origin='lower', cmap='gray')

    outd = output / directory.name

    outd.mkdir(exist_ok=True)

    out = outd / 'D.png'

    plt.savefig(out)

    plt.clf()

    plt.cla()

    plt.close('all')

    fig = plt.figure(figsize=(12, 12), dpi=450)
    ax1 = fig.add_subplot(111)
    ax1.contour(
        X, Y, active_networks_mask, 0, colors='yellow', linewidths=0.03
    )
    ax1.imshow(aia_data, origin='lower', cmap='gray')

    outd = output / directory.name

    outd.mkdir(exist_ok=True)

    out = outd / 'E.png'

    plt.savefig(out)

    plt.clf()

    plt.cla()

    plt.close('all')

    fig = plt.figure(figsize=(12, 12), dpi=450)
    ax1 = fig.add_subplot(111)
    ax1.contour(
        X, Y, active_networks_mask, 0, colors='yellow', linewidths=0.03
    )
    ax1.contour(
        X, Y, plages_mask, 0, colors='red', linewidths=0.03
    )
    ax1.contour(
        X, Y, sunspot_mask, 0, colors='blue', linewidths=0.03
    )
    ax1.imshow(magnetogram, origin='lower', cmap='gray')

    outd = output / directory.name

    outd.mkdir(exist_ok=True)

    out = outd / 'F.png'

    plt.savefig(out)

    plt.clf()

    plt.cla()

    plt.close('all')


def do_all(directory, output):

    if isinstance(directory, str):
        directory = Path(directory)

    if isinstance(output, str):
        output = Path(output)

    output.mkdir(exist_ok=True)

    all_files = directory.glob('**/*')

    all_directories = [x for x in all_files if x.is_dir()]

    for subdir in all_directories:
        make_six_images(subdir, output)
