import sqlite3
import shutil


filenames = [
    (2011, 1, 1, 2, 0),
    (2011, 1, 1, 2, 1),
    (2012, 1, 1, 2, 0),
    (2012, 1, 1, 2, 1),
    (2013, 1, 1, 2, 0),
    (2013, 1, 1, 2, 1),
    (2014, 1, 1, 2, 0),
    (2014, 1, 1, 2, 1),
    (2015, 1, 1, 2, 0),
    (2015, 1, 1, 2, 1),
    (2016, 1, 1, 2, 0),
    (2016, 1, 1, 2, 1),
    (2017, 1, 1, 2, 0),
    (2017, 1, 1, 2, 1),
    (2018, 1, 1, 2, 0),
    (2018, 1, 1, 2, 1),
    (2019, 1, 1, 2, 0),
    (2019, 1, 1, 2, 1)
]


def generate_combined_file():

    shutil.copyfile(
        'hmi_{}_{}_{}_{}_{}.db'.format(
            *filenames[0]
        ),
        'hmi.db'
    )

    con3 = sqlite3.connect('hmi.db')

    for index, argument in enumerate(filenames):
        if index == 0:
            continue

        print (
            "ATTACH 'hmi_{}_{}_{}_{}_{}.db' as dba".format(
                *argument
            )
        )

        con3.execute(
            "ATTACH 'hmi_{}_{}_{}_{}_{}.db' as dba".format(
                *argument
            )
        )

        con3.execute("BEGIN")

        combine = "INSERT INTO record (date, hmi_filename, hmi_ic_filename, aia_filename, time_difference, no_of_pixel_sunspot, total_mag_field_sunspot, no_of_pixel_plage_and_active, total_mag_field_plage_active, no_of_pixel_background, total_background_field, total_pixels, total_magnetic_field) SELECT date, hmi_filename, hmi_ic_filename, aia_filename, time_difference, no_of_pixel_sunspot, total_mag_field_sunspot, no_of_pixel_plage_and_active, total_mag_field_plage_active, no_of_pixel_background, total_background_field, total_pixels, total_magnetic_field FROM dba.record"
        print(combine)
        con3.execute(combine)

        con3.commit()
        con3.execute("detach database dba")


if __name__ == '__main__':
    generate_combined_file()
