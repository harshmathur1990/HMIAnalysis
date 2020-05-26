from sqlalchemy import Column, Integer, Date, Float, String
from sqlalchemy.orm import sessionmaker
from utils import engine, Base
import numpy as np
import pandas as pd


Session = sessionmaker(bind=engine)
session = Session()


class Record(Base):
    __tablename__ = 'record'

    id = Column(Integer, primary_key=True)

    date = Column(Date, unique=True)  # Must be from HMI Filename

    hmi_filename = Column(String)

    hmi_ic_filename = Column(String)

    aia_filename = Column(String)

    time_difference = Column(Float)  # in Julian Days

    no_of_pixel_sunspot = Column(Integer)

    total_mag_field_sunspot = Column(Float)

    no_of_pixel_plage_and_active = Column(Integer)

    total_mag_field_plage_active = Column(Float)

    no_of_pixel_background = Column(Integer)

    total_background_field = Column(Float)

    total_pixels = Column(Integer)

    total_magnetic_field = Column(Float)

    mmf = Column(Float)

    mmbf = Column(Float)

    mmsf = Column(Float)

    mmapf = Column(Float)

    verify_mmf = Column(Float)

    julday = Column(Float)

    def save(self):

        if not self.id:

            session.add(self)

        session.commit()

    @staticmethod
    def find_by_date(date_object):

        session = Session()

        record_query = session.query(Record)\
            .filter(Record.date == date_object)

        return record_query.one_or_none()

    @staticmethod
    def get_all(date_object_lower=None, date_object_upper=None):
        session = Session()

        record_query = session.query(Record)

        if date_object_lower:
            record_query = record_query.filter(
                Record.date >= date_object_lower
            )

        if date_object_upper:
            record_query = record_query.filter(
                Record.date < date_object_upper
            )

        return record_query.order_by(Record.date).order_by(Record.julday).all()

    @staticmethod
    def get_pandas_data_frame(date_object_lower=None, date_object_upper=None):
        data_sql_object_list = Record.get_all(
            date_object_lower=date_object_lower,
            date_object_upper=date_object_upper
        )

        date_list = list()

        hmi_filename_list = list()

        hmi_ic_filename_list = list()

        aia_filename_list = list()

        time_difference_list = list()

        no_of_pixel_sunspot_list = list()

        total_mag_field_sunspot_list = list()

        no_of_pixel_plage_and_active_list = list()

        total_mag_field_plage_active_list = list()

        no_of_pixel_background_list = list()

        total_background_field_list = list()

        total_pixels_list = list()

        total_magnetic_field_list = list()

        julday_list = list()

        for a_record in data_sql_object_list:
            date_list.append(a_record.date)
            hmi_filename_list.append(a_record.hmi_filename)
            hmi_ic_filename_list.append(a_record.hmi_ic_filename)
            aia_filename_list.append(a_record.aia_filename)
            time_difference_list.append(a_record.time_difference)
            no_of_pixel_sunspot_list.append(a_record.no_of_pixel_sunspot)
            total_mag_field_sunspot_list.append(
                a_record.total_mag_field_sunspot
            )
            no_of_pixel_plage_and_active_list.append(
                a_record.no_of_pixel_plage_and_active
            )
            total_mag_field_plage_active_list.append(
                a_record.total_mag_field_plage_active
            )
            no_of_pixel_background_list.append(
                a_record.no_of_pixel_background
            )
            total_background_field_list.append(
                a_record.total_background_field
            )
            total_pixels_list.append(
                a_record.total_pixels
            )
            total_magnetic_field_list.append(
                a_record.total_magnetic_field
            )
            julday_list.append(
                a_record.julday
            )

        data_frame = pd.DataFrame(
            {
                'date': np.array(date_list),
                'hmi_filename': np.array(hmi_filename_list),
                'hmi_ic_filename': np.array(hmi_ic_filename_list),
                'aia_filename': np.array(aia_filename_list),
                'time_difference': np.array(time_difference_list),
                'no_of_pixel_sunspot': np.array(no_of_pixel_sunspot_list),
                'total_mag_field_sunspot': np.array(
                    total_mag_field_sunspot_list
                ),
                'no_of_pixel_plage_and_active': np.array(
                    no_of_pixel_plage_and_active_list
                ),
                'total_mag_field_plage_active': np.array(
                    total_mag_field_plage_active_list
                ),
                'no_of_pixel_background': np.array(
                    no_of_pixel_background_list
                ),
                'total_background_field': np.array(
                    total_background_field_list
                ),
                'total_pixels': np.array(
                    total_pixels_list
                ),
                'total_magnetic_field': np.array(
                    total_magnetic_field_list
                ),
                'julday': np.array(julday_list)
            }
        )

        data_frame['mmf'] = data_frame['total_magnetic_field'] \
            / data_frame['total_pixels']

        data_frame['mmbf'] = data_frame['total_background_field'] \
            / data_frame['total_pixels']

        data_frame['mmsf'] = data_frame['total_mag_field_sunspot'] \
            / data_frame['total_pixels']

        data_frame['mmapf'] = data_frame['total_mag_field_plage_active'] \
            / data_frame['total_pixels']

        return data_frame
