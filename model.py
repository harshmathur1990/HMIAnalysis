from sqlalchemy import Column, Integer, Date, Float, String
from sqlalchemy.orm import sessionmaker
from utils import engine, Base


Session = sessionmaker(bind=engine)


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

    # mmf = Column(Float)

    # mmbf = Column(Float)

    # mmsf = Column(Float)

    # mmapf = Column(Float)

    # verify_mmf = Column(Float)

    def save(self):

        session = Session()

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

        return record_query.order_by(Record.date).all()
