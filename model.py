from sqlalchemy import Column, Integer, Date, Float
from sqlalchemy.orm import sessionmaker
from utils import engine, Base


Session = sessionmaker(bind=engine)


class Record(Base):
    __tablename__ = 'record'

    id = Column(Integer, primary_key=True)

    date = Column(Date, unique=True)

    no_of_pixel_sunspot = Column(Integer)

    total_mag_field_sunspot = Column(Float)

    no_of_pixel_plage_and_active = Column(Integer)

    total_mag_field_plage_active = Column(Float)

    no_of_pixel_background = Column(Integer)

    total_background_field = Column(Float)

    total_pixels = Column(Integer)

    total_magnetic_field = Column(Float)

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

        return record_query.all()
