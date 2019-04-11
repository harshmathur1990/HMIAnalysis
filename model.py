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

    no_of_pixel_plage = Column(Integer)

    total_mag_field_plage = Column(Float)

    no_of_pixel_active = Column(Integer)

    total_mag_field_active = Column(Float)

    no_of_pixel_background = Column(Integer)

    total_background_field = Column(Float)

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
