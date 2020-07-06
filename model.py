from sqlalchemy import Column, String, Boolean, Date, DateTime, ForeignKey, Float, Numeric
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class EarningsSchedule(Base):
    __tablename__ = 'earnings_schedule'
    id = Column(String, primary_key=True, nullable=False, unique=True)
    date = Column(DateTime)
    code = Column(String)
    name = Column(String)
    term = Column(String)
    segment = Column(String)
    pattern = Column(String)
    market = Column(String)