from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import os

DATABASE_URL = os.environ.get('HEROKU_POSTGRESQL_COBALT_URL').replace('postgres', 'postgresql')
engine = create_engine(DATABASE_URL, echo=False)
session = Session(engine)