from sqlalchemy import *
from sqlalchemy.orm import *
import os

DATABASE_URL = os.environ.get('HEROKU_POSTGRESQL_COBALT_URL').replace('postgres', 'postgresql')
# setting for db
engine = create_engine(DATABASE_URL, echo=False)
session = sessionmaker(engine)