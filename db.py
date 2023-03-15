from sqlalchemy import *
from sqlalchemy.orm import *
import os

DATABASE_URL = os.environ.get('HEROKU_POSTGRESQL_COBALT').replace('postgres', 'postgresql')
# setting for db
ENGINE = create_engine(DATABASE_URL, encoding='utf-8', echo=False)

META = MetaData(bind=ENGINE)
META.reflect()

# create session
session = scoped_session(
    # Setting for ORM, auto commit is set to false. Do session.commit to commit
    sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)
)