from sqlalchemy import *
from sqlalchemy.orm import *
import os

DATABASE = os.environ.get('DATABASE_URL')

# setting for db
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=False)

META = MetaData(bind=ENGINE)
META.reflect()

# create session
session = scoped_session(
    # Setting for ORM, auto commit is set to false. Do session.commit to commit
    sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)
)