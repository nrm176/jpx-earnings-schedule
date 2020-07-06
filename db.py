from sqlalchemy import *
from sqlalchemy.orm import *

DATABASE = 'postgresql://@localhost:5432/kabu_db_local'

# setting for db
ENGINE = create_engine(DATABASE, encoding='utf-8', echo=False)

META = MetaData(bind=ENGINE)
META.reflect()

# create session
session = scoped_session(
    # Setting for ORM, auto commit is set to false. Do session.commit to commit
    sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)
)

# Base.query = session.query_property()
# Base.metadata.create_all(ENGINE)