import sqlalchemy  as db
from sqlalchemy import create_engine 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import insert
from sqlalchemy import table,Column,Integer,String
from sqlalchemy import MetaData

base = declarative_base()

USER = "root"
PASSWORD = "root"
HOST= "localhost"
PORT =3306
DATABASE = "test"

url =f"mysql+pymysql://%s:%s@%s:%s/%s" % (USER, PASSWORD, HOST, PORT, DATABASE)

engine = create_engine(url)

base.metadata.create_all(engine)
session =  sessionmaker(bind=engine)
metadata = MetaData()
metadata.reflect(engine)








class users(base):

    __tablename__ = 'users'

    id = Column(Integer, primary_key = True)
    name = Column(String(36), nullable=False, unique=True)
    surname = Column(String(500), nullable=False, unique=True)
    address = Column(String(500), nullable=False, unique=True)



