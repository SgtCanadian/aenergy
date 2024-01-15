#!/usr/bin/env python3


import requests
import json
import psycopg2
from sqlalchemy import DateTime, Integer, String, ForeignKey
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone
import configparser
import os

config = configparser.ConfigParser()
if os.path.isfile('./settings.ini'):
    config.read('settings.ini')
    
if not config.has_section('./settings'):
    config.add_section('settings')

api_key = config.get('settings','apikey',fallback=os.getenv('AENERGY_API_KEY',''))
db_url = config.get('settings','DATABASE_URL',fallback=os.getenv('AENERGY_DB_URL',''))



response1 = requests.get(
    "https://api.aeso.ca/report/v1/csd/summary/current",
    headers={
        "X-API-Key": api_key
    },
)


summarycurrent = response1.json()

response2 = requests.get(
    "https://api.aeso.ca/report/v1/csd/generation/assets/current",
    headers={
        "X-API-Key": api_key
    },
)

assetscurrent = response2.json()
engine = create_engine(db_url)
Session = sessionmaker(engine)



# declarative base class
class Base(DeclarativeBase):
    pass


# table for the summary
class Summary(Base):
    __tablename__ = "summary"
    summary_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rowtime: Mapped[datetime]
    last_updated_datetime_utc: Mapped[datetime]
    total_max_generation_capability: Mapped[int]
    total_net_generation: Mapped[int]
    net_to_grid_generation: Mapped[int]
    net_actual_interchange: Mapped[int]
    alberta_internal_load: Mapped[int]
    contingency_reserve_required: Mapped[int]
    dispatched_contigency_reserve_total: Mapped[int]
    dispatched_contingency_reserve_gen: Mapped[int]
    dispatched_contingency_reserve_other: Mapped[int]
    lssi_armed_dispatch: Mapped[int]
    lssi_offered_volume: Mapped[int]


class Genlist(Base):
    __tablename__ = "genlist"
    genlist_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rowtime: Mapped[datetime]
    last_updated_datetime_utc: Mapped[datetime]
    fuel_type: Mapped[str]
    aggregated_maximum_capability: Mapped[int]
    aggregated_net_generation: Mapped[int]
    aggregated_dispatched_contingency_reserve: Mapped[int]


class Interchange(Base):
    __tablename__ = "interchange"
    interchange_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rowtime: Mapped[datetime]
    last_updated_datetime_utc: Mapped[datetime]
    location: Mapped[str]
    actual_flow: Mapped[int]
    
class Assets(Base):
    __tablename__ = "assets"
    assets_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rowtime: Mapped[datetime]
    last_updated_datetime_utc: Mapped[datetime]
    asset: Mapped[str]
    fuel_type: Mapped[str]
    sub_fuel_type: Mapped[str]
    maximum_capability: Mapped[int]
    net_generation: Mapped[int]
    dispatched_contingency_reserve: Mapped[int]

Summary.__table__.create(bind=engine, checkfirst=True)
Genlist.__table__.create(bind=engine, checkfirst=True)  
Interchange.__table__.create(bind=engine, checkfirst=True)
Assets.__table__.create(bind=engine, checkfirst=True)

run_time = datetime.utcnow()   
last_updated_utc_summary = datetime.fromisoformat(summarycurrent['return']['last_updated_datetime_utc'])
last_updated_utc_assets = datetime.fromisoformat(assetscurrent['return']['last_updated_datetime_utc'])


summary = Summary()
summary.rowtime = run_time
summary.last_updated_datetime_utc = last_updated_utc_summary
summary.total_max_generation_capability = summarycurrent['return']['total_max_generation_capability']
summary.total_net_generation = summarycurrent['return']['total_net_generation']
summary.net_to_grid_generation = summarycurrent['return']['net_to_grid_generation']
summary.net_actual_interchange = summarycurrent['return']['net_actual_interchange']
summary.alberta_internal_load = summarycurrent['return']['alberta_internal_load']
summary.contingency_reserve_required = summarycurrent['return']['contingency_reserve_required']
summary.dispatched_contigency_reserve_total = summarycurrent['return']['dispatched_contigency_reserve_total']
summary.dispatched_contingency_reserve_gen = summarycurrent['return']['dispatched_contingency_reserve_gen']
summary.dispatched_contingency_reserve_other = summarycurrent['return']['dispatched_contingency_reserve_other']
summary.lssi_armed_dispatch = summarycurrent['return']['lssi_armed_dispatch']
summary.lssi_offered_volume = summarycurrent['return']['lssi_offered_volume']

for i in summarycurrent['return']['generation_data_list']:
    fuel = Genlist()
    fuel.fuel_type = i['fuel_type']
    fuel.aggregated_net_generation = i['aggregated_net_generation']
    fuel.aggregated_maximum_capability = i['aggregated_maximum_capability']
    fuel.aggregated_dispatched_contingency_reserve = i['aggregated_dispatched_contingency_reserve']
    fuel.rowtime = run_time
    fuel.last_updated_datetime_utc = last_updated_utc_summary
    with Session() as session:
        session.begin()
    try:
        session.add(fuel)
    except:
        session.rollback()
        raise
    else:
        session.commit()

for i in summarycurrent['return']['interchange_list']:
    interchange = Interchange()
    interchange.location = i['path']
    interchange.actual_flow = i['actual_flow']
    interchange.rowtime = run_time
    interchange.last_updated_datetime_utc = last_updated_utc_summary
    with Session() as session:
        session.begin()
    try:
        session.add(interchange)
    except:
        session.rollback()
        raise
    else:
        session.commit()


with Session() as session:
        session.begin()
try:
        session.add(summary)

except:
        session.rollback()
        raise
else:
        session.commit()

for i in assetscurrent['return']['asset_list']:
    assets = Assets()
    assets.asset = i['asset']
    assets.fuel_type = i['fuel_type']
    assets.sub_fuel_type = i['sub_fuel_type']
    assets.maximum_capability = i['maximum_capability']
    assets.net_generation = i['net_generation']
    assets.dispatched_contingency_reserve = i['dispatched_contingency_reserve']
    assets.rowtime = run_time
    assets.last_updated_datetime_utc = last_updated_utc_assets
    with Session() as session:
        session.begin()
    try:
        session.add(assets)
    except:
        session.rollback()
        raise
    else:
        session.commit()