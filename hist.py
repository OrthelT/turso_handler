import libsql_experimental as libsql
import pandas as pd
import os
from dotenv import load_dotenv
from pandas import read_sql_query
from sqlalchemy import create_engine, select, text, inspect, Table
from sqlalchemy.orm import Session, base, DeclarativeBase, sessionmaker
import sqlalchemy_libsql
import sqlalchemy.dialects.sqlite
from datetime import timedelta, time, datetime
import argparse
import sqlite3

import logging
from logging.handlers import RotatingFileHandler

from models import MarketStats, MarketOrders, Base, MarketHistory, Doctrines, ShipTargets

sde = libsql.connect("sde.db")
mkt = libsql.connect("wcmkt.db")

mkt_types = mkt.execute("SELECT DISTINCT type_id FROM market_history").fetchall()

type_list = [type[0] for type in mkt_types]

params = {
    "type_id": type_list
}

items = sde.execute("SELECT invTypes.typeID, invTypes.typeName, invGroups.groupName FROM invGroups JOIN invTypes ON invGroups.groupID = invTypes.groupID WHERE invGroups.categoryID = 6").fetchall()

df = pd.DataFrame(items, columns=["typeID", "typeName", "groupName"])

ships_history = mkt.execute("SELECT * FROM market_history WHERE type_id IN ({})".format(",".join(str(type[0]) for type in items))).fetchall()

df = pd.DataFrame(ships_history, columns=["id", "date", "type_name", "type_id", "average", "volume", "highest", "lowest", "order_count", "last_update"])

print(df.head())
df.to_csv("ships_history.csv", index=False)

