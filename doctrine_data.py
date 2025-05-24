import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from logging_config import setup_logging
from models import DoctrineFit


setup_logging(log_name='doctrine_data')

mkt_db = "sqlite:///wcmkt.db"
doctrine_data = "/mnt/c/Users/User/PycharmProjects/eveESO/output/brazil/doctrine_fits.csv"
ship_targets = "/mnt/c/Users/User/PycharmProjects/eveESO/data/ship_targets2.csv"
lead_ships = "/mnt/c/Users/User/PycharmProjects/eveESO/output/brazil/lead_ships.csv"

def construct_doctrine_data():
    df = pd.read_csv(doctrine_data)
    return df

def preprocess_doctrine_fits():
    fits = construct_doctrine_data()
    targets = pd.read_csv(ship_targets)
    targets = targets.drop(columns=['fit_name','ship_name','created_at', 'ship_id'])
  
    combined = pd.merge(fits, targets, on='fit_id', how='left')
    combined = combined.rename(columns={'ship_target': 'target'}).reset_index(drop=True)
    combined = combined.drop(columns=['id'])
    combined = combined.dropna()
    
    return combined


if __name__ == "__main__":
    pass