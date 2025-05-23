import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from logging_config import setup_logging
from turso_handler import ship_targets

setup_logging(log_name='doctrine_data')

mkt_db = "sqlite:///wcmkt.db"
doctrine_data = "/mnt/c/Users/User/PycharmProjects/eveESO/output/brazil/doctrine_fits.csv"

def construct_doctrine_data():
    df = pd.read_csv(doctrine_data)
    return df

def convert_to_sqlalchemy_model():
    """
    Demonstrates converting the DataFrame to a SQLAlchemy model
    """
    from doctrine_model import dataframe_to_database
    
    # Get the DataFrame
    df = construct_doctrine_data()
    
    # Convert to SQLAlchemy model and save to database
    engine = dataframe_to_database(df)
    
    # Query and display results to confirm
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM doctrine_fits LIMIT 5"))
        for row in result:
            print(row)
    
    return "Database created and populated successfully"

def update_local_doctrine_fits():
    engine = create_engine(mkt_db)
    df = construct_doctrine_data()
    df2 = pd.read_csv(ship_targets)
    with engine.connect() as conn:
        delete_query = text("DELETE FROM doctrine_fits")
        conn.execute(delete_query)
        df.to_sql("doctrine_fits", conn, if_exists="replace", index=False)
        df2.to_sql("ship_targets", conn, if_exists="replace", index=False)
        conn.commit()
        fits = pd.read_sql_table("doctrine_fits", conn)
        targets = pd.read_sql_table("ship_targets", conn)
        targets = targets.drop(columns=['fit_name','ship_name','created_at'])

        combined = pd.merge(fits, targets, on='fit_id', how='left')
        delete_query = text("DELETE FROM doctrine_fits")
        conn.execute(delete_query)
        combined.to_sql("doctrine_fits", conn, if_exists="replace", index=False)
        conn.commit()
  
    return combined

def preprocess_doctrine_fits():
    engine = create_engine(mkt_db)
    fits = construct_doctrine_data()
    targets = pd.read_csv(ship_targets)
    targets = targets.drop(columns=['fit_name','ship_name','created_at'])
    combined = pd.merge(fits, targets, on='fit_id', how='left')
    return combined 
  

if __name__ == "__main__":
    pass
    
    # Uncomment the line below to convert to SQLAlchemy model
    # result = convert_to_sqlalchemy_model()
    # print(result)








