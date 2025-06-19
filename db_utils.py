from sqlalchemy import create_engine, text
import libsql
import os
from dotenv import load_dotenv


load_dotenv()

turso_url = os.getenv("TURSO_URL")
turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")

def get_wcmkt_local_engine():
    engine = create_engine("sqlite+libsql:///wcmkt.db")
    return engine

def get_wcmkt_remote_engine():
    engine = create_engine(
    f"sqlite+{turso_url}?secure=true",
    connect_args={
        "auth_token": turso_auth_token,
    },echo_pool=False, echo=False)
    return engine

def get_wcmkt_libsql_connection():
    conn = libsql.connect("wcmkt.db", sync_url=turso_url, auth_token=turso_auth_token)
    return conn

if __name__ == "__main__":
    pass