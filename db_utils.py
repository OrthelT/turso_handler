from sqlalchemy import create_engine, text
import libsql
import os
from dotenv import load_dotenv
from models import Base

load_dotenv()

turso_url = os.getenv("TURSO_URL")
turso_auth_token = os.getenv("TURSO_AUTH_TOKEN")
mock_url = os.getenv("MOCK_URL")
mock_auth_token = os.getenv("MOCK_AUTH_TOKEN")

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

def get_mock_libsql_connection():
    conn = libsql.connect("mock.db", sync_url=mock_url, auth_token=mock_auth_token)
    return conn

def get_mock_remote_engine():
    engine = create_engine(f"sqlite+{mock_url}?secure=true",
    connect_args={
        "auth_token": mock_auth_token,
    },echo_pool=False, echo=False)
    return engine

if __name__ == "__main__":
    pass