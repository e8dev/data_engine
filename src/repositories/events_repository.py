from pandas import DataFrame
import pandas.io.sql as psql

from ..db.engine import engine


def read_events(limit, offset) -> DataFrame:
    return psql.read_sql(f"SELECT * FROM events limit {limit} offset {offset} ", engine)


def read_events_optimized(limit) -> DataFrame:
    return psql.read_sql("SELECT * FROM events", engine, chunksize=1000)
        #print(f"Got dataframe w/{len(chunk_dataframe)} rows")
        
