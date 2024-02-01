#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
import os
from time import time
from sqlalchemy import create_engine


def main(params):
    host = params.host  # "localhost"
    port = params.port  # "5432"
    usr = params.user  # "root"
    pwd = params.password  # "root"
    db = params.database  # "ny_taxi"
    url = params.url  # "./green_tripdata_2019-09.csv.gz"
    table_name = params.table
    file_name = "data.CSV"

    os.system(f"wget {url} -O {file_name}")

    connection_string = f"postgresql://{usr}:{pwd}@{host}:{port}/{db}"
    engine = create_engine(connection_string)
    connection = engine.connect()

    df_iterator = pd.read_csv(file_name, compression="gzip", chunksize=50_000)

    df = next(df_iterator)
    for col in df.columns:
        if "datetime" in col.lower():
            df[col] = pd.to_datetime(df[col])

    t_start = time()

    df.to_sql(name=table_name, con=engine, if_exists="replace")

    t_end = time()

    print(
        f"insert #1 chunk into table {table_name}, took {(t_end - t_start):.3f} seconds, {df.shape[0]:,} inserted."
    )

    for i, chunck_df in enumerate(df_iterator):
        t_start = time()
        chunck_df.to_sql(name=table_name, con=engine, if_exists="append")
        t_end = time()
        print(
            f"insert #{i+2} chunk into table {table_name}, took {(t_end - t_start):.3f} seconds, {chunck_df.shape[0]:,} inserted."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres DB")

    parser.add_argument("--host", help="hostname for DB connection")
    parser.add_argument("--port", help="port for DB connection")
    parser.add_argument("--user", help="username for DB connection")
    parser.add_argument("--password", help="password for DB connection")
    parser.add_argument("--database", help="database name for DB connection")
    parser.add_argument("--url", help="full URL\Path for  the CSV file")
    parser.add_argument(
        "--table", help="the name of the table you want to insert the data on"
    )

    args = parser.parse_args()
    print(args)
    main(args)
