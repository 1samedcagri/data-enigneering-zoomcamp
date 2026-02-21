import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# 1) Schema / parsing rules (pandas read_csv için)
DTYPE = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

PARSE_DATES = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():
    # 2) Parameters (sonra bunu CLI/env ile de alacağız)
    pg_user = "root"
    pg_pass = "root"
    pg_host = "localhost"
    pg_port = 5432
    pg_db = "ny_taxi"

    year = 2021
    month = 1
    chunksize = 100_000

    # 3) Dataset URL (parameterized)
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    # 4) DB connection
    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # 5) Chunk iterator
    df_iter = pd.read_csv(
        url,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
        iterator=True,
        chunksize=chunksize,
    )

    # 6) Ingestion loop (GitHub'daki mantığın temiz hali)
    first = True

    for df_chunk in tqdm(df_iter):
        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name="yellow_taxi_data",
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="append"
        )

    print("Ingestion finished")


if __name__ == "__main__":
    run()








