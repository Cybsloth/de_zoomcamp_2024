import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{M:02d}.parquet'

    green_taxi_dtypes = {
        'VendorID':  pd.Int64Dtype(),
        'store_and_fwd_flag':  str,
        'RatecodeID':  pd.Int64Dtype(),
        'PULocationID':  pd.Int64Dtype(),
        'DOLocationID':  pd.Int64Dtype(),
        'passenger_count':  pd.Int64Dtype(),
        'trip_distance':  float,
        'fare_amount':  float,
        'extra':  float,
        'mta_tax':  float,
        'tip_amount':  float,
        'tolls_amount':  float,
        'ehail_fee':  float,
        'improvement_surcharge':  float,
        'total_amount':  float,
        'payment_type':  pd.Int64Dtype(),
        'trip_type':  pd.Int64Dtype(),
        'congestion_surcharge':  float,
    }

    dates = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
    ]

    data_df = []
    for month in kwargs['months']:
        _url = url.format(M=month)
        
        df = pd.read_parquet(
            _url,
            # compression='gzip', 
            # dtype=green_taxi_dtypes,
            # parse_dates=dates
        )
        print(f'Download: {_url}, number of rows = {df.shape[0]}')
        data_df.append(df)
    data_df = pd.concat(data_df, axis=0, ignore_index=True)
    print(f'Final number of rows = {data_df.shape[0]}')

    return data_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
