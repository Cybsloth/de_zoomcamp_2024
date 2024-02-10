if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd


@transformer
def transform(data: pd.DataFrame, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    # data = data[
    #     (data['passenger_count'] > 0) &\
    #     (data['trip_distance'] > 0)
    # ]
    data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    data['lpep_dropoff_datetime'] = pd.to_datetime(data['lpep_dropoff_datetime'])
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data = data.rename(
        columns={
            'VendorID': 'vendor_id',
            'RatecodeID': 'ratecode_id',
            'PULocationID': 'pu_location_id',
            'DOLocationID': 'do_location_id',
        }
    )

    # vendor_id is one of the existing values in the column (currently)
    # passenger_count is greater than 0
    # trip_distance is greater than 0


    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
