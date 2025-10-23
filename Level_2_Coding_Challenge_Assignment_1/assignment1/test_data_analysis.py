import pytest
import pandas as pd
from data_analysis import (
    get_population_by_gender,
    get_sales_by_gender,
    get_most_used_payment,
    get_day_with_most_sales,
)

@pytest.fixture
def mock_df():
    return pd.DataFrame({
        'gender': ['Male', 'Female', 'Female', 'Male', 'Male'],
        'quantity': [2, 1, 3, 2, 5],
        'price': [10.0, 20.0, 5.0, 15.0, 12.0],
        'payment_method': ['Credit Card', 'Debit Card', 'Cash', 'Credit Card', 'Cash'],
        'invoice_date': ['10/01/2023', '11/02/2023', '12/02/2023', '22/03/2023', '30/03/2023']
    })

def test_population_by_gender(mock_df):
    result, total = get_population_by_gender(mock_df)
    assert total == 5
    assert set(result['gender']) == {'Male', 'Female'}
    assert result.loc[result['gender'] == 'Male', 'Count'].values[0] == 3
    assert result.loc[result['gender'] == 'Female', 'Count'].values[0] == 2

def test_sales_by_gender(mock_df):
    result, grand_total = get_sales_by_gender(mock_df)
    assert 'gender' in result.columns
    assert 'total_sales' in result.columns
    assert pytest.approx(grand_total, 0.01) == (2*10 + 1*20 + 3*5 + 2*15 + 5*12)

def test_most_used_payment(mock_df):
    payment_df, most_used, count = get_most_used_payment(mock_df)
    assert 'Count' in payment_df.columns
    assert most_used in ['Credit Card', 'Debit Card', 'Cash']  # could tie
    assert count > 0

def test_day_with_most_sales(mock_df):
    result = get_day_with_most_sales(mock_df)
    assert 'invoice_date' in result.columns
    assert 'total_sales' in result.columns
    assert isinstance(result.iloc[0]['total_sales'], float)

def test_missing_column_raises_error():
    df = pd.DataFrame({'gender': ['M', 'F']})
    with pytest.raises(KeyError):
        get_sales_by_gender(df)

def test_empty_gender_column():
    df = pd.DataFrame({'gender': [None, None]})
    with pytest.raises(ValueError):
        get_population_by_gender(df)
