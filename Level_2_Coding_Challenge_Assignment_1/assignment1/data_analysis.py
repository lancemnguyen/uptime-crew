"""
Customer Shopping Data Analysis
-------------------------------
Performs data analysis on customer shopping data, including:
- Population by gender
- Total sales by gender
- Most used payment method
- Day with the highest total sales
"""

import pandas as pd
import os

# 1. Read the data from a CSV file into a collection
def load_data() -> pd.DataFrame:
    """Reads the CSV file and returns a DataFrame."""
    try:
        dir_path = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(dir_path, 'customer_shopping_data.csv')
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        raise ValueError("The file is empty or corrupted.")

# 2. Count the population grouped by gender
def get_population_by_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Returns and prints the population count grouped by gender."""
    counts = df['gender'].value_counts()
    total = counts.sum()
    percentages = counts / total * 100
    result = pd.DataFrame({
        'Count': counts,
        'Percentage': percentages.map("{:.2f}%".format)
    })
    print("Population grouped by gender:")
    print(result.reset_index().to_string(index=False))
    print(f"Total: {total:,}")
    return result

# 3. Find total sales grouped by gender (total sales = quantity * price)
def get_sales_by_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates, prints, and returns total sales grouped by gender."""
    df['total_sales'] = df['quantity'] * df['price']
    sales = df.groupby('gender', as_index=False)['total_sales'].sum()
    
    grand_total = sales['total_sales'].sum()
    sales['Total Sales'] = sales['total_sales'].map("${:,.2f}".format)
    sales['Percentage'] = (sales['total_sales'] / grand_total * 100).map("{:.2f}%".format)
    sales = sales.drop(columns='total_sales')
    
    print("\nTotal Sales by Gender")
    print(sales.to_string(index=False))
    print(f"Grand Total: ${grand_total:,.2f}")
    
    return sales

# 4. Find most used payment method
def get_most_used_payment(df: pd.DataFrame) -> str:
    """Finds, prints, and returns the most common payment method."""
    counts = df['payment_method'].value_counts()
    total = counts.sum()
    percentages = counts / total * 100
    result = pd.DataFrame({
        'Count': counts,
        'Percentage': percentages.map("{:.2f}%".format)
    })
    most_used = counts.idxmax()
    most_used_count = counts.max()
    print("\nPayment Method Usage:")
    print(result.reset_index().to_string(index=False))
    print(f"\nMost used payment method: {most_used} ({most_used_count:,} transactions)")
    return result

# 5. Find day with the most sales
def get_day_with_most_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Finds, prints, and returns the day with the highest total sales."""
    df['invoice_date'] = pd.to_datetime(df['invoice_date'], errors='coerce', dayfirst=True)
    df['total_sales'] = df['quantity'] * df['price']
    result = (df.groupby('invoice_date', as_index=False)['total_sales']
                .sum()
                .nlargest(1, 'total_sales'))
    print("\nDay with the most sales:")
    print(result.to_string(index=False, float_format='{:,.2f}'.format))
    return result

def main():
    """Main function to run all analyses."""
    # Load data
    df = load_data()

    # Run analyses
    get_population_by_gender(df)
    get_sales_by_gender(df)
    get_most_used_payment(df)
    get_day_with_most_sales(df)

if __name__ == "__main__":
    main()
