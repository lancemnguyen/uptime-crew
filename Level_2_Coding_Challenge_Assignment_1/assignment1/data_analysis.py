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
import pathlib

# 1. Read the data from a CSV file into a collection
def load_data() -> pd.DataFrame:
    """Reads the CSV file and returns a DataFrame."""
    try:
        script_dir = pathlib.Path(__file__).parent
        file_path = script_dir / 'customer_shopping_data.csv'
        df = pd.read_csv(file_path)

        if df.empty:
            raise ValueError("The dataset is empty.")
        required_columns = {'gender', 'quantity', 'price', 'payment_method', 'invoice_date'}
        missing = required_columns - set(df.columns)
        if missing:
            raise KeyError(f"Missing required columns: {missing}")

        return df

    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        raise ValueError("The file is empty or corrupted.")
    except pd.errors.ParserError as e:
        raise ValueError(f"Error parsing CSV: {e}")

# 2. Count the population grouped by gender
def get_population_by_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates and returns the population count grouped by gender and the total count."""
    if 'gender' not in df.columns:
        raise KeyError("Column 'gender' not found in DataFrame.")
    if df['gender'].isna().all():
        raise ValueError("No valid gender data available.")
    
    counts = df['gender'].value_counts()
    total = counts.sum()
    percentages = counts / total * 100
    result_df = pd.DataFrame({
        'Count': counts,
        'Percentage': percentages
    })
    return result_df.reset_index(), total

# 3. Find total sales grouped by gender
def get_sales_by_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates and returns total sales grouped by gender and the grand total."""
    for col in ['gender', 'quantity', 'price']:
        if col not in df.columns:
            raise KeyError(f"Column '{col}' not found in DataFrame.")

    df_temp = df.copy()
    df_temp = df_temp.dropna(subset=['gender', 'quantity', 'price'])
    df_temp['total_sales'] = df_temp['quantity'] * df_temp['price']

    sales = df_temp.groupby('gender', as_index=False)['total_sales'].sum()
    grand_total = sales['total_sales'].sum()
    sales['Percentage'] = (sales['total_sales'] / grand_total * 100) if grand_total > 0 else 0
    return sales, grand_total

# 4. Find most used payment method
def get_most_used_payment(df: pd.DataFrame):
    """Calculates and returns payment method usage counts, the most used method, and its count."""
    if 'payment_method' not in df.columns:
        raise KeyError("Column 'payment_method' not found in DataFrame.")
    if df['payment_method'].isna().all():
        raise ValueError("No valid payment method data available.")
    
    counts = df['payment_method'].value_counts()
    total = counts.sum()
    percentages = counts / total * 100
    result_df = pd.DataFrame({
        'Count': counts,
        'Percentage': percentages
    })
    
    most_used = counts.idxmax()
    most_used_count = counts.max()
    
    return result_df.reset_index(), most_used, most_used_count

# 5. Find day with the most sales
def get_day_with_most_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates and returns the day with the highest total sales."""
    for col in ['invoice_date', 'quantity', 'price']:
        if col not in df.columns:
            raise KeyError(f"Column '{col}' not found in DataFrame.")
    
    df_temp = df.copy()
    df_temp['invoice_date'] = pd.to_datetime(df_temp['invoice_date'], errors='coerce', dayfirst=True)
    df_temp = df_temp.dropna(subset=['invoice_date', 'quantity', 'price'])
    df_temp['total_sales'] = df_temp['quantity'] * df_temp['price']

    if df_temp.empty:
        raise ValueError("No valid invoice data available for sales computation.")
    
    result = (
        df_temp.groupby('invoice_date', as_index=False)['total_sales']
        .sum()
        .nlargest(1, 'total_sales')
    )
    return result

def main():
    """Main function to run all analyses and handle presentation."""
    print("--- Customer Shopping Data Analysis ---")

    try:
        df = load_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    try:
        # --- 1. Population by Gender ---
        population_df, total_population = get_population_by_gender(df)
        print("\n1. Population Grouped by Gender")
        population_df['Percentage'] = population_df['Percentage'].map("{:.2f}%".format)
        print(population_df.rename(columns={'index': 'Gender'}).to_string(index=False))
        print(f"Total: {total_population:,}")

        # --- 2. Total Sales by Gender ---
        sales_df, grand_total_sales = get_sales_by_gender(df)
        print("\n2. Total Sales by Gender")
        sales_df['Total Sales'] = sales_df['total_sales'].map("${:,.2f}".format)
        sales_df['Percentage'] = sales_df['Percentage'].map("{:.2f}%".format)
        print(sales_df.rename(columns={'gender': 'Gender'}).drop(columns=['total_sales']).to_string(index=False))
        print(f"Grand Total: ${grand_total_sales:,.2f}")

        # --- 3. Most Used Payment Method ---
        payment_df, most_used_method, most_used_count = get_most_used_payment(df)
        print("\n3. Payment Method Usage")
        payment_df['Percentage'] = payment_df['Percentage'].map("{:.2f}%".format)
        print(payment_df.rename(columns={'index': 'Payment Method'}).to_string(index=False))
        print(f"\nMost used payment method: {most_used_method} ({most_used_count:,} transactions)")

        # --- 4. Day with the Most Sales ---
        day_sales_df = get_day_with_most_sales(df)
        print("\n4. Day with the Most Sales")
        day_sales_df['total_sales'] = day_sales_df['total_sales'].map('{:,.2f}'.format)
        print(day_sales_df.rename(columns={'invoice_date': 'Date', 'total_sales': 'Total Sales'}).to_string(index=False))
    
    except Exception as e:
        print(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()
