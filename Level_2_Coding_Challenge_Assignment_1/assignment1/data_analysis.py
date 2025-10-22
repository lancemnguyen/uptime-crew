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
        return df
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        raise ValueError("The file is empty or corrupted.")

# 2. Count the population grouped by gender
def get_population_by_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates and returns the population count grouped by gender and the total count."""
    counts = df['gender'].value_counts()
    total = counts.sum()
    percentages = counts / total * 100
    result_df = pd.DataFrame({
        'Count': counts,
        'Percentage_Raw': percentages
    })
    return result_df.reset_index(), total

# 3. Find total sales grouped by gender (total sales = quantity * price)
def get_sales_by_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates and returns total sales grouped by gender and the grand total."""
    df_temp = df.copy() 
    df_temp['total_sales'] = df_temp['quantity'] * df_temp['price']
    
    sales = df_temp.groupby('gender', as_index=False)['total_sales'].sum()
    grand_total = sales['total_sales'].sum()
    sales['Percentage_Raw'] = (sales['total_sales'] / grand_total * 100)
    
    return sales, grand_total

# 4. Find most used payment method
def get_most_used_payment(df: pd.DataFrame) -> str:
    """Calculates and returns payment method usage counts, the most used method, and its count."""
    counts = df['payment_method'].value_counts()
    total = counts.sum()
    percentages = counts / total * 100
    
    result_df = pd.DataFrame({
        'Count': counts,
        'Percentage_Raw': percentages
    })
    
    most_used = counts.idxmax()
    most_used_count = counts.max()
    
    return result_df.reset_index(), most_used, most_used_count

# 5. Find day with the most sales
def get_day_with_most_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates and returns the day with the highest total sales."""
    df_temp = df.copy() 
    df_temp['invoice_date'] = pd.to_datetime(df_temp['invoice_date'], errors='coerce', dayfirst=True)
    df_temp['total_sales'] = df_temp['quantity'] * df_temp['price']
    
    result = (df_temp.groupby('invoice_date', as_index=False)['total_sales']
                .sum()
                .nlargest(1, 'total_sales'))
    
    return result

def main():
    """Main function to run all analyses and handle presentation."""
    print("--- Customer Shopping Data Analysis ---")

    # Load data
    try:
        df = load_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # --- 1. Population by Gender ---
    population_df, total_population = get_population_by_gender(df)
    print("\n1. Population Grouped by Gender")
    population_df['Percentage'] = population_df['Percentage_Raw'].map("{:.2f}%".format)
    display_df = population_df.rename(columns={'index': 'Gender', 'Percentage_Raw': 'Raw'}).drop(columns='Raw')
    print(display_df.to_string(index=False))
    print(f"Total: {total_population:,}")

    # --- 2. Total Sales by Gender ---
    sales_df, grand_total_sales = get_sales_by_gender(df)
    print("\n2. Total Sales by Gender")
    sales_df['Total Sales'] = sales_df['total_sales'].map("${:,.2f}".format)
    sales_df['Percentage'] = sales_df['Percentage_Raw'].map("{:.2f}%".format)
    display_df = sales_df.rename(columns={'gender': 'Gender', 'total_sales': 'Raw', 'Percentage_Raw': 'Raw2'}).drop(columns=['Raw', 'Raw2'])
    print(display_df.to_string(index=False))
    print(f"Grand Total: ${grand_total_sales:,.2f}")

    # --- 3. Most Used Payment Method ---
    payment_df, most_used_method, most_used_count = get_most_used_payment(df)
    print("\n3. Payment Method Usage")
    payment_df['Percentage'] = payment_df['Percentage_Raw'].map("{:.2f}%".format)
    display_df = payment_df.rename(columns={'index': 'Payment Method', 'Percentage_Raw': 'Raw'}).drop(columns='Raw')
    print(display_df.to_string(index=False))
    print(f"\nMost used payment method: {most_used_method} ({most_used_count:,} transactions)")

    # --- 4. Day with the Most Sales ---
    day_sales_df = get_day_with_most_sales(df)
    print("\n4. Day with the Most Sales")
    day_sales_df['total_sales'] = day_sales_df['total_sales'].map('{:,.2f}'.format)
    print(day_sales_df.rename(columns={'invoice_date': 'Date', 'total_sales': 'Total Sales'}).to_string(index=False))


if __name__ == "__main__":
    main()