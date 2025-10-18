import pandas as pd

# 1. Read the data from a CSV file into a collection
data_file = 'Level_2_Coding_Challenge_Assignment_1/assignment1/customer_shopping_data.csv'
df = pd.read_csv(data_file)
print(f"{df.head()}\n")

# 2. Count the population grouped by gender
population_by_gender = df['gender'].value_counts().reset_index()
population_by_gender.columns = ['gender', 'count']
print(f"Population grouped by gender:\n{population_by_gender}\n")

# 3. Find total sales grouped by gender (total sales = quantity * price)
df['total_sales'] = df['quantity'] * df['price']

sales_by_gender = df.groupby('gender')['total_sales'].sum().reset_index()
print("Total sales grouped by gender:")
print(f"{sales_by_gender.to_string(float_format='{:,.2f}'.format)}\n")

# 4. Find most used payment method
most_used_payment = df['payment_method'].value_counts().idxmax()
print(f"Most used payment method: {most_used_payment}\n")

# 5. Find day with the most sales
# Convert invoice_date to datetime and handle dates in D/M/Y format
df['invoice_date'] = pd.to_datetime(df['invoice_date'], errors='coerce', dayfirst=True)

# Sum total sales per day
sales_by_day = df.groupby('invoice_date')['total_sales'].sum().reset_index()

top_day = sales_by_day.nlargest(1, 'total_sales')
print(f"Day with the most sales:\n{top_day.to_string(index=False)}")
