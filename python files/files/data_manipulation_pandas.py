import pandas as pd

data = pd.read_csv()

df_clean = data.dropna()

df_clean['Order Date'] = pd.to_datetime(df_clean['Order Date'])

total_sales_by_product = df_clean.groupby('Product Name')['Sales'].sum()

total_sales_by_product = total_sales_by_product.sort_values(ascending=False)

df_clean['Month'] = df_clean['Order Date'].dt.to_period('M')

sales_by_month = df_clean.groupby('Month').['Sales'].sum()

