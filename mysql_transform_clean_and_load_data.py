import pandas as pd
from sqlalchemy import create_engine

# Connect to MySQL Database
DATABASE_URL = "mysql+mysqlconnector://root:XXXXXXXXXXXXX@localhost/Sales_Project"
engine = create_engine(DATABASE_URL)

# Define column data types for SQL database
from sqlalchemy import Integer, String, Float, Date

sql_dtypes = {
    'area_code': Integer,
    'state': String(100),
    'market': String(100),
    'market_size': String(100),
    'profit': Float,
    'margin': Integer,
    'sales': Float,
    'cogs': Integer,
    'total_expenses': Float,
    'marketing': Float,
    'inventory': Integer,
    'budget_profit': Float,
    'budget_cogs': Float,
    'budget_margin': Float,
    'budget_sales': Float,
    'productid': Integer,
    'date': Date,
    'product_type': String(100),
    'product': String(100),
    'type': String(100)
}


try:
    # 1. Read Excel File
    print("Reading Excel file...")
    excel_data = pd.read_excel('sales.xlsx')
    print("Excel file has been read successfully.")

    # Cleaning column headers
    excel_data.columns = ["".join(filter(lambda char: char.isprintable(), col)).replace(' ', '_').lower() for col in excel_data.columns]
    print("Column headers cleaned.")

    # 2. Data Preprocessing

    # Type Validation
    integer_columns = ['area_code', 'productid']
    float_columns = ['profit', 'margin', 'sales', 'cogs', 'total_expenses', 'marketing', 'inventory', 'budget_profit', 'budget_cogs', 'budget_sales']

    for col in integer_columns:
        excel_data[col] = excel_data[col].astype(int, errors='ignore')

    for col in float_columns:
        excel_data[col] = excel_data[col].astype(float, errors='ignore')

    # Handle Missing Values
    excel_data[float_columns + integer_columns] = excel_data[float_columns + integer_columns].fillna(0)
    string_columns = ['state', 'market', 'product_type', 'product', 'type']
    excel_data[string_columns] = excel_data[string_columns].fillna('Unknown')

    # Date Formatting
    excel_data['date'] = pd.to_datetime(excel_data['date'], errors='coerce')

    # Text Cleaning
    excel_data['state'] = excel_data['state'].str.upper()
    excel_data['market'] = excel_data['market'].str.upper()
    excel_data['product_type'] = excel_data['product_type'].str.upper()
    excel_data['product'] = excel_data['product'].str.upper()
    excel_data['type'] = excel_data['type'].str.upper()

    # Range Checks
    financial_metrics = ['profit', 'margin', 'sales', 'cogs', 'total_expenses', 'marketing', 'inventory', 'budget_profit', 'budget_cogs', 'budget_sales']
    excel_data = excel_data[(excel_data[financial_metrics] >= 0).all(axis=1)]

    # Removing Duplicates
    excel_data.drop_duplicates(inplace=True)
    print("Data preprocessing completed.")

    # 3. Load data into Database
    print("Loading data into database...")
    excel_data.to_sql('Business_Data', engine, dtype=sql_dtypes, index=False, if_exists='replace')
    print("Data has been loaded successfully!")
  
except FileNotFoundError:
    print("Error: The specified Excel file was not found.")
except pd.errors.EmptyDataError:
    print("Error: The Excel file is empty.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

