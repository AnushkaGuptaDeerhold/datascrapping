# 1.get some data source csv
# 2.make sure the data soource is considerably large
# 3.write code to scrape the data from source(eg find a link where the csv is set as a downloadable file)
# 4.Transform data into pandas df and then into a spark data frame.
# 5. be sure to get the column name and data type correct(define schema for that)
# 6. perform data manipulation and checks such as replacing null with empty string, making sure the numeric columns should not contain alphabets, the amount column should contain $ sign, etc
# 7. load the resulting table in to data base(prefer mysql since it is locally easy to deploy) or you can find any thing according to convenience

import os
import requests
import pandas as pd
from pyspark.sql import SparkSession

# PostgreSQL database configuration
db_config = {
    'host': '172.31.17.197',
    'port': '5678',
    'database': 'risk_modeler_dev',
    'user': 'anushka_gupta',
    'password': 'BPJ9f9',
    'schema': 'cne_dev'
}

# Path to your PostgreSQL JDBC driver
jdbc_driver_path = "/Users/agupta/Downloads/postgresql-42.6.2.jar"

# Function to download the CSV file from the URL
def download_csv(url, file_name):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        with open(file_name, "w") as file:
            file.write(response.text)
        print(f"CSV file downloaded and saved as {file_name}.")
    except requests.RequestException as e:
        print(f"Failed to download file. Error: {e}")
    return file_name

# Function to convert the CSV file to a Pandas DataFrame
def csv_to_dataframe(file_name):
    try:
        columns = [
            "Serial Number", "List Year", "Date Recorded", "Town", "Address", "Assessed Value",
            "Sale Amount", "Sales Ratio", "Property Type", "Residential Type", "Non Use Code",
            "Assessor Remarks", "OPM remarks", "Location"
        ]
        dtype = {
            "Sale Amount": str,
            "Sales Ratio": str,
        }
        df = pd.read_csv(file_name, names=columns, header=0, dtype=dtype, low_memory=False)
        print("CSV file successfully loaded into DataFrame.")
        return df
    except Exception as e:
        print(f"Failed to load CSV into DataFrame. Error: {e}")

# Function to clean data
def clean_data(df):
    # Replace null values with empty strings
    df.fillna("", inplace=True)
    
    # Ensure numeric columns contain valid data
    df["Sale Amount"] = df["Sale Amount"].apply(lambda x: f"${x}" if x.replace('.', '', 1).isdigit() else "")
    df["Sales Ratio"] = df["Sales Ratio"].apply(lambda x: x if x.replace('.', '', 1).isdigit() else "")
    
    # Clean the 'List Year' column by removing commas and converting to int
    df["List Year"] = df["List Year"].apply(lambda x: str(x).replace(",", "") if isinstance(x, str) else str(x))
    
    # Ensure 'List Year' is a valid 4-digit integer
    df["List Year"] = df["List Year"].apply(lambda x: int(x) if x.isdigit() and len(x) == 4 else None)
    
    return df

# Function to convert Pandas DataFrame to Spark DataFrame
def pandas_to_spark_dataframe(pandas_df):
    # Initialize a Spark session with the correct JDBC driver
    spark = SparkSession.builder \
        .appName("CSV to Spark DataFrame") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)
    return spark_df

# Function to load the data into PostgreSQL
def load_to_postgresql(spark_df, table_name, db_config):
    url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['database']}"
    properties = {
        "user": db_config['user'],
        "password": db_config['password'],
        "driver": "org.postgresql.Driver"
    }
    # Write DataFrame to PostgreSQL
    spark_df.write.jdbc(url=url, table=f"{db_config['schema']}.{table_name}", mode="overwrite", properties=properties)
    print(f"Data successfully written to PostgreSQL table: {table_name}")

# Main function to run the steps
def main():
    url = "https://data.ct.gov/api/views/5mzw-sjtu/rows.csv?accessType=DOWNLOAD"
    file_name = "RealEstateSales3.csv"
    
    # Download the CSV
    csv_file = download_csv(url, file_name)
    
    # Convert CSV file to Pandas DataFrame
    pandas_df = csv_to_dataframe(csv_file)
    
    # Clean the data
    cleaned_df = clean_data(pandas_df)
    
    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = pandas_to_spark_dataframe(cleaned_df)
    
    # Load the cleaned data into PostgreSQL
    load_to_postgresql(spark_df, "real_estate_sales", db_config)

if __name__ == "__main__":
    main()

