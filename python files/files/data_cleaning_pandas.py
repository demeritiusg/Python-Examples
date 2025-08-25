import pandas as pd
import logging
import os


# Congfigue logging 
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s [%(levelname)s] %(message)s"
)

def clean_customer_data(
  input_path: str,
  output_path: str = "output/cleaned_customer_data.csv"
):
  """
  Cleans and transforms raw customer data for analytics.

  steps:
  - Handles missing values
  - Feature engineering (tenure in months)
  - Encodes categorical variables
  - Saves cleaned dataset for downstream use

  Parameters
  ----------
  input_path : str
      Path to the raw customer data CSV file.
  output_path : str, optional
      Path to save the cleaned CSV. Default is 'output/cleaned_customer_data.csv

  Returns
  -------
  df : pd.DataFrame
      The cleaned and transformed dataset
  """


  # Load data
  logging.info(f"Loading customer data from {input_path}...")
  try:
    df = pd.read_csv(input_path)
  except FileNotFoundError:
    logging.error(f"File not found: {input_path}")
    raise
  except Exception as e:
    logging.error(f"Error reading {input_path}: {e}")
    raise
  
  logging.info(f"Dataset loaded with {df.shape[0]} rows and {df.shape[1]} columns")
  
  # Handle mising values
  if df.isnull().any():
    logging.info("Handling missing values with forward fill...")
    df.fillna(method="ffill", inplace=True)
  
  # Feature engineering
  if "tenure_days" in df.columns:
    logging.info("Creating 'tenure_months' feature...")
    df["tenure_months"] = (df["tenure_days"] / 30).round().astype(int)
  
  # Encode categorical variables
  if "region" in df.columns:
    logging.info("Encoding 'region' as categorical codes...")
    df["region_encoded"] = df["region"].astype("cataegory").cat.codes
  
  # Save cleaned data
  os.makedirs(os.path.dirname(out_path), exist_ok=True)
  df.to_csv(out_path, index=False)
  logging.info(f"Cleaned data saved to {Output_path}")
  
  return df

if __name__ == "__main__":
  clean_customer_data()
