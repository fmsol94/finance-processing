from pathlib import Path

from chase_csv import load_chase_csv, process_data
from chase_metadata import process_chase, statement_not_available

## STEP IF STATEMENT IS AVAILABLE:

# Step by step
# 1. Download statement and put in account/statements/year/month
# Example:
# Accounts/Chase-4106/Statements/2025/08/20250823-statements-4106-.pdf
# Just run the process_chase function
project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts")
process_chase(project_path, "4106")

# 2. Now download csv and put it in csv folder (not required but recommended)
# Check that dates of the csv are correct to contain statement
# Specify year, month and account
year = 2025
month = 8
acct = "4106"

csv_path = Path(
    "/home/francisco/Documents/Finances/Statements/Accounts/Chase-4106/CSV/Chase4106_Activity20230906_20250906_20250906.CSV"
)
df = load_chase_csv(csv_path, acct)
process_data(df, month, str(year), project_path, acct=acct, csv_path=csv_path)

# IF STATEMENT IS NOT AVAILABLE:
statement_not_available(2025, 7, "4106", project_path, csv_path)
