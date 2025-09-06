import os
from decimal import Decimal
from pathlib import Path

import pandas as pd


def convert_to_decimal(amt, amt_decimal_places=2):
    if isinstance(amt, str):
        amt = amt.replace("$", "").replace(",", "")
    return Decimal(
        "{:.{}f}".format(float(amt), amt_decimal_places)
        .replace("$", "")
        .replace(",", "")
    )


def load_data_from_csv(filepath, load_all_columns=False):
    if os.path.exists(filepath):
        if load_all_columns:
            data = pd.read_csv(filepath, index_col=False)
        else:
            data = pd.read_csv(filepath, index_col=False)[
                [
                    "Date",
                    "Description",
                    "Amount",
                    "Category",
                    "Flow Type",
                    "TransactionID",
                    "Source",
                    "Account",
                ]
            ]
    else:
        raise FileNotFoundError(
            f"The CSV data associated with this statement '{filepath}' does not exist."
        )
    data["Amount"] = data["Amount"].apply(convert_to_decimal)
    data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
    return data


def get_report_folder(report_date: str, project_path) -> Path:
    try:
        m, y = map(int, report_date.split("-"))
    except ValueError:
        raise ValueError("Invalid format: must be MM-YYYY or M-YYYY")

    if not (1 <= m <= 12):
        raise ValueError("Month must be 1–12")
    if not (1000 <= y <= 9999):
        raise ValueError("Year must be 4 digits")

    folder = project_path / "Monthly Reports" / f"{y:04d}" / f"{m:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


report_date = "5-2025"
project_path = Path("/home/francisco/Documents/Finances/Statements")
folder = get_report_folder(report_date, project_path)
transactions = load_data_from_csv(folder / "transactions.csv")

# Columns to validate
cols_to_check = ["Category", "Flow Type"]

# Find rows where any of the selected columns has "Undefined"
mask = transactions[cols_to_check].eq("Undefined").any(axis=1)

if mask.any():
    bad_rows = transactions.loc[mask]
    raise ValueError(
        f"Found 'Undefined' values in columns {cols_to_check}. "
        f"Please annotate manually.\n\nProblematic rows:\n{bad_rows}"
    )
