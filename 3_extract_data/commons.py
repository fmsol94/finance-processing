import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import List

import pandas as pd
import pdfplumber


def open_json(path: str) -> dict:
    """Open a JSON file and return it as a Python dict."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def custom_encoder(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.strftime("%m-%d-%Y")
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def save_dict_as_json(data, filepath):
    """
    Save a dictionary as a JSON file.

    Args:
        data (dict): The dictionary to be saved.
        filepath (str): The name of the JSON file where the data will be saved.
    """
    with open(filepath, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4, default=custom_encoder)


def open_pdf(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        # Initialize an empty list to hold the data
        data = []

        # Loop through each page in the PDF
        for page in pdf.pages:
            # Extract the text from the page
            text = page.extract_text()
            data.append(text)
    return data


def convert_to_decimal(amt, amt_decimal_places=2):
    """
    Converts a given amount to a Decimal number with the specified number of decimal places.

    Parameters:
    - amt (str or float): The amount to be converted. If a string, it may include "$" and "," characters.
    - amt_decimal_places (int): The number of decimal places in the resulting Decimal number (default is 2).

    Returns:
    - Decimal: The converted amount as a Decimal number.

    Example:
    >>> convert_to_decimal("1,234.56")
    Decimal('1234.56')
    """
    if isinstance(amt, str):
        amt = amt.replace("$", "").replace(",", "")
    return Decimal(
        "{:.{}f}".format(float(amt), amt_decimal_places)
        .replace("$", "")
        .replace(",", "")
    )


def sort_df_by_date(df, sorting_date_col):
    """
    Sorts a DataFrame by a specified date column in ascending order. In case of tie-breaking, it uses the
    original index to maintain the order.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to be sorted.
    - sorting_date_col (str): The name of the date column used for sorting.

    Returns:
    - pandas.DataFrame: The sorted DataFrame.

    Example:
    >>> sorted_df = sort_df_by_date(df, "Date")
    """
    df["original_index"] = df.index
    df = df.sort_values([sorting_date_col, "original_index"], ascending=[True, False])
    df = df.reset_index(drop=True)
    df = df.drop(columns=["original_index"])
    return df


def process_financial_df(
    df,
    date_cols=["Date"],
    date_formats=["%m/%d/%Y"],
    amt_cols=["Amount"],
    amt_decimal_places=2,
    sorting_date_col="Date",
    fillna="N/A",
):
    """
    Processes a financial DataFrame by converting date columns to datetime objects, converting amount columns
    to Decimal numbers, filling NaN values, and sorting the DataFrame by a specified date column.

    Parameters:
    - df (pandas.DataFrame): The financial DataFrame to be processed.
    - date_cols (list): List of column names containing date information (default is ["Date"]).
    - date_formats (list or str): List of date formats corresponding to date_cols or a single date format for all (default is ["%m/%d/%Y"]).
    - amt_cols (list): List of column names containing amounts to be converted (default is ["Amount"]).
    - amt_decimal_places (int or list): Number of decimal places for amount columns or a list of decimal places (default is 2).
    - sorting_date_col (str): The name of the date column used for sorting (default is "Date").
    - fillna (str): The value used to fill NaN entries in the DataFrame (default is "N/A").

    Returns:
    - pandas.DataFrame: The processed and sorted DataFrame.

    Example:
    >>> processed_df = process_financial_df(df, date_cols=["TransactionDate"], amt_cols=["TotalAmount"])
    """
    if isinstance(date_formats, list):
        for date_col, date_format in zip(date_cols, date_formats):
            df[date_col] = pd.to_datetime(df[date_col], format=date_format)
    elif isinstance(date_formats, str):
        for date_col in date_cols:
            df[date_col] = pd.to_datetime(df[date_col], format=date_formats)
    else:
        raise TypeError(
            f"Input 'date_formats' should be type 'list' or 'str', received '{type(date_formats).__name__}'"
        )

    if isinstance(amt_decimal_places, list):
        for amt_col, amt_decimal in zip(amt_cols, amt_decimal_places):
            df[amt_col] = df[amt_col].apply(
                lambda amt, amt_decimal=amt_decimal: convert_to_decimal(
                    amt=amt, amt_decimal_places=amt_decimal
                )
            )
    elif isinstance(amt_decimal_places, int):
        for amt_col in amt_cols:
            df[amt_col] = df[amt_col].apply(
                lambda amt: convert_to_decimal(
                    amt=amt, amt_decimal_places=amt_decimal_places
                )
            )
    else:
        raise TypeError(
            f"Input 'amt_decimal_places' should be type 'list' or 'int', received '{type(amt_decimal_places).__name__}'"
        )
    df = df.fillna(fillna)
    df = sort_df_by_date(df=df, sorting_date_col=sorting_date_col)
    return df


def load_statements_db(statements_db_path: Path) -> pd.DataFrame:
    """
    Load a CSV database of statements if it exists.
    If not, initialize an empty DataFrame and save it for future usage.
    """
    statements_db_path = Path(statements_db_path)
    csv_file = statements_db_path / "statements_db.csv"

    if csv_file.exists():
        statements_db = pd.read_csv(csv_file)
    else:
        statements_db = pd.DataFrame(
            columns=[
                "account_number",
                "month",
                "year",
                "raw_documents_path",
                "processed_documents_path",
            ]
        )
        statements_db.to_csv(csv_file, index=False)

    return statements_db


def find_pdfs(root_folder: str) -> List[Path]:
    """
    Find all PDF files under a folder, including subdirectories.

    Args:
        root_folder (str): Path to the root folder.

    Returns:
        List[Path]: List of PDF file paths.
    """
    root_path = Path(root_folder)
    return list(root_path.rglob("*.pdf"))


def setup_logging(log_file: str = "process_apple.log"):
    """Configure logging to file and console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="w"),
            logging.StreamHandler(),
        ],
    )


def _normalize_text(x):
    if pd.isna(x):
        return ""
    # lower, strip, collapse whitespace
    return " ".join(str(x).strip().lower().split())


def _amount_to_cents(x):
    # robust for strings/floats; keeps sign
    return int(round(float(x) * 100))


def _canonical_name(row, account_number=""):
    # pick column names that likely exist in your df; adjust if yours differ
    date_val = row["Date"]
    date_iso = ""
    if pd.notna(date_val) and date_val != "":
        date_iso = pd.to_datetime(date_val).date().isoformat()

    desc = _normalize_text(row["Description"])
    amt = row["Amount"]
    cents = _amount_to_cents(amt)

    acct = _normalize_text(account_number)

    return f"{acct}|{date_iso}|{desc}|{cents}"


def _deterministic_id(row, account_number="", length=16):
    name = _canonical_name(row, account_number)
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:length]


def load_metadata_from_json(filename):
    """
    Load a dictionary from a JSON file.

    Args:
        filename (str): The name of the JSON file to be loaded.

    Returns:
        dict: The loaded dictionary.
    """
    if not os.path.exists(filename):
        raise FileNotFoundError(
            f"The JSON metadata associated with this statement '{filename}' does not exist."
        )

    with open(filename, "r", encoding="utf-8") as json_file:
        data = json.load(json_file)

    decimal_keys = {
        "Beginning balance",
        "Ending balance",
        "payments_and_credits",
        "purchases",
        "fees_charged",
    }
    date_keys = {"date", "beginning_date", "ending_date"}
    # Convert specific keys to their respective types
    for key, value in data.items():
        if key in decimal_keys:
            data[key] = Decimal(value)
        elif key in date_keys:
            data[key] = datetime.strptime(value, "%m-%d-%Y")

    return data


def calculate_datetime_middle_point(dt1, dt2):
    if dt1 > dt2:
        dt1, dt2 = dt2, dt1  # Ensure dt1 is the earlier datetime

    time_difference = dt2 - dt1
    middle_point = dt1 + time_difference / 2

    return middle_point


def first_day_of_month(datetime_obj):
    # Calculate the first day of the current month
    first_day = datetime_obj.replace(day=1)
    return first_day


def last_day_of_month(datetime_obj):
    # Calculate the first day of the next month
    if datetime_obj.month == 12:
        first_day_next_month = datetime_obj.replace(
            year=datetime_obj.year + 1, month=1, day=1
        )
    else:
        first_day_next_month = datetime_obj.replace(month=datetime_obj.month + 1, day=1)

    # Subtract one day from the first day of the next month to get the last day of the current month
    last_day = first_day_next_month - timedelta(days=1)

    return last_day
