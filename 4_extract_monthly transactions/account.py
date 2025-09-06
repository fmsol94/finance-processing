import json
import os
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict

import pandas as pd


class DuplicateFileError(RuntimeError):
    pass


def build_account_file_index(
    account_root: Path,
) -> Dict[str, Dict[str, Dict[str, Path]]]:
    """
    Scan for .../<YEAR>/<MONTH>/(metadata.json|transactions.csv) and return:

        {
          "<year>": {
            "<month>": {
              "metadata": Path,        # .../metadata.json
              "transactions": Path     # .../transactions.csv
            }, ...
          }, ...
        }

    - Raises DuplicateFileError if more than one of the same file type exists
      under the same YEAR/MONTH.
    - Ignores unrelated files.
    """
    index: Dict[str, Dict[str, Dict[str, Path]]] = {}

    for p in account_root.rglob("*"):
        if p.name not in {"metadata.json", "transactions.csv"}:
            continue
        # Expect .../<year>/<month>/file
        try:
            month = p.parent.name
            year = p.parent.parent.name
        except Exception as exc:
            raise RuntimeError(f"Unexpected folder layout for {p}") from exc

        year_bucket = index.setdefault(year, {})
        month_bucket = year_bucket.setdefault(month, {})

        if p.name == "metadata.json":
            if "metadata" in month_bucket:
                raise DuplicateFileError(
                    f"Duplicate metadata for {year}/{month}: "
                    f"{month_bucket['metadata']} and {p}"
                )
            month_bucket["metadata"] = p
        else:  # "transactions.csv"
            if "transactions" in month_bucket:
                raise DuplicateFileError(
                    f"Duplicate transactions for {year}/{month}: "
                    f"{month_bucket['transactions']} and {p}"
                )
            month_bucket["transactions"] = p

    return index


def validate_account_file_index(index: Dict[str, Dict[str, Dict[str, Path]]]) -> None:
    """
    Optional: ensure each (year, month) has both files.
    Raises FileNotFoundError if any are missing.
    """
    for year, months in index.items():
        for month, files in months.items():
            missing = {"metadata", "transactions"} - set(files)
            if missing:
                missing_list = ", ".join(sorted(missing))
                raise FileNotFoundError(f"Missing {missing_list} for {year}/{month}")


def open_json(path: str) -> dict:
    """Open a JSON file and return it as a Python dict."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path}: {e}") from e

    if not isinstance(data, dict):
        raise ValueError(
            f"Expected JSON object (dict) in {path}, got {type(data).__name__}"
        )

    return data


def load_metadata_from_json(filepath):
    """
    Load a dictionary from a JSON file.

    Args:
        filepath (str): The filepath of the JSON file to be loaded.

    Returns:
        dict: The loaded dictionary.
    """
    data = open_json(filepath)

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


def get_col_val(
    df,
    col,
    date_obj,
    acct_name,
    date_col="Date",
    min_date=None,
    max_date=None,
    return_warnings=False,
):
    """
    Get the value of a specific column for a given date in a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        col (str): The name of the column for which to retrieve the value.
        date_obj (datetime.date): The date object to look for in the DataFrame.
        acct_name (str): The name of the account or entity associated with the data.
        date_col (str, optional): The name of the date column in the DataFrame. Default is "Date".
        min_date (datetime.date, optional): The minimum date allowed in the search range. Default is None.
        max_date (datetime.date, optional): The maximum date allowed in the search range. Default is None.
        return_warnings (bool, optional): Whether to return warnings in case of out-of-range dates. Default is False.

    Returns:
        Decimal: The value of the specified column for the given date in the DataFrame.
        list of str: A list of warnings (if return_warnings is True).
    """
    warnings = []
    if min_date is None:
        min_date = df[date_col].min()

    if max_date is None:
        max_date = df[date_col].max()

    if date_obj < min_date:
        col_val = Decimal(0)
    elif date_obj > max_date:
        warnings.append(
            f"Warning! Provided date {date_obj.strftime('%b %d, %Y')} is out of range for {acct_name}, last processed date is {max_date.strftime('%b %d, %Y')}"
        )
        col_val = df.iloc[-1][col]
    else:
        idx = df[date_col].searchsorted(date_obj, side="right")
        col_val = df.iloc[idx - 1][col]
    if return_warnings:
        return col_val, warnings
    else:
        return col_val


def get_df_range(
    df,
    start_date_obj,
    end_date_obj,
    acct_name,
    date_col="Date",
    min_date=None,
    max_date=None,
    return_warnings=False,
):
    """
    Get a subset of a DataFrame based on a date range.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        start_date_obj (datetime.date): The start date of the range (inclusive).
        end_date_obj (datetime.date): The end date of the range (inclusive).
        acct_name (str): The name of the account or entity associated with the data.
        date_col (str, optional): The name of the date column in the DataFrame. Default is "Date".
        min_date (datetime.date, optional): The minimum date allowed in the search range. Default is None.
        max_date (datetime.date, optional): The maximum date allowed in the search range. Default is None.
        return_warnings (bool, optional): Whether to return warnings in case of out-of-range dates. Default is False.

    Returns:
        pd.DataFrame: A subset of the input DataFrame containing rows within the specified date range.
        list of str: A list of warnings (if return_warnings is True).
    """
    # Implementation of the function to extract a date range from the DataFrame
    warnings = []
    if min_date is None:
        min_date = df[date_col].min()

    if max_date is None:
        max_date = df[date_col].max()

    if start_date_obj > end_date_obj:
        raise ValueError("Start date cannot be greater than end date.")

    if end_date_obj < min_date:
        warnings.append(
            f"Warning! End date {end_date_obj.strftime('%b %d, %Y')} is out of range for {acct_name}, last processed date is {max_date.strftime('%b %d, %Y')}"
        )
        filtered_df = df.head(0)  # Empty DataFrame

    elif start_date_obj > max_date:
        warnings.append(
            f"Warning! Start date {start_date_obj.strftime('%b %d, %Y')} is out of range for {acct_name}, last processed date is {max_date.strftime('%b %d, %Y')}"
        )
        filtered_df = df.head(0)  # Empty DataFrame
    elif end_date_obj > max_date:
        warnings.append(
            f"Warning! End date {end_date_obj.strftime('%b %d, %Y')} is out of range for {acct_name}, last processed date is {max_date.strftime('%b %d, %Y')}"
        )
        filtered_df = df[
            (df[date_col] >= start_date_obj) & (df[date_col] <= end_date_obj)
        ]
    else:
        filtered_df = df[
            (df[date_col] >= start_date_obj) & (df[date_col] <= end_date_obj)
        ]

    if return_warnings:
        return filtered_df, warnings
    else:
        return filtered_df


class Account:
    REQUIRED_FIELDS = [
        "name",
        "acct_n",
        "open_date",
    ]

    def __init__(self, acct_folder: str, start=None, end=None):
        self.acct_folder = Path(acct_folder)
        details_path = self.acct_folder / "details.json"
        details = open_json(details_path)

        # Validate required fields
        missing = [field for field in self.REQUIRED_FIELDS if field not in details]
        if missing:
            raise ValueError(f"Missing required fields in details.json: {missing}")

        self.acct_number = str(details["acct_n"])

        try:
            self.open_date = datetime.strptime(details["open_date"], "%m/%Y")
        except ValueError as e:
            raise ValueError(
                f"Invalid date format for 'open_date' in details.json. "
                f"Expected MM/YYYY, got: {details['open_date']}"
            ) from e

        self.name = details["name"]
        self.load_data(start=start, end=end)
        self.min_date = self.data.Date.min()
        self.max_date = self.data.Date.max()

    def load_data(self, start=None, end=None):
        """
        Load statements for a monthly range [start, end] (inclusive), concatenate
        transactions, and compute running balances.

        start/end can be:
        - None (defaults: start=self.open_date, end=latest available)
        - tuple like (2024, 9) or ('2024', '09')
        - string like '2024-09', '09/2024', '2024/9', '202409', '2024-09-15'
        - datetime/date

        Raises FileNotFoundError if any month in the range is missing.
        Raises ValueError for balance inconsistencies.
        """
        index = build_account_file_index(self.acct_folder)
        if not index:
            raise FileNotFoundError(f"No statements found under {self.acct_folder}")
        validate_account_file_index(index)

        # Normalize index: years -> 'YYYY', months -> 'MM'
        nindex = {
            str(int(y)): {str(int(m)).zfill(2): files for m, files in months.items()}
            for y, months in index.items()
        }

        def _to_month_start(x):
            """Return a datetime at the first day of the month for diverse inputs."""
            if x is None:
                return None
            if isinstance(x, (datetime, date)):
                return datetime(x.year, x.month, 1)
            if isinstance(x, tuple) and len(x) == 2:
                y, m = x
                return datetime(int(y), int(m), 1)
            s = str(x)

            m1 = re.search(r"(?P<y>\d{4})\D+(?P<m>\d{1,2})", s)  # YYYY sep M
            if m1:
                return datetime(int(m1.group("y")), int(m1.group("m")), 1)

            m2 = re.search(r"(?P<m>\d{1,2})\D+(?P<y>\d{4})", s)  # M sep YYYY
            if m2:
                return datetime(int(m2.group("y")), int(m2.group("m")), 1)

            m3 = re.fullmatch(r"(\d{4})(\d{2})", s)  # YYYYMM
            if m3:
                return datetime(int(m3.group(1)), int(m3.group(2)), 1)

            raise ValueError(
                "start/end must be like '2024-09', '09/2024', '202409', "
                "'2024-09-15', a (YYYY, M) tuple, or a datetime/date."
            )

        # Defaults: start from account open date; end at latest available statement
        start_dt = _to_month_start(start) or datetime(
            self.open_date.year, self.open_date.month, 1
        )
        latest_year = max(nindex.keys(), key=lambda yy: int(yy))
        latest_month = max(nindex[latest_year].keys(), key=lambda mm: int(mm))
        latest_dt = datetime(int(latest_year), int(latest_month), 1)
        end_dt = _to_month_start(end) or latest_dt

        if end_dt < start_dt:
            raise ValueError(
                f"'end' ({end_dt:%Y-%m}) must be >= 'start' ({start_dt:%Y-%m})."
            )

        def _month_iter(sdt, edt):
            y, m = sdt.year, sdt.month
            while (y < edt.year) or (y == edt.year and m <= edt.month):
                yield str(y), str(m).zfill(2)
                if m == 12:
                    y, m = y + 1, 1
                else:
                    m += 1

        periods = list(_month_iter(start_dt, end_dt))

        # Ensure no gaps: every month must exist
        missing = [
            f"{y}/{m}" for (y, m) in periods if y not in nindex or m not in nindex[y]
        ]
        if missing:
            avail = ", ".join(
                f"{yy}/{mm}"
                for yy in sorted(nindex, key=int)
                for mm in sorted(nindex[yy], key=int)
            )
            raise FileNotFoundError(
                f"Missing statements for: {', '.join(missing)}. Available: {avail}"
            )

        # Load, validate month totals + continuity, and concat
        self.metadata_by_period = {}
        frames = []
        prev_end = None
        first_beginning = None

        for y, m in periods:
            files = nindex[y][m]
            meta = load_metadata_from_json(str(files["metadata"]))
            df = load_data_from_csv(str(files["transactions"])).copy()
            df["StatementYear"] = int(y)
            df["StatementMonth"] = int(m)

            # Month total check
            month_sum = df["Amount"].sum()
            if (
                meta["Beginning balance"] + month_sum - meta["Ending balance"]
            ).copy_abs() > Decimal("0.01"):
                raise ValueError(
                    f"Final balance doesn't match for {y}/{m}; "
                    f"expected {meta['Ending balance']}, "
                    f"got {meta['Beginning balance'] + month_sum}"
                )

            # Continuity check with previous month
            if prev_end is not None and (
                meta["Beginning balance"] - prev_end
            ).copy_abs() > Decimal("0.01"):
                raise ValueError(
                    f"Balance continuity error: {y}/{m} beginning balance {meta['Beginning balance']} "
                    f"does not match previous ending balance {prev_end}"
                )

            if first_beginning is None:
                first_beginning = meta["Beginning balance"]
            prev_end = meta["Ending balance"]

            self.metadata_by_period[(y, m)] = meta
            frames.append(df)

        # Combine & sort (stable) once
        self.data = pd.concat(frames, ignore_index=True)
        self.data = self.data.sort_values(
            ["Date", "StatementYear", "StatementMonth"], kind="stable"
        ).reset_index(drop=True)

        # Running balance in pure Decimal arithmetic
        running = []
        total = first_beginning
        for amt in self.data["Amount"]:
            total = total + amt
            running.append(total)
        self.data["Balance"] = running

        # Convenient aggregate metadata for the loaded window
        self.metadata = {
            "Beginning balance": first_beginning,
            "Ending balance": prev_end,
            "start_period": f"{start_dt.year}/{str(start_dt.month).zfill(2)}",
            "end_period": f"{end_dt.year}/{str(end_dt.month).zfill(2)}",
        }

    def get_balance(self, year, month, day, return_warnings=False):
        """
        Retrieves the balance for a given date and set of accounts.

        Args:
            year (int): The year for which the balance is required.
            month (int): The month for which the balance is required.
            day (int): The day for which the balance is required.

        Returns:
            balance (Decimal): The total balance for the given date.
        """
        date_obj = datetime(year=year, month=month, day=day)
        balance, warnings = get_col_val(
            df=self.data,
            col="Balance",
            date_col="Date",
            date_obj=date_obj,
            min_date=self.min_date,
            max_date=self.max_date,
            acct_name=self.name,
            return_warnings=True,
        )
        if return_warnings:
            return balance, warnings
        else:
            return balance

    def get_transactions(self, start_date, end_date):
        data, warnings = get_df_range(
            df=self.data,
            start_date_obj=start_date,
            end_date_obj=end_date,
            acct_name="this dataframe",
            return_warnings=True,
        )
        return data, warnings

    def __str__(self):
        return f"Account({self.name})"

    def __repr__(self):
        return f"Account({self.name})"
