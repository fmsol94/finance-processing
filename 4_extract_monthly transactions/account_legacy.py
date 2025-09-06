import json
import os
from datetime import datetime
from decimal import Decimal

import pandas as pd


def convert_to_decimal(amt, amt_decimal_places=2):
    if isinstance(amt, str):
        amt = amt.replace("$", "").replace(",", "")
    return Decimal(
        "{:.{}f}".format(float(amt), amt_decimal_places)
        .replace("$", "")
        .replace(",", "")
    )


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


def load_data_from_csv(filepath, load_all_columns=False):
    if os.path.exists(filepath):
        if load_all_columns:
            data = pd.read_csv(filepath, index_col=False)
        else:
            data = pd.read_csv(filepath, index_col=False)[
                ["Date", "Description", "Amount", "Category"]
            ]
    else:
        raise FileNotFoundError(
            f"The CSV data associated with this statement '{filepath}' does not exist."
        )
    data["Amount"] = data["Amount"].apply(convert_to_decimal)
    data["Date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
    return data


class Statement:
    def __init__(self, statement_path, load_all_data_columns=False):
        self.statement_path = statement_path
        self.metadata = load_metadata_from_json(f"{statement_path}.json")
        self.month = self.metadata["date"].month
        self.year = self.metadata["date"].year
        self.data = load_data_from_csv(
            f"{statement_path}.csv", load_all_columns=load_all_data_columns
        )
        self.check_data()
        self.data = self.data.sort_values("Date")
        self.data["Balance"] = (
            self.data.Amount.cumsum() + self.metadata["Beginning balance"]
        )
        self.statement_id = "/".join(self.statement_path.split("/")[-2:])
        self.bank, statement_filename = self.statement_id.split("/")
        self.acct_number = statement_filename.split("_", 1)[0]

    def check_data(self):
        end_balance_data = (
            self.data["Amount"].sum() + self.metadata["Beginning balance"]
        )
        end_balance_metadata = self.metadata["Ending balance"]
        if abs(end_balance_data - end_balance_metadata) > 0.01:
            raise ValueError(
                f"Final balance doesn't match for statement '{self.statement_path}', it is supposed to be ${end_balance_metadata} and got ${end_balance_data}"
            )

    def __str__(self):
        return f"Statement({self.statement_id})"

    def __repr__(self):
        return f"Statement({self.statement_id})"


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


def is_statement_available(statement_path):
    if os.path.exists(f"{statement_path}.json") and os.path.exists(
        f"{statement_path}.csv"
    ):
        return True
    else:
        return False


class Account:
    def __init__(
        self,
        acct_number,
        open_date,
        nickname,
        bank,
        statement_paths,
        statement_class=None,
    ):
        if statement_class is None:
            self.statement_class = Statement
        else:
            self.statement_class = statement_class
        self.acct_number = str(acct_number)
        self.open_date = datetime.strptime(open_date, "%m/%Y")
        self.nickname = nickname
        self.bank = bank
        self.statement_paths = set(statement_paths)
        self.statements, self.data = self.load_statements()
        self.min_date = self.data.Date.min()
        self.max_date = self.data.Date.max()
        self.check_missing_months()

    def load_statements(self):
        statements = {}
        for path in self.statement_paths:
            if is_statement_available(statement_path=path):
                statement = self.statement_class(statement_path=path)
                statements[statement.statement_id] = statement

        data = []
        for key in sorted(statements):
            data.append(statements[key].data)

        if len(data):
            data = pd.concat(data)
        else:
            raise ValueError(
                f"There are no statements available for acct {self.acct_number}"
            )
        return statements, data

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
            acct_name=self.nickname,
            return_warnings=True,
        )
        if return_warnings:
            return balance, warnings
        else:
            return balance

    def check_missing_months(self):
        required_month_years_combinations = get_month_year_combinations(
            start_month=self.open_date.month,
            start_year=self.open_date.year,
            end_month=self.max_date.month,
            end_year=self.max_date.year,
        )

        available_month_years_combinations = set(
            (s.month, s.year) for s in self.statements.values()
        )

        missing_months_years_combinations = (
            required_month_years_combinations - available_month_years_combinations
        )
        n_missing = len(missing_months_years_combinations)
        if n_missing > 0:
            raise ValueError(
                f"The following {n_missing} combinations of months and years are missing in account {self.nickname}: {missing_months_years_combinations}"
            )

    def __str__(self):
        return f"Account({self.nickname}:{self.acct_number})"

    def __repr__(self):
        return f"Account({self.nickname}:{self.acct_number})"
