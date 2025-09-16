import os
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
from account import Account
from tqdm import tqdm


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


def get_month_range(report_date: str):
    """
    Given a string in the format 'MM-YYYY',
    return datetime objects for the start and end of that month.

    Example:
        '06-2025' -> (2025-06-01 00:00:00, 2025-06-30 23:59:59)
    """
    # Parse "MM-YYYY"
    month, year = map(int, report_date.split("-"))
    start_date = datetime(year, month, 1)

    # Calculate the next month's first day
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    # Subtract one second to get the last moment of the month
    end_date = next_month - timedelta(seconds=1)
    return start_date, end_date


def extract_transactions(report_date, accounts, project_path):
    start_date, end_date = get_month_range(report_date)
    folder = get_report_folder(report_date, project_path)

    frames = []
    for acct in accounts.values():
        # Check coverage
        if acct.max_date < end_date + timedelta(days=2):
            raise ValueError(f"{acct} does not extend enough past {end_date}")

        if start_date > acct.open_date and acct.min_date > start_date - timedelta(
            days=2
        ):
            raise ValueError(f"{acct} does not start early enough before {start_date}")

        # Get transactions
        df, warnings = acct.get_transactions(start_date, end_date)
        if warnings:
            print(f"Warnings from {acct}: {warnings}")
        frames.append(df)

    transactions = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not transactions.empty:
        transactions = transactions.sort_values(
            ["Date", "StatementYear", "StatementMonth"], kind="stable"
        ).reset_index(drop=True)

    # Update balance:
    balances = []
    for date_obj in transactions["Date"]:
        day = date_obj.day
        month = date_obj.month
        year = date_obj.year
        balance = Decimal(0)
        for acct in accounts.values():
            balance += acct.get_balance(year, month, day)
        balances.append(balance)
    transactions["Balance"] = balances

    transactions.to_csv(
        folder / "transactions.csv",
        index=False,
    )


if __name__ == "__main__":
    project_path = Path("/home/francisco/Documents/Finances/Statements")
    # accounts = os.listdir(project_path / "Accounts")
    accounts = [
        "Apple-5843",
        "Audi",
        "Chase-0402",
        "Chase-1010",
        "Chase-1600",
        "Chase-2425",
        "Chase-4106",
        "Chase-7593",
        "Chase-8021",
        "Discover-8384",
        "Discover-4589",
        "PNC-6552",
        "PNC-6579",
        "PNC-6587",
        "SoFi-8365",
        "SoFi-9806",
    ]
    for m in tqdm(range(4, 6)):
        extract_transactions(
            report_date=f"{m}-2025",
            accounts={
                acct: Account(project_path / f"Accounts/{acct}") for acct in accounts
            },
            project_path=project_path,
        )

    df = pd.read_csv(
        "/home/francisco/Documents/Finances/Statements/Monthly Reports/2025/05/transactions.csv"
    )
    df_accounts = set(df["Account"])
    expected_accounts = set(accounts)
    missing_accounts = expected_accounts - df_accounts

    if missing_accounts:
        raise ValueError(f"Missing accounts: {', '.join(map(str, missing_accounts))}")
