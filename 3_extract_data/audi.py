import json
import os
from calendar import monthrange
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from commons import _deterministic_id


def create_metadata(beginning_balance, ending_balance, acct_number, date, savepath):
    # Ensure directory exists
    os.makedirs(os.path.dirname(savepath), exist_ok=True)

    # Ensure date is in mm-dd-yyyy format
    if isinstance(date, datetime):
        date_str = date.strftime("%m-%d-%Y")
    else:
        # assume it's a string that needs formatting
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            date_str = date_obj.strftime("%m-%d-%Y")
        except ValueError:
            # if already mm-dd-yyyy, keep as is
            date_str = date

    metadata = {
        "Beginning balance": str(beginning_balance),
        "Ending balance": str(ending_balance),
        "date": date_str,
        "Account number": str(acct_number),
    }

    with open(savepath, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)


def create_transactions(date, amount, current_balance, savepath):
    if date is None:
        df = pd.DataFrame(
            columns=[
                "Date",
                "Description",
                "Amount",
                "Balance",
                "Category",
                "Flow Type",
                "Account",
                "Source",
                "TransactionID",
            ]
        )
    else:
        data = {
            "Date": date,
            "Description": "Audi Payment",
            "Amount": amount,
            "Balance": current_balance,
            "Category": "Undefined",
            "Flow Type": "Undefined",
            "Account": "Audi",
            "Source": "/home/francisco/Documents/Finances/Statements/Accounts/Audi/Statements/Amortization Statement.pdf",
        }
        df = pd.DataFrame([data])
        acct = "Audi"
        df["TransactionID"] = df.apply(lambda r: _deterministic_id(r, acct), axis=1)
    df.to_csv(savepath / "transactions.csv", index=False)


def to_money(x):
    return x if isinstance(x, Decimal) else Decimal(str(x))


def create_default_audi_month_year(
    month: int, year: int, audi_path, amortization_df: pd.DataFrame
):

    df = amortization_df.copy()

    for col in ("Amount", "Interest", "Principal"):
        df[col] = df[col].apply(to_money)

    # Totals across the entire schedule
    total_interest = sum(df["Interest"], Decimal(0))
    total_principal = sum(df["Principal"], Decimal(0))
    total_scheduled = total_interest + total_principal

    # Parse dates & sort once
    df["Payment Date"] = pd.to_datetime(
        df["Payment Date"], format="%m/%d/%Y", errors="raise"
    )
    df = df.sort_values("Payment Date", kind="mergesort").reset_index(drop=True)

    # One cumulative sum over payments
    df["cum_paid"] = df["Amount"].cumsum()

    # Helper: cumulative paid as of END of (year, month),
    # plus the amount paid within that month (0 if none).
    def _cum_paid_month_end(y: int, m: int):
        tz = df["Payment Date"].dt.tz  # preserve timezone if present
        month_start = pd.Timestamp(year=y, month=m, day=1, tz=tz)
        month_end = pd.Timestamp(year=y, month=m, day=monthrange(y, m)[1], tz=tz)

        # --- cumulative as of month end ---
        mask_upto_eom = df["Payment Date"] <= month_end
        if mask_upto_eom.any():
            last_date = df.loc[mask_upto_eom, "Payment Date"].max()
            last_idx = df.index[df["Payment Date"] == last_date]
            cum_as_of_eom = df.at[last_idx[-1], "cum_paid"]
        else:
            # nothing paid at all yet
            last_date = month_end  # use EOM as "reference" date when none exist
            cum_as_of_eom = Decimal(0)

        # --- cumulative just before the month starts ---
        mask_before_month = df["Payment Date"] < month_start
        if mask_before_month.any():
            prev_last = df.loc[mask_before_month, "Payment Date"].max()
            prev_last_idx = df.index[df["Payment Date"] == prev_last]
            cum_before_month = df.at[prev_last_idx[-1], "cum_paid"]
        else:
            cum_before_month = Decimal(0)

        # amount paid within the target month
        paid_in_month = cum_as_of_eom - cum_before_month

        # Return:
        #  - cumulative paid as of month end
        #  - last actual payment date on/before month end (or month_end if none exist)
        #  - total paid within the month (Decimal 0 if none)
        return cum_as_of_eom, last_date, paid_in_month

    # Use it
    paid_to_target, payment_date, paid_in_month = _cum_paid_month_end(year, month)

    # Previous month (wrap year if needed)
    prev_month = 12 if month == 1 else month - 1
    prev_year = year - 1 if month == 1 else year
    paid_to_prev, _, _ = _cum_paid_month_end(prev_year, prev_month)
    if paid_to_prev is None:
        paid_to_prev = Decimal(0)

    # Balances

    current_balance = -total_scheduled + paid_to_target
    previous_balance = -total_scheduled + paid_to_prev
    last_day = monthrange(year, month)[1]
    create_metadata(
        previous_balance,
        current_balance,
        "audi",
        f"{month:02d}-{last_day:02d}-{year}",
        audi_path / f"Processed Data/{year}/{month:02d}/metadata.json",
    )
    create_transactions(
        date=(
            payment_date.strftime("%Y-%m-%d")
            if previous_balance != current_balance
            else None
        ),
        amount=paid_in_month,
        current_balance=current_balance,
        savepath=audi_path / f"Processed Data/{year}/{month:02d}",
    )


if __name__ == "__main__":
    # Assume that since december 2023 there has a 1098.27 payment every 23
    audi_path = Path("/home/francisco/Documents/Finances/Statements/Accounts/Audi")
    amortization_df = pd.read_csv(
        "/home/francisco/Documents/Finances/Statements/Accounts/Audi/Statements/Amortization.csv"
    )
    start = datetime(2023, 12, 1)
    end = datetime(2025, 9, 1)

    months = []
    current = start
    while current <= end:
        print((current.month, current.year))
        create_default_audi_month_year(
            current.month, current.year, audi_path, amortization_df
        )
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
