import datetime
import logging
import os
import re
import shutil
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
from commons import (
    _deterministic_id,
    convert_to_decimal,
    find_pdfs,
    load_statements_db,
    open_pdf,
    process_financial_df,
    save_dict_as_json,
    setup_logging,
)
from tqdm import tqdm


def parse_month_year(date_str: str) -> tuple[int, int]:
    # Parse the input string like "September 2024"
    dt = datetime.datetime.strptime(date_str, "%B %Y")
    return dt.month, dt.year


def apple_setup():
    path = Path("/home/francisco/Documents/Finances/Statements/Accounts/Apple-5843")
    statement_path = path / "Statements"
    files = list(statement_path.glob("*.pdf"))
    for file in files:
        m, y = parse_month_year(file.name.split(" - ")[-1].split(".")[0])
        dest_dir = statement_path / str(y) / f"{m:02d}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / file.name
        shutil.move(str(file), str(dest))
        print(f"Moved {file.name} to {dest}")


def find_pattern_index(text, pattern):
    index = text.find(pattern)
    return index


def detect_start_payments_table(text):
    pattern = "\nPayments\nDate Description Amount\n"
    index = find_pattern_index(text, pattern)
    return index


def detect_end_payments_table(text):
    pattern = "\nTotal payments"
    index = find_pattern_index(text, pattern)
    if index != -1:
        subtext = text[index + 1 :].split("\n")[0]
        index += len(subtext) + 1
    else:
        index = len(text)
    return index


def detect_start_transactions_table(text):
    pattern = "\nTransactions\nDate Description Daily Cash Amount\n"
    index = find_pattern_index(text, pattern)
    return index


def detect_end_transactions_table(text):
    patterns = ["\nTotal charges, credits and returns", "\nTotal Daily Cash"]
    indices = []
    for pattern in patterns:
        indices.append(find_pattern_index(text, pattern))
    index = max(indices)
    if index != -1:
        subtext = text[index + 1 :].split("\n")[0]
        index += len(subtext) + 1
    else:
        index = len(text)
    return index


def extract_payment_info(input_string):
    # Define the regex pattern for matching the date
    date_pattern = r"\d{2}/\d{2}/\d{4}|Total payments"

    # Check if the input starts with a date
    if not re.match(date_pattern, input_string):
        return None

    # Extract the date using regex
    date = re.search(date_pattern, input_string).group()

    # Split the input string by spaces
    desc_and_amount = input_string.split(" ", 1)[1]

    # Extract the description and amount from the split parts
    desc, amount = desc_and_amount.rsplit(" ", 1)

    # Return the extracted information as a dictionary
    return {"Date": date, "Description": desc, "Amount": amount}


def extract_payments_table(text):
    table = []
    st_index = detect_start_payments_table(text)
    if st_index != -1:
        end_index = detect_end_payments_table(text)
        table_rows = text[st_index:end_index].split("\n")
        for row in table_rows:
            extracted_info = extract_payment_info(row)
            if extracted_info is not None:
                table.append(extracted_info)
    table = [
        (
            {
                "Date": "N/A",
                "Description": "Total payments for this month",
                "Amount": re.findall(
                    r"\$[\d.]+", text[st_index:end_index].replace(",", "")
                )[-1],
            }
            if elem["Date"].startswith("Total payments")
            else elem
        )
        for elem in table
    ]
    return table


def extract_transaction_info(input_string):
    # Define the regex pattern for matching the date
    date_pattern = (
        r"\d{2}/\d{2}/\d{4}|Total Daily Cash|Total charges, credits and returns"
    )

    # Check if the input starts with a date
    if not re.match(date_pattern, input_string):
        return None

    # Extract the date using regex
    date = re.search(date_pattern, input_string).group()

    # Split the input string by spaces
    other_info = input_string.split(" ", 1)[1]

    # Extract the description and amount from the split parts
    desc, perc, cashback, amount = other_info.rsplit(" ", 3)

    # Return the extracted information as a dictionary
    return {
        "Date": date,
        "Description": desc,
        "Percentage": (
            perc if "%" in perc else "0"
        ),  # Changes to fix error where percentages and cashbacks are in the row below. We will ignore for now (it is only cents)
        "Cashback": cashback if "$" in cashback else "0",
        "Amount": amount,
    }


def extract_extra_daily_cash(input_string, previous_extracted_info, promo):
    desc, perc, amt = input_string.rsplit(" ", 2)
    extracted_info = {
        "Date": previous_extracted_info["Date"],
        "Description": desc,
        "Percentage": perc,
        "Cashback" if promo else "Amount": amt,
        "Amount" if promo else "Cashback": "$0.00",
    }
    return extracted_info


def extract_transactions_table(text):
    table = []
    st_index = detect_start_transactions_table(text)

    if st_index != -1:
        end_index = detect_end_transactions_table(text)
        table_rows = text[st_index:end_index].split("\n")
        for row in table_rows:
            if "credit adjustment" in row.lower():
                extracted_info = extract_payment_info(row)
                extracted_info["Percentage"] = 0
                extracted_info["Cashback"] = 0

            elif (
                "promo daily cash" in row.lower() or "daily cash at uber" in row.lower()
            ):
                extracted_info = extract_extra_daily_cash(
                    input_string=row, previous_extracted_info=table[-1], promo=True
                )

            elif "daily cash adjustment" in row.lower():
                extracted_info = extract_extra_daily_cash(
                    input_string=row, previous_extracted_info=table[-1], promo=False
                )
            else:
                extracted_info = extract_transaction_info(row)
            if extracted_info is not None:
                table.append(extracted_info)
    return table


def correct_last_rows_payments(payments_table):
    for item in payments_table:
        item["Percentage"] = "N/A"
        item["Cashback"] = "N/A"
        item["table_type"] = "Payments"
        if item["Date"].startswith("Total payments"):
            item["Date"] = "N/A"
            item["Description"] = "Total payments for this period"
    return payments_table


def correct_last_rows_transactions(transactions_table):
    for item in transactions_table:
        item["table_type"] = "Transactions"
        if item["Date"].startswith("Total Daily Cash"):
            item["Date"] = "N/A"
            item["Description"] = "Total Daily Cash this month"
            item["Percentage"] = "N/A"
            item["Cashback"] = item["Amount"]
            item["Amount"] = "N/A"
        if item["Date"] == "Total charges, credits and returns":
            item["Date"] = "N/A"
            item["Description"] = "Total charges, credits and returns"
            item["Percentage"] = "N/A"
            item["Cashback"] = "N/A"
    return transactions_table


# Metadata
def extract_amount(text):
    match = re.search(r"([-+]?\$[0-9.]+)", text)
    if match:
        # Extract the entire matched amount including the sign
        dollar_amount = float(match.group(1).replace("$", ""))
    else:
        raise ValueError("No dollar amount found in the line:", text)
    return convert_to_decimal(dollar_amount)


def extract_date(text):
    try:
        date_output = datetime.strptime(text, "as of %b %d, %Y")
    except Exception as e:
        raise Exception(f"Date was not properly extracted from text '{text}'")
    return date_output


def initial_metadata_check(metadata):
    if set(metadata.keys()) != {
        "previous_monthly_balance",
        "previous_total_balance",
        "total_balance",
    }:
        raise ValueError(
            f"Extracted keys for metadata do not match expected values: Extracted keys are {set(metadata.keys())} and expected were ['previous_monthly_balance', 'previous_total_balance', 'total_balance']"
        )
    for key in ["Amount", "Date"]:
        if (
            metadata["previous_monthly_balance"][key]
            != metadata["previous_total_balance"][key]
        ):
            if key == "Amount":
                if metadata["previous_monthly_balance"][key] != Decimal(
                    "0"
                ) or metadata["previous_total_balance"][key] >= Decimal("0"):
                    raise ValueError(
                        f"{key} from 'previous_monthly_balance' {metadata['previous_monthly_balance'][key]} is different than 'previous_total_balance' {metadata['previous_total_balance'][key]}, and they don't meet the condition of monthly = 0.00 and total < 0.00"
                    )  # Condition extracted from Oct 2022 statement.
            else:
                raise ValueError(
                    f"{key} from 'previous_monthly_balance' {metadata['previous_monthly_balance'][key]} is different than 'previous_total_balance' {metadata['previous_total_balance'][key]}"
                )


def extract_legacy_metadata(text):
    metadata = {}
    september_balance = {
        "Amount": Decimal("0.00"),
        "Date": datetime(2019, 9, 30),
    }
    october_balance = {
        "Amount": Decimal("3.12"),
        "Date": datetime(2019, 10, 31),
    }
    november_balance = {
        "Amount": Decimal("0.00"),
        "Date": datetime(2019, 11, 30),
    }
    if "Oct 11 — Oct 31, 2019" in text:
        metadata["previous_monthly_balance"] = september_balance
        metadata["previous_total_balance"] = september_balance
        metadata["total_balance"] = october_balance
    elif "Nov 1 — Nov 30, 2019" in text:
        metadata["previous_monthly_balance"] = october_balance
        metadata["previous_total_balance"] = october_balance
        metadata["total_balance"] = november_balance
    return metadata


def extract_metadata(text):
    if "Oct 11 — Oct 31, 2019" not in text and "Nov 1 — Nov 30, 2019" not in text:
        rows = text.split("\n")
        metadata = {}
        for n, row in enumerate(rows):
            if (
                "previous monthly balance" in row.lower()
                or "prior monthly balance" in row.lower()
            ):
                metadata["previous_monthly_balance"] = {
                    "Amount": extract_amount(row.replace(",", "")),
                    "Date": extract_date(rows[n + 1]),
                }
            elif (
                "previous total balance" in row.lower()
                or "prior total balance" in row.lower()
            ):
                metadata["previous_total_balance"] = {
                    "Amount": extract_amount(row.replace(",", "")),
                    "Date": extract_date(rows[n + 1]),
                }
            elif "total balance" in row.lower():
                metadata["total_balance"] = {
                    "Amount": extract_amount(row.replace(",", "")),
                    "Date": extract_date(rows[n + 1]),
                }
    else:
        metadata = extract_legacy_metadata(text)
    initial_metadata_check(metadata)
    return metadata


def apple_data_extract(data):
    metadata = extract_metadata(data[0])
    payments_table = []
    transactions_table = []
    for text in data:
        payments_table += extract_payments_table(text)
        transactions_table += extract_transactions_table(text)
    payments_table = correct_last_rows_payments(payments_table)
    transactions_table = correct_last_rows_transactions(transactions_table)
    df = pd.DataFrame(payments_table + transactions_table)[
        ["Date", "Description", "Percentage", "Cashback", "Amount", "table_type"]
    ]
    return df, metadata


# Table processing:
def check_totals(df, file):
    try:
        special_rows = [
            "Total Daily Cash",
            "Total charges, credits and returns",
            "Total payments",
        ]
        totals = {}
        if df.Date.isna().sum() != 3:
            raise Exception(
                f"Data extracted in {file} has a number of nan values in 'Date' column different than 3."
            )
        for desc in special_rows:
            row = df[df.Description.str.contains(desc)]
            if not row.shape[0] == 1:
                raise Exception(
                    f"Data extracted in '{file}' does not contain row for '{desc}'."
                )
            if row.Date.iloc[0] == row.Date.iloc[0]:
                raise Exception(
                    f"Data for row '{desc}' in dataframe '{file}' is not nan."
                )
            try:
                total = (
                    row["Cashback" if desc == "Total Daily Cash" else "Amount"]
                    .iloc[0]
                    .replace("$", "")
                    .replace(",", "")
                )
                totals[desc] = Decimal(total)
            except Exception as e:
                raise Exception(
                    f"Unable to convert '{total}' to decimal in row '{desc}' from file '{file}'"
                )

        def is_float(value):
            try:
                float(value)
                return True
            except ValueError:
                return False

        process_amt = lambda amount: amount.replace("$", "").replace(",", "")
        str2dec = lambda amount: (
            Decimal(process_amt(str(amount)))
            if is_float(process_amt(str(amount)))
            else Decimal("0")
        )
        df = df.dropna(subset="Date")
        payments_df = df[df.table_type == "Payments"]
        payments_df.loc[:, "Amount"] = payments_df.Amount.apply(str2dec)
        transactions_df = df[df.table_type == "Transactions"]
        transactions_df.loc[:, "Cashback"] = transactions_df.Cashback.apply(str2dec)
        transactions_df.loc[:, "Amount"] = transactions_df.Amount.apply(str2dec)
        if (
            not totals["Total charges, credits and returns"]
            == transactions_df.Amount.sum()
        ):
            raise Exception(
                "Total charges, credits, and returns do not match transaction amounts."
            )
        if not -totals["Total payments"] == payments_df.Amount.sum():
            raise Exception("Total payments do not match payment amounts.")
        if not totals["Total Daily Cash"] == transactions_df.Cashback.sum():
            raise Exception("Total daily cash do not match transactions cashback.")
    except Exception as e:
        raise Exception(f"Exception occurred in file {file}: {e}")


def is_special_row(row_description):
    special_rows = [
        "Total Daily Cash",
        "Total charges, credits and returns",
        "Total payments",
    ]
    for desc in special_rows:
        if desc in row_description:
            output = True
            break
        else:
            output = False

    return output


def apple_data_process(df, metadata, filepath):
    df.Date = df.Date.replace("N/A", np.nan)

    check_totals(df, filepath)

    special_rows_bool = df.Description.apply(is_special_row)

    # Initialize metadata
    metadata_df = df[special_rows_bool]
    for _, row in metadata_df.iterrows():
        if "Total Daily Cash" in row["Description"]:
            metadata["total_daily_cash"] = convert_to_decimal(row["Cashback"])
        elif "Total charges, credits and returns" in row["Description"]:
            metadata["total_charges_credits_returns"] = convert_to_decimal(
                row["Amount"]
            )
        elif "Total payments" in row["Description"]:
            metadata["total_payments"] = convert_to_decimal(row["Amount"])
        else:
            raise ValueError(
                f"The description of this in metadata_df does not fall in any of the expected categories: {row['Description']}"
            )

    # Initialize data
    data = process_financial_df(
        df=df[~special_rows_bool].copy().replace("N/A", "0"),
        date_cols=["Date"],
        date_formats=["%m/%d/%Y"],
        amt_cols=["Cashback", "Amount"],
    )

    # Check metadata
    calculated_total = (
        metadata["previous_total_balance"]["Amount"]
        + metadata["total_charges_credits_returns"]
        - metadata["total_payments"]
    )
    extracted_total = metadata["total_balance"]["Amount"]
    if calculated_total != extracted_total:
        raise ValueError(
            f"Extracted metadata is not consistent for file {filepath}. Calculated total: ${calculated_total}, Extracted total: ${extracted_total}"
        )

    metadata["Beginning balance"] = metadata["previous_total_balance"][
        "Amount"
    ]  # This is total based on what we see in statement October 2022.
    metadata["Ending balance"] = metadata["total_balance"]["Amount"]
    metadata["date"] = metadata["total_balance"]["Date"]
    metadata["Account number"] = "5843"

    # Calculate balance col
    data["Balance"] = data.Amount.cumsum() + metadata["Beginning balance"]
    end_balance = metadata["Ending balance"]
    if abs(data["Balance"].iloc[-1] - end_balance) > 0.01:
        raise Exception(
            f"Final balance doesn't match for {filepath}, it is supposed to be {end_balance} and got {data['Balance'].iloc[-1]}"
        )
    output_keys = [
        "total_payments",
        "total_daily_cash",
        "total_charges_credits_returns",
        "Beginning balance",
        "Ending balance",
        "date",
        "Account number",
    ]
    for field in ["Amount", "Balance"]:
        data[field] = -data[field]

    for field in [
        "Beginning balance",
        "Ending balance",
    ]:
        metadata[field] = -metadata[field]

    return data, {key: metadata[key] for key in output_keys}


def process_apple_pdf(filepath, save_filepath):
    data = open_pdf(pdf_path=filepath)
    df, metadata = apple_data_extract(data)
    data, metadata = apple_data_process(df, metadata, filepath)
    processed_statements = [
        {
            "account_number": metadata["Account number"],
            "month": metadata["date"].month,
            "year": metadata["date"].year,
            "raw_documents_path": filepath,
            "processed_documents_path": save_filepath,
        }
    ]
    os.makedirs(save_filepath, exist_ok=True)
    save_dict_as_json(data=metadata, filepath=save_filepath / "metadata.json")
    data["Category"] = "Undefined"
    data["Flow Type"] = "Undefined"
    acct = metadata.get("Account number", "")
    data["TransactionID"] = data.apply(lambda r: _deterministic_id(r, acct), axis=1)
    data["Account"] = "Apple-5843"
    data["Source"] = filepath
    data.to_csv(save_filepath / "transactions.csv", index=False)
    return processed_statements


def process_apple(apple_folder: str):
    """
    Process Apple PDF statements with logging and tqdm progress tracking.
    """
    logging.info(f"Starting Apple statement processing for {apple_folder}")
    folder_path = Path(apple_folder) / "Statements"

    try:
        statements_db = load_statements_db(Path(apple_folder))
        logging.info(f"Loaded statements DB from {folder_path.parent}")
    except Exception as e:
        logging.error(f"Failed to load statements DB: {e}", exc_info=True)
        return

    try:
        files = find_pdfs(folder_path)
        logging.info(f"Found {len(files)} PDF files in {folder_path}")
    except Exception as e:
        logging.error(f"Failed to find PDFs in {folder_path}: {e}", exc_info=True)
        return

    unprocessed_files = {str(file) for file in files} - set(
        statements_db["raw_documents_path"]
    )
    unprocessed_files = [Path(file) for file in unprocessed_files]
    # unprocessed_files = files
    logging.info(f"{len(unprocessed_files)} unprocessed PDF files.")
    new_statements = []
    for file in tqdm(unprocessed_files, desc="Processing PDFs", unit="file"):
        try:
            filepath = file
            path = file.parent

            before, after = str(path).rsplit("Statements", 1)
            save_filepath = Path(before + "Processed Data" + after)

            logging.info(f"Processing file {filepath} -> {save_filepath}")
            processed_statements = process_apple_pdf(filepath, save_filepath)
            new_statements += processed_statements

        except Exception as e:
            logging.error(f"Error processing file {file}: {e}", exc_info=True)
    statements_db = pd.concat(
        [statements_db, pd.DataFrame(new_statements)], ignore_index=True
    )
    statements_db["account_number"] = statements_db["account_number"].astype(str)
    statements_db.drop_duplicates().to_csv(
        Path(apple_folder) / "statements_db.csv", index=False
    )
    logging.info("Finished processing all Apple statements.")


if __name__ == "__main__":
    # Instructions, just drop pdf statement in correspondent folder
    setup_logging()
    process_apple("/home/francisco/Documents/Finances/Statements/Accounts/Apple-5843")
