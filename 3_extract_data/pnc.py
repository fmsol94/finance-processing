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
    save_dict_as_json,
)
from tqdm import tqdm


def extract_acct_n(data):
    lines = data[0].splitlines()
    pattern = r"Primary account number:\s(?:\d{2}|X{2})-(?:\d{4}|X{4})-(\d{4})"

    acct_n = [m.group(1) for s in lines if (m := re.search(pattern, s))]
    acct_n = set(acct_n)

    if len(acct_n) > 1:
        raise ValueError(f"Multiple account numbers detected: {acct_n}")

    acct_n = acct_n.pop() if acct_n else None

    if acct_n not in ["6587", "6552", "6579"]:
        raise ValueError(f"Invalid account number '{acct_n}'")
    return acct_n


def extract_month_year(s: str):
    s = re.sub(r"\(\d+\)$", "", s)
    date_str = "_".join(s.split("_")[-3:]).strip()

    dt = datetime.strptime(date_str, "%b_%d_%Y")

    return dt.month, dt.year


def pnc_setup(project_path, historical_pnc_path):
    files = find_pdfs(historical_pnc_path)
    for file in tqdm(files):
        data = open_pdf(file)
        try:
            acct_n = extract_acct_n(data)
            month, year = extract_month_year(file.stem)
        except ValueError as e:
            raise ValueError(f"Error in file {file.name}: {e}") from e
        dest_dir = (
            project_path / f"PNC-{acct_n}" / "Statements" / str(year) / f"{month:02d}"
        )
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / file.name
        shutil.move(str(file), str(dest))
        print(f"Moved {file.name} to {dest}")


def extract_balance_summary(text):
    """
    Extract the balance summary details from the given text.

    Args:
        text (str): Text content of a PDF page.

    Returns:
        dict: Extracted balance summary details.
    """
    text = text.replace(",", "")
    pattern = r"balance\s*summary\nbeginning\s*deposits\s*and\s*checks\s*and\s*other\s*ending\nbalance\s*other\s*additions\s*deductions\s*balance\n(\d*\.?\d+)\s*(\d*\.?\d+)\s*(\d*\.?\d+)\s*(\d*\.?\d+)\naverage\s*monthly\s*charges\nbalance\s*and\s*fees\n(\d*\.?\d+)\s*(\d*\.?\d+)"
    match = re.search(pattern, text, re.IGNORECASE)

    if match:
        return {
            "Beginning balance": float(match.group(1)),
            "Deposits and other additions": float(match.group(2)),
            "Checks and other deductions": float(match.group(3)),
            "Ending balance": float(match.group(4)),
            "Average monthly balance": float(match.group(5)),
            "Charges and fees": float(match.group(6)),
        }
    else:
        return None


def generate_checks_table(data_list):
    # Extract the relevant information using regex
    date = []
    amount = []
    description = []
    for line in data_list:
        line = line.replace(",", "")
        if re.search(r"\d{2}\/\d{2}", line) and re.search(r"\d+\.\d{2}", line):
            amount += re.findall(r"\d+\.\d{2}", line)
            date_desc = re.findall(r"(\d{2}/\d{2}) (\S+)", line)
            date += [pair[0] for pair in date_desc]
            description += [f"Check with number {pair[1]}" for pair in date_desc]

    # Create a DataFrame using the extracted info
    df = pd.DataFrame({"Date": date, "Amount": amount, "Description": description})

    return df


def detect_table_headers(rows):
    """
    Detect the table headers in the given rows of text.

    Args:
        rows (list of str): The rows of text to search for table headers.

    Returns:
        dict: Detected table headers with their corresponding row indices.
    """
    table_headers = [
        "Checks and Substitute Checks",
        "Deposits and Other Additions",
        "Banking/Debit Card Withdrawals and Purchases",
        "Banking/Check Card Withdrawals and Purchases",
        "Online and Electronic Banking Deductions",
        "Other Deductions",
        "Daily Balance Detail",
    ]
    output = {}
    for n, row in enumerate(rows):
        for header in table_headers:
            if header in row:
                if header in output:
                    output[header].append(n)
                else:
                    output[header] = [n]
    values = [item for sublist in output.values() for item in sublist]
    mod_output = {}
    for n, val in enumerate(values):
        key = [k for k, v in output.items() if val in v][0]
        next_value = values[n + 1] if n + 1 < len(values) else len(rows)
        if key in mod_output:
            mod_output[key].append((val, next_value))
        else:
            mod_output[key] = [(val, next_value)]
    return mod_output


def process_list_to_df(lst):
    """
    Convert a list of extracted table data into a Pandas DataFrame.

    Args:
        lst (list of str): List of strings representing rows of a table.

    Returns:
        DataFrame: DataFrame with columns Date, Amount, and Description.
    """
    processed_data = []
    for i in range(len(lst)):
        # Check if the line starts with a date pattern
        if re.match(r"\d{2}\/\d{2}", lst[i]):
            # Split the line into date, amount, and description
            data = lst[i].split(maxsplit=2)
            # If the next line doesn't start with a date, it's a continuation of the current line.
            if i < len(lst) - 1 and not re.match(r"\d{2}\/\d{2}", lst[i + 1]):
                data[-1] += "\n" + lst[i + 1]
            processed_data.append(data)

    df = pd.DataFrame(processed_data, columns=["Date", "Amount", "Description"])
    return df


def pnc_data_extract(data):
    tables = []
    title = None
    balance_summary_found = False

    for page in data:
        # Extract the text from the page
        text = page
        # Split the text into rows based on newlines
        rows = text.split("\n")
        if title is None:
            title = rows[0]

        if not balance_summary_found:
            balance_summary = extract_balance_summary(text)
            if balance_summary:
                balance_summary_found = True

        output = detect_table_headers(rows)
        for th, t_idx_list in output.items():
            if th != "Daily Balance Detail":
                if th == "Checks and Substitute Checks":
                    for t_idx in t_idx_list:
                        table = generate_checks_table(rows[t_idx[0] : t_idx[1]])
                        table["table_header"] = th
                        tables.append(table)
                else:
                    for t_idx in t_idx_list:
                        table = process_list_to_df(rows[t_idx[0] : t_idx[1]])
                        table["table_header"] = th
                        tables.append(table)
    if tables:
        data = pd.concat(tables)
    else:
        data = pd.DataFrame([])
    return title, data, balance_summary


def extract_account(title):
    """
    Extract account type from the given title.

    Args:
        title (str): The title to extract account type from.

    Returns:
        str: Extracted account type.
    """
    accounts = ["Reserve", "Spend", "Growth"]
    for acct in accounts:
        if acct in title:
            return acct
    raise Exception(f"Title {title} doesn't contain any of the account names!")


def extract_date(file_path):
    """
    Extract date from the given file path.

    Args:
        file_path (str): File path from which to extract date.

    Returns:
        str: Extracted date in the format "Month_Day_Year".
    """
    # Define the regular expression pattern
    pattern = r"Statement_(\w+_\d+_\d{4})"

    # Extract the date using regex
    match = re.search(pattern, file_path)

    if match:
        date = match.group(1)
    else:
        raise Exception("No date found in the file path.")
    return date


def process_metadata(metadata, account_n, filepath):
    acct_type = extract_account(account_n)
    date_str = extract_date(str(filepath))
    metadata["date"] = datetime.strptime(date_str, "%b_%d_%Y")
    fields_to_convert = [
        "Beginning balance",
        "Deposits and other additions",
        "Checks and other deductions",
        "Ending balance",
        "Average monthly balance",
        "Charges and fees",
    ]
    for field in fields_to_convert:
        metadata[field] = convert_to_decimal(metadata[field])
    metadata["acct_type"] = acct_type
    return metadata


def fix_signs(row):
    addition_headers = {"Deposits and Other Additions"}
    deduction_headers = {
        "Banking/Debit Card Withdrawals and Purchases",
        "Banking/Check Card Withdrawals and Purchases",
        "Online and Electronic Banking Deductions",
        "Checks and Substitute Checks",
        "Other Deductions",
    }
    amount = row["Amount"]
    if isinstance(amount, str):
        amount = Decimal(amount.replace(",", ""))
    if not isinstance(amount, Decimal):
        raise Exception(f"Invalid input type, {type(amount)} is not supported.")

    if row["table_header"] in addition_headers:
        if amount < 0:
            amount = amount
    elif row["table_header"] in deduction_headers:
        if amount > 0:
            amount = -amount
    else:
        raise Exception(f"Table header {row['table_header']} is not valid!")
    return amount


def check_files_aggregate(data, metadata, filepath):
    """
    Validates the aggregation of the data for a given key.

    Args:
        key (str): Key to identify which file/data to validate.

    Raises:
        Exception: If there's a mismatch or error in the aggregation.
    """
    try:
        table_headers = {
            "Banking/Debit Card Withdrawals and Purchases",
            "Banking/Check Card Withdrawals and Purchases",
            "Deposits and Other Additions",
            "Online and Electronic Banking Deductions",
            "Checks and Substitute Checks",
            "Other Deductions",
        }

        data["Amount"] = (
            data.Amount.apply(str).str.replace(",", "").apply(lambda x: Decimal(x))
        )

        data["Date"] = data["Date"] + f"/{metadata['date'].year}"
        data["Date"] = pd.to_datetime(data["Date"], format="%m/%d/%Y")

        if set(data.table_header).issubset(table_headers):
            aggregates = data.groupby("table_header")["Amount"].apply(np.sum)
        else:
            raise Exception(
                f"These table headers were not considered before: {set(data.table_header) - table_headers}"
            )

        global_additions = metadata["Deposits and other additions"]
        data_additions = aggregates.get("Deposits and Other Additions", Decimal(0))
        if global_additions != data_additions:
            raise Exception(
                "Global additions to the account do not match with the data. Data deductions: {data_additions}, Metadata deductions: {global_additions}"
            )

        global_deductions = metadata["Checks and other deductions"]
        data_deductions = (
            aggregates.get("Banking/Debit Card Withdrawals and Purchases", Decimal(0))
            + aggregates.get("Online and Electronic Banking Deductions", Decimal(0))
            + aggregates.get("Checks and Substitute Checks", Decimal(0))
            + aggregates.get("Banking/Check Card Withdrawals and Purchases", Decimal(0))
            + aggregates.get("Other Deductions", Decimal(0))
        )
        if global_deductions != data_deductions:
            raise Exception(
                f"Global deductions to the account do not match with the data. Data deductions: {data_deductions}, Metadata deductions: {global_deductions}"
            )
    except Exception as e:
        raise Exception(f"File {filepath} failed: {e}")


def pnc_data_process(filepath, account_n, metadata, data):
    metadata = process_metadata(metadata, account_n, filepath)
    if data.shape[0] == 0:
        data = pd.DataFrame(
            [
                {
                    "Date": metadata["date"].strftime("%m/%d"),
                    "Description": "No monthly activity",
                    "Amount": Decimal("0.00"),
                    "table_header": "Other Deductions",
                }
            ]
        )
    check_files_aggregate(data, metadata, filepath)
    data["Amount"] = data.apply(fix_signs, axis=1)
    if set(data.Date.apply(lambda date: date.month)) == {1, 12}:
        # Fix year error
        data["Date"] = data.Date.apply(
            lambda date: date.replace(year=date.year - 1) if date.month == 12 else date
        )

    data = data.sort_values("Date")
    data["Balance"] = data.Amount.cumsum() + metadata["Beginning balance"]
    end_balance = metadata["Ending balance"]
    if abs(data["Balance"].iloc[-1] - end_balance) > 0.01:
        raise Exception(
            f"Final balance doesn't match for {filepath}, it is supposed to be {end_balance} and got {data['Balance'].iloc[-1]}"
        )
    data = data.reset_index(drop=True)
    acct_numbers = {"Growth": "6587", "Reserve": "6579", "Spend": "6552"}
    metadata["Account number"] = acct_numbers.get(metadata["acct_type"])
    if metadata["Account number"] is None:
        raise ValueError(f"Unsupported account type: {metadata['acct_type']}")
    return data, metadata


def process_pnc_statement(filepath, save_filepath):
    data = open_pdf(filepath)

    account_n, data, metadata = pnc_data_extract(data)
    data, metadata = pnc_data_process(filepath, account_n, metadata, data)

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
    data["Account"] = f"PNC-{metadata['Account number']}"
    data["Source"] = filepath
    data.to_csv(save_filepath / "transactions.csv", index=False)
    return processed_statements


def process_pnc(statements_folder):

    acct_folder = statements_folder.parent

    statements_db = load_statements_db(Path(acct_folder))
    logging.info(f"Loaded statements DB from {acct_folder}")

    files = find_pdfs(statements_folder)
    logging.info(f"Found {len(files)} PDF files in {statements_folder}")

    unprocessed_files = {str(file) for file in files} - set(
        statements_db["raw_documents_path"]
    )
    unprocessed_files = [Path(file) for file in unprocessed_files]
    # unprocessed_files = files
    logging.info(f"{len(unprocessed_files)} unprocessed PDF files.")
    new_statements = []
    for file in tqdm(unprocessed_files, desc="Processing PDFs", unit="file"):
        filepath = file
        path = file.parent

        before, after = str(path).rsplit("Statements", 1)
        save_filepath = Path(before + "Processed Data" + after)

        logging.info(f"Processing file {filepath} -> {save_filepath}")
        processed_statements = process_pnc_statement(filepath, save_filepath)
        new_statements += processed_statements
    statements_db = pd.concat(
        [statements_db, pd.DataFrame(new_statements)], ignore_index=True
    )
    statements_db["account_number"] = statements_db["account_number"].astype(str)
    statements_db.drop_duplicates().to_csv(
        Path(acct_folder) / "statements_db.csv", index=False
    )
    logging.info(f"Finished processing all PNC statements.")


if __name__ == "__main__":
    project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts/")
    # historical_pnc_path = Path(
    #     "/home/francisco/Documents/Finances/Statements/Historical data/PNC/"
    # )
    # pnc_setup(project_path, historical_pnc_path)
    for acct in ["6552", "6579", "6587"]:
        pnc_path = project_path / f"PNC-{acct}/Statements"
        process_pnc(pnc_path)
