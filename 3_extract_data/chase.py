import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from commons import (
    convert_to_decimal,
    find_pdfs,
    load_statements_db,
    open_pdf,
    save_dict_as_json,
    setup_logging,
)
from tqdm import tqdm


def extract_beginning_end_dates(input_string):
    # Define a regular expression pattern to match date strings
    date_pattern = r"(\w+)\s*(\d{2}),(\d{4})"

    # Find all matching dates in the input string
    dates = re.findall(date_pattern, input_string)
    n_detected_dates = len(dates)

    if len(dates) != 2:
        raise ValueError(
            f"Number of detected dates is not 2, detected {n_detected_dates}"
        )

    # Convert the date strings to datetime objects
    datetime_objs = []
    for date_str in dates:
        try:
            month, day, year = date_str
            date_string = f"{month} {day}, {year}"
            datetime_obj = datetime.strptime(date_string, "%B %d, %Y")
            datetime_objs.append(datetime_obj)
        except Exception as e:
            raise ValueError(
                f"Date string {date_str} was not possible to convert into datetime object. Exception: {e}"
            ) from e

    if datetime_objs[1] > datetime_objs[0]:
        ending_date = datetime_objs[1]
        beginning_date = datetime_objs[0]
    else:
        raise ValueError(
            f"The second extracted date '{datetime_objs[1].strftime('%b %d, %Y')}' is before the first extracted date '{datetime_objs[0].strftime('%b %d, %Y')}', please review why."
        )

    return beginning_date, ending_date


def extract_dollar_amount(s):
    # Regular expression pattern for dollar amounts
    pattern = r"[-+]?\$\d{1,3}(?:,\d{3})*\.?\d{0,2}"
    match = re.search(pattern, s)
    if match:
        output = match.group()
        try:
            output = convert_to_decimal(output)
        except Exception as e:
            raise ValueError(
                f"Dollar amount {output} was not possible to convert into decimal. Exception: {e}"
            ) from e
    else:
        raise ValueError(f"Dollar amount was not detected in input string '{s}'")
    return output


def calculate_datetime_middle_point(dt1, dt2):
    if dt1 > dt2:
        dt1, dt2 = dt2, dt1  # Ensure dt1 is the earlier datetime

    time_difference = dt2 - dt1
    middle_point = dt1 + time_difference / 2

    return middle_point


def extract_chase_checking_metadata(lines):
    fields = [
        {
            "fieldline_starts": ["beginningbalance"],
            "field_name": "Beginning balance",
        },
        {"fieldline_starts": ["endingbalance"], "field_name": "Ending balance"},
    ]
    metadata = {}
    for line in lines:
        if "through" in line and (
            "beginning_date" not in metadata or "ending_date" not in metadata
        ):
            beginning_date, ending_date = extract_beginning_end_dates(
                line.replace("through", "")
            )
            metadata["beginning_date"] = beginning_date
            metadata["ending_date"] = ending_date
        for field in fields:
            if (
                any(line.startswith(start) for start in field["fieldline_starts"])
                and field["field_name"] not in metadata
            ):
                metadata[field["field_name"]] = extract_dollar_amount(line)

    dollar_amount_fields = [
        "Beginning balance",
        "Ending balance",
    ]
    required_fields = set(dollar_amount_fields + ["beginning_date", "ending_date"])
    found_fields = set(metadata.keys())
    missing_fields = required_fields - found_fields
    if len(missing_fields) > 0:
        raise ValueError(
            f"The following fields were not found in the file: {missing_fields}"
        )
    metadata["date"] = calculate_datetime_middle_point(
        metadata["beginning_date"], metadata["ending_date"]
    )

    return metadata


def extract_beginning_end_dates_sapphire(input_string):
    # Define a regular expression pattern to match date strings
    date_pattern = r"(\d{2}/\d{2}/\d{2})-(\d{2}/\d{2}/\d{2})"

    # Find all matching dates in the input string
    dates = re.findall(date_pattern, input_string)[0]
    n_detected_dates = len(dates)

    if len(dates) != 2:
        raise ValueError(
            f"Number of detected dates is not 2, detected {n_detected_dates}"
        )

    # Convert the date strings to datetime objects
    date_format = "%m/%d/%y"
    datetime_objs = []
    for date_str in dates:
        try:
            datetime_objs.append(datetime.strptime(date_str, date_format))
        except Exception as e:
            raise ValueError(
                f"Date string {date_str} was not possible to convert into datetime object. Exception: {e}"
            ) from e
    if datetime_objs[1] > datetime_objs[0]:
        ending_date = datetime_objs[1]
        beginning_date = datetime_objs[0]
    else:
        raise ValueError(
            f"The second extracted date '{datetime_objs[1].strftime('%b %d, %Y')}' is before the first extracted date '{datetime_objs[0].strftime('%b %d, %Y')}', please review why."
        )

    return beginning_date, ending_date


def extract_chase_sapphire_metadata(lines):
    fields = [
        {
            "fieldline_starts": ["previousbalance"],
            "field_name": "Beginning balance",
        },
        {"fieldline_starts": ["newbalance$"], "field_name": "Ending balance"},
        {"fieldline_starts": ["newbalance-$"], "field_name": "Ending balance"},
        {
            "fieldline_starts": ["payment,credits"],
            "field_name": "payments_and_credits",
        },
        {"fieldline_starts": ["purchases+"], "field_name": "purchases"},
        {"fieldline_starts": ["purchases$"], "field_name": "purchases"},
        {"fieldline_starts": ["balancetransfers"], "field_name": "balance_transfers"},
        {"fieldline_starts": ["cashadvances"], "field_name": "cash_advances"},
        {"fieldline_starts": ["feescharged"], "field_name": "fees_charged"},
        {"fieldline_starts": ["interestcharged"], "field_name": "interest_charged"},
    ]
    metadata = {}
    for line in lines:
        line = line.replace("`", "")
        if line.startswith("opening/closingdate") and (
            "beginning_date" not in metadata or "ending_date" not in metadata
        ):
            beginning_date, ending_date = extract_beginning_end_dates_sapphire(line)
            metadata["beginning_date"] = beginning_date
            metadata["ending_date"] = ending_date

        for field in fields:
            if (
                any(line.startswith(start) for start in field["fieldline_starts"])
                and field["field_name"] not in metadata
            ):
                metadata[field["field_name"]] = extract_dollar_amount(line)
    dollar_amount_fields = [
        "Beginning balance",
        "payments_and_credits",
        "purchases",
        "balance_transfers",
        "cash_advances",
        "fees_charged",
        "interest_charged",
        "Ending balance",
    ]
    required_fields = set(dollar_amount_fields + ["beginning_date", "ending_date"])
    found_fields = set(metadata.keys())
    missing_fields = required_fields - found_fields
    if len(missing_fields) > 0:
        raise ValueError(
            f"The following fields were not found in the file: {missing_fields}"
        )
    metadata["date"] = metadata["ending_date"]

    for key in dollar_amount_fields:
        metadata[key] = -metadata[key]
    return metadata


def process_checking_statement(filepath, save_filepath, acct_number):
    data = open_pdf(filepath)
    lines = [
        line.lower().replace(" ", "") for page in data for line in page.splitlines()
    ]
    if acct_number in ["2425", "0402", "1010"]:
        metadata = extract_chase_checking_metadata(lines)
    elif acct_number in ["8021", "7593", "1600", "4106"]:
        metadata = extract_chase_sapphire_metadata(lines)
    else:
        raise Exception(f"{acct_number} not valid account!")
    metadata["Account number"] = acct_number
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
    return processed_statements


def process_chase(project_path, acct_number):
    if acct_number not in ["2425", "0402", "1010", "8021", "7593", "1600", "4106"]:
        raise Exception(f"{acct_number} not valid account!")
    chase_folder = project_path / f"Chase-{acct_number}"
    folder_path = chase_folder / "Statements"

    statements_db = load_statements_db(Path(chase_folder))
    logging.info(f"Loaded statements DB from {folder_path.parent}")

    files = find_pdfs(folder_path)
    logging.info(f"Found {len(files)} PDF files in {folder_path}")

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
        processed_statements = process_checking_statement(
            filepath, save_filepath, acct_number
        )
        new_statements += processed_statements
    statements_db = pd.concat(
        [statements_db, pd.DataFrame(new_statements)], ignore_index=True
    )
    statements_db["account_number"] = statements_db["account_number"].astype(str)
    statements_db.drop_duplicates().to_csv(
        Path(chase_folder) / "statements_db.csv", index=False
    )
    logging.info(f"Finished processing all {acct_number} statements.")


if __name__ == "__main__":
    project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts")
    setup_logging()
    process_chase(project_path, "2425")
    process_chase(project_path, "0402")
    process_chase(project_path, "1010")
    process_chase(project_path, "8021")
    process_chase(project_path, "7593")
    process_chase(project_path, "1600")
    process_chase(project_path, "4106")
