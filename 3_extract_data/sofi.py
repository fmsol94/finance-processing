import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from commons import (
    _deterministic_id,
    calculate_datetime_middle_point,
    convert_to_decimal,
    find_pdfs,
    load_statements_db,
    open_pdf,
    process_financial_df,
    save_dict_as_json,
    setup_logging,
)
from tqdm import tqdm


def extract_beginning_end_dates(input_string):
    # Define a regex pattern to match MM/DD/YYYY date format
    date_pattern = r"(\w{3}\d{1,2},\d{4})-(\w{3}\d{2},\d{4})"

    # Find all matching dates in the input string
    statement_periods = re.findall(date_pattern, input_string)
    n_detected_statement_periods = len(statement_periods)

    if n_detected_statement_periods != 1:
        raise ValueError(
            f"Number of detected statement_period is not 1, detected {n_detected_statement_periods}"
        )

    start_date_str, end_date_str = statement_periods[0]

    # Convert the date strings to datetime objects
    date_format = "%b%d,%Y"
    datetime_objs = []
    for date_str in [start_date_str, end_date_str]:
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


def extract_dollar_and_percentage_amounts(input_string):
    input_string = input_string.replace(",", "")

    # Define the regex pattern to capture three numbers with two decimal places
    pattern = r"\$(\d+\.\d{2})(\d+\.\d{2})%\$(\d+\.\d{2})"

    # Search for the pattern in the input string
    match = re.search(pattern, input_string)

    if match:
        # Extract the captured expressions
        output = []
        for i in range(3):
            try:
                output.append(convert_to_decimal(match.group(i + 1)))
            except Exception as e:
                raise ValueError(
                    f"Amount {output} was not possible to convert into decimal object. Exception: {e}"
                ) from e

    else:
        raise ValueError(
            f"Dollar and percentage amounts were not detected in input string '{input_string}'"
        )
    return output


def extract_metadata(lines):
    lines = [line.lower().replace(" ", "") for line in lines]
    metadata = {}
    for idx, line in enumerate(lines):
        if "monthlystatementperiod" in line and (
            "beginning_date" not in metadata or "ending_date" not in metadata
        ):
            if idx + 1 < len(lines):
                beginning_date, ending_date = extract_beginning_end_dates(
                    lines[idx + 1]
                )
                metadata["beginning_date"] = beginning_date
                metadata["ending_date"] = ending_date
            else:
                raise ValueError(
                    "Beginning date line was not found when extracting metadata!"
                )

        if line.startswith("currentbalance") and ("Ending balance" not in metadata):
            if idx + 1 < len(lines):
                extracted_fields = extract_dollar_and_percentage_amounts(lines[idx + 1])
                metadata["Ending balance"] = extracted_fields[0]
                metadata["current_interest_rate"] = extracted_fields[1] / 100
                metadata["monthly_interest_paid"] = extracted_fields[2]
            else:
                raise ValueError(
                    "Beginning balance line was not found when extracting metadata!"
                )

        if line.startswith("beginningbalance") and (
            "Beginning balance" not in metadata
        ):
            if idx + 1 < len(lines):
                extracted_fields = extract_dollar_and_percentage_amounts(lines[idx + 1])
                metadata["Beginning balance"] = extracted_fields[0]
                metadata["annual_percentage_yield_earned"] = extracted_fields[1] / 100
                metadata["year_to_date_interest_paid"] = extracted_fields[2]
            else:
                raise ValueError(
                    "Ending balance line was not found when extracting metadata!"
                )

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


def extract_transaction_details(input_string):
    # Regular expression pattern with capturing groups
    pattern = r"^([A-Z][a-z]{2} \d{1,2}, \d{4})\s+(.*?)\s+(-?\$(\d{1,3}(?:,\d{3})*\.\d{2}))\s+(-?\$(\d{1,3}(?:,\d{3})*\.\d{2}))$"

    match = re.match(pattern, input_string)
    if match:
        date, desc, amount1, amount2 = (
            match.group(1),
            match.group(2).strip(),
            match.group(3),
            match.group(5),
        )

        return date, desc, amount1, amount2

    return None


def extract_data(lines, metadata):
    df = []
    for line in lines:
        output = extract_transaction_details(line)
        if output is not None:
            df.append(
                {
                    "Date": output[0],
                    "Description": output[1],
                    "Amount": output[2],
                    "Balance": output[3],
                }
            )
    if len(df) > 0:
        df = pd.DataFrame(df)
    else:
        raise ValueError("No transaction lines were extracted!")

    df = process_financial_df(
        df=df, date_formats=["%b %d, %Y"], amt_cols=["Amount", "Balance"]
    )

    # Check balance
    df["Balance"] = df["Amount"].cumsum() + metadata["Beginning balance"]
    if df["Balance"].iloc[-1] != metadata["Ending balance"]:
        raise ValueError(
            f"Extracted 'ending balance' amount ${metadata['Ending balance']} is different from the calculated ${df['Balance'].iloc[-1]}"
        )

    return df


def sofi_data_extract(data):
    checking_pages = [
        page for page in data if "checkingaccount-8365" in page.replace(" ", "").lower()
    ]
    savings_pages = [
        page for page in data if "savingsaccount-9806" in page.replace(" ", "").lower()
    ]
    lines = {
        "8365": [line for page in checking_pages for line in page.splitlines()],
        "9806": [line for page in savings_pages for line in page.splitlines()],
    }

    metadata, data = [], []
    for acct_number, acct_lines in lines.items():
        acct_metadata = extract_metadata(acct_lines)
        acct_metadata["Account number"] = acct_number
        metadata.append(acct_metadata)

        acct_data = extract_data(acct_lines, acct_metadata)
        data.append(acct_data)
    return data, metadata


def sofi_setup():
    rootpath = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/SoFi-8365/Statements"
    )
    for year in [2023, 2024, 2025]:
        for month in range(1, 13):
            path = rootpath / str(year) / f"{month:02d}"
            print(path)
            os.makedirs(path, exist_ok=True)

        files = list((rootpath / str(year)).glob("*.pdf"))
        for file in files:
            elems = str(file).split("-")
            y = elems[-3].strip()
            m = elems[-2].strip()
            path = rootpath / y / m
            # print(file, path)
            dest = path / file.name
            print(f"Moved {file.name} to {dest}")
            shutil.move(str(file), str(dest))


def process_sofi_statement(filepath, save_filepath):
    data = open_pdf(pdf_path=filepath)
    data, metadata = sofi_data_extract(data)

    processed_statements = []
    for data_, metadata_ in zip(data, metadata):
        acct_name = f"SoFi-{metadata_['Account number']}"
        actual_savepath = Path(str(save_filepath).replace("SoFi-8365", acct_name))
        processed_statements.append(
            {
                "account_number": metadata_["Account number"],
                "month": metadata_["date"].month,
                "year": metadata_["date"].year,
                "raw_documents_path": filepath,
                "processed_documents_path": actual_savepath,
            }
        )
        os.makedirs(actual_savepath, exist_ok=True)
        save_dict_as_json(data=metadata_, filepath=actual_savepath / "metadata.json")
        data_["Category"] = "Undefined"
        data_["Flow Type"] = "Undefined"
        acct = metadata_.get("Account number", "")
        data_["TransactionID"] = data_.apply(
            lambda r, acct=acct: _deterministic_id(r, acct), axis=1
        )
        data_["Account"] = acct_name
        data_["Source"] = filepath
        data_.to_csv(actual_savepath / "transactions.csv", index=False)
    return processed_statements


def process_sofi(statements_folder):

    acct_folder = statements_folder.parent

    statements_db = load_statements_db(Path(acct_folder))
    logging.info(f"Loaded statements DB from {acct_folder}")

    files = find_pdfs(statements_folder)
    logging.info(f"Found {len(files)} PDF files in {statements_folder}")

    unprocessed_files = {str(file) for file in files} - set(
        statements_db["raw_documents_path"]
    )
    # unprocessed_files = [Path(file) for file in unprocessed_files]
    unprocessed_files = files
    logging.info(f"{len(unprocessed_files)} unprocessed PDF files.")
    new_statements = []
    for file in tqdm(unprocessed_files, desc="Processing PDFs", unit="file"):
        filepath = file
        path = file.parent

        before, after = str(path).rsplit("Statements", 1)
        save_filepath = Path(before + "Processed Data" + after)

        logging.info(f"Processing file {filepath} -> {save_filepath}")
        processed_statements = process_sofi_statement(filepath, save_filepath)
        new_statements += processed_statements
    statements_db = pd.concat(
        [statements_db, pd.DataFrame(new_statements)], ignore_index=True
    )
    statements_db["account_number"] = statements_db["account_number"].astype(str)
    statements_db.drop_duplicates().to_csv(
        Path(acct_folder) / "statements_db.csv", index=False
    )
    logging.info(f"Finished processing all SoFi statements.")


if __name__ == "__main__":
    # sofi_setup()
    setup_logging()
    sofi_statements_folder = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/SoFi-8365/Statements"
    )
    process_sofi(sofi_statements_folder)
