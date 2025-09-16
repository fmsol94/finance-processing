# pylint: disable=logging-fstring-interpolation
import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from commons import (
    _deterministic_id,
    calculate_datetime_middle_point,
    convert_to_decimal,
    find_pdfs,
    first_day_of_month,
    last_day_of_month,
    load_statements_db,
    open_pdf,
    process_financial_df,
    save_dict_as_json,
    setup_logging,
)
from tqdm import tqdm


def extract_beginning_end_dates_legacy(input_string):
    # Use regular expressions to extract date and time information
    match = re.search(
        r"opendate:([a-z]+\d{1,2},\d{4})-closedate:([a-z]+\d{1,2},\d{4})", input_string
    )
    if match:
        open_date_str, close_date_str = match.groups()
        datetime_objs = []
        for date_str in [open_date_str, close_date_str]:
            try:
                datetime_objs.append(datetime.strptime(date_str, "%b%d,%Y"))
            except Exception as e:
                raise ValueError(
                    f"Date string {date_str} was not possible to convert into datetime object. Exception: {e}"
                ) from e
    else:
        raise ValueError(f"Dates were not extracted from line '{input_string}'.")
    if datetime_objs[1] > datetime_objs[0]:
        ending_date = datetime_objs[1]
        beginning_date = datetime_objs[0]
    else:
        raise ValueError(
            f"The second extracted date '{datetime_objs[1].strftime('%b %d, %Y')}' is before the first extracted date '{datetime_objs[0].strftime('%b %d, %Y')}', please review why."
        )

    return beginning_date, ending_date


def extract_beginning_end_dates_from_savings(input_string):
    # Define a regex pattern to match MM/DD/YYYY date format
    date_pattern = r"(\w{3}\d{2},\d{4})-(\w{3}\d{2},\d{4})"

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


def extract_beginning_end_dates(input_string):
    # Define a regex pattern to match MM/DD/YYYY date format
    date_pattern = r"(\d{2}/\d{2}/\d{4})"

    # Find all matching dates in the input string
    dates = re.findall(date_pattern, input_string)
    n_detected_dates = len(dates)

    if len(dates) != 2:
        raise ValueError(
            f"Number of detected dates is not 2, detected {n_detected_dates}"
        )

    # Convert the date strings to datetime objects
    date_format = "%m/%d/%Y"
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
                f"Dollar amount {output} was not possible to convert into datetime object. Exception: {e}"
            ) from e
    else:
        raise ValueError(f"Dollar amount was not detected in input string '{s}'")
    return output


def extract_dollar_perc_amount(s):
    # Regular expression pattern for dollar amounts and percentages
    pattern = r"[-+]?(?:\$\d{1,3}(?:,\d{3})*\.?\d{0,2}|\d+(?:\.\d+)?%)"
    match = re.search(pattern, s)
    if match:
        output = match.group()
        try:
            # Check if the extracted value is a percentage
            if "%" in output:
                output = (
                    float(output.strip("%")) / 100
                )  # Convert percentage to a decimal
            else:
                output = convert_to_decimal(output)
        except Exception as e:
            raise ValueError(
                f"Amount {output} was not possible to convert. Exception: {e}"
            ) from e
    else:
        raise ValueError(f"Amount was not detected in input string '{s}'")
    return output


def check_extracted_metadata(metadata):
    calculated_end_balance = (
        metadata["Beginning balance"]
        + metadata["payments_and_credits"]
        + metadata["purchases"]
        + metadata["balance_transfers"]
        + metadata["cash_advances"]
        + metadata["fees_charged"]
        + metadata["interest_charged"]
    )
    extracted_end_balance = metadata["Ending balance"]
    if calculated_end_balance != extracted_end_balance:
        raise ValueError(
            f"Extracted end balance ${extracted_end_balance} does not match the calculated end balance ${calculated_end_balance}"
        )


def extract_discover_credit_card_metadata(lines):
    fields = [
        {
            "fieldline_starts": ["previousbalance"],
            "field_name": "Beginning balance",
        },
        {"fieldline_starts": ["newbalance"], "field_name": "Ending balance"},
        {
            "fieldline_starts": ["paymentsandcredits"],
            "field_name": "payments_and_credits",
        },
        {"fieldline_starts": ["purchases"], "field_name": "purchases"},
        {"fieldline_starts": ["balancetransfers"], "field_name": "balance_transfers"},
        {"fieldline_starts": ["cashadvances"], "field_name": "cash_advances"},
        {"fieldline_starts": ["feescharged"], "field_name": "fees_charged"},
        {"fieldline_starts": ["interestcharged"], "field_name": "interest_charged"},
    ]
    metadata = {}
    for line in lines:
        if line.startswith("accountsummary") and (
            "beginning_date" not in metadata or "ending_date" not in metadata
        ):
            beginning_date, ending_date = extract_beginning_end_dates(line)
            metadata["beginning_date"] = beginning_date
            metadata["ending_date"] = ending_date

        if line.startswith("opendate") and (
            "beginning_date" not in metadata or "ending_date" not in metadata
        ):
            beginning_date, ending_date = extract_beginning_end_dates_legacy(line)
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
    metadata["date"] = calculate_datetime_middle_point(
        metadata["beginning_date"], metadata["ending_date"]
    )

    for key in dollar_amount_fields:
        metadata[key] = -metadata[key]
    return metadata


def extract_discover_bank_metadata(lines):
    fields = [
        {
            "fieldline_starts": ["beginningbalance"],
            "field_name": "Beginning balance",
        },
        {"fieldline_starts": ["endingbalance"], "field_name": "Ending balance"},
        {
            "fieldline_starts": ["depositsandcredits"],
            "field_name": "deposits_and_credits",
        },
        {
            "fieldline_contains": ["annualpercentageyieldearned"],
            "field_name": "annual_percentage_yield_earned",
        },
        {
            "fieldline_contains": ["interestearnedthisperiod"],
            "field_name": "interest_earned_this_period",
        },
        {
            "fieldline_contains": ["interestearnedyear-to-date"],
            "field_name": "interest_earned_year_to_date",
        },
        {
            "fieldline_contains": ["interestpaidyear-to-date"],
            "field_name": "interest_earned_year_to_date",
        },
        {
            "fieldline_starts": ["electronicwithdrawals"],
            "field_name": "electronic_withdrawals",
        },
        {
            "fieldline_starts": ["servicecharges,fees,andotherwithdrawals"],
            "field_name": "service_charges_fees_and_other_withdrawals",
        },
    ]
    metadata = {}
    for line in lines:
        if line.startswith("statementperiod") and (
            "beginning_date" not in metadata or "ending_date" not in metadata
        ):
            beginning_date, ending_date = extract_beginning_end_dates_from_savings(line)
            metadata["beginning_date"] = beginning_date
            metadata["ending_date"] = ending_date
        for field in fields:
            if "fieldline_starts" in field:
                if (
                    any(line.startswith(start) for start in field["fieldline_starts"])
                    and field["field_name"] not in metadata
                ):
                    elems = [elem for elem in line.split("..") if elem != ""]
                    metadata[field["field_name"]] = extract_dollar_amount(elems[1])
            if "fieldline_contains" in field:
                if (
                    any(substring in line for substring in field["fieldline_contains"])
                    and field["field_name"] not in metadata
                ):
                    elems = [elem for elem in line.split("..") if elem != ""]
                    metadata[field["field_name"]] = extract_dollar_perc_amount(
                        elems[-1]
                    )
    dollar_amount_fields = [
        "Beginning balance",
        "annual_percentage_yield_earned",
        "deposits_and_credits",
        "interest_earned_this_period",
        "interest_earned_year_to_date",
        "electronic_withdrawals",
        "service_charges_fees_and_other_withdrawals",
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


def extract_acct_number(lines):
    acct_number = None
    if any("4589" in line for line in lines):
        acct_number = "4589"
    if any("8384" in line for line in lines):
        if acct_number is None:
            acct_number = "8384"
        else:
            raise ValueError(
                "Both discover account numbers were found in this document."
            )
    return acct_number


def extract_transaction_details(input_string):
    # Regular expression pattern with capturing groups
    pattern = r"^([A-Z][a-z]{2} \d{1,2}) ([A-Z][a-z]{2} \d{1,2})(.*?)(\$?\s*\d{1,3}(?:,\d{3})*\.\d{2})?$"

    match = re.match(pattern, input_string)
    if match:
        date1, date2, remaining_string, amount = (
            match.groups()[0],
            match.groups()[1],
            match.groups()[2],
            match.groups()[3],
        )
        # Trim any trailing or leading whitespace from the description
        description = remaining_string.strip()

        return date1, date2, description, amount
    else:
        return None


def discover_savings_data_extract_from_pdf(data, metadata):
    lines = [line for page in data for line in page.splitlines()]
    data = []
    for line in lines:
        output = extract_transaction_details(line)
        if output is not None:
            date1, date2, description, amount = output
            data.append(
                {
                    "Date": date1,
                    "Bus. Date": date2,
                    "Description": description,
                    "Amount": amount,
                }
            )
    if len(data) > 0:
        data = pd.DataFrame(data)
    else:
        raise ValueError("No transaction lines were extracted!")
    data["Date"] = data["Date"] + f", {metadata['beginning_date'].year}"
    data = process_financial_df(df=data, date_formats=["%b %d, %Y"])

    months = set(data["Date"].apply(lambda date: date.month))
    if 1 in months and 12 in months:
        data["Date"] = data["Date"].apply(
            lambda date: (
                date.replace(year=metadata["ending_date"].year)
                if date.month == 1
                else date
            )
        )

    # Fix sign for withdrawals:
    data["Amount"] = data.apply(
        lambda row: (
            -row["Amount"]
            if "withdrawal" in row["Description"].lower().replace(" ", "")
            else row["Amount"]
        ),
        axis=1,
    )

    data["Balance"] = data["Amount"].cumsum() + metadata["Beginning balance"]
    if data["Balance"].iloc[-1] != metadata["Ending balance"]:
        raise ValueError(
            f"Extracted 'ending balance' amount ${metadata['Ending balance']} is different from the calculated ${data['Balance'].iloc[-1]}"
        )
    return data, metadata


def discover_savings_statement_split(data, metadata):
    if metadata["beginning_date"].month != metadata["ending_date"].month:
        # Assumption: beginning and end dates are always first and last day of the month (except for the first statement,
        # since the acct was open on a day different that the first of the month).
        if metadata["beginning_date"] != first_day_of_month(
            metadata["beginning_date"]
        ) and metadata["beginning_date"] != datetime(2022, 10, 21, 0, 0):
            raise ValueError(
                f"Expected 'beginning_date' is not first day of the month '{first_day_of_month(metadata['beginning_date']).strftime('%b %d, %Y')}', received: '{metadata['beginning_date'].strftime('%b %d, %Y')}'."
            )
        if metadata["ending_date"] != last_day_of_month(metadata["ending_date"]):
            raise ValueError(
                f"Expected 'ending_date' is not last day of the month '{last_day_of_month(metadata['ending_date']).strftime('%b %d, %Y')}', received: '{metadata['ending_date'].strftime('%b %d, %Y')}'."
            )
        output_data = []
        output_metadata = []
        for month in range(
            metadata["beginning_date"].month, metadata["ending_date"].month + 1
        ):
            month_data = (
                data[data.Date.apply(lambda date, m=month: date.month == m)]
                .reset_index(drop=True)
                .copy()
            )
            month_metadata = {
                "beginning_date": first_day_of_month(
                    metadata["beginning_date"].replace(month=month)
                ),
                "ending_date": last_day_of_month(
                    metadata["ending_date"].replace(month=month, day=25)
                ),
                "Beginning balance": month_data["Balance"].iloc[0]
                - month_data["Amount"].iloc[0],
                "Ending balance": month_data["Balance"].iloc[-1],
                "annual_percentage_yield_earned": metadata[
                    "annual_percentage_yield_earned"
                ],
                "Account number": metadata["Account number"],
            }
            month_metadata["date"] = calculate_datetime_middle_point(
                month_metadata["beginning_date"], month_metadata["ending_date"]
            )
            output_data.append(month_data)
            output_metadata.append(month_metadata)
        data = output_data
        metadata = output_metadata

    return data, metadata


def discover_metadata_extract_from_pdf(data):
    lines = [line.lower().replace(" ", "") for line in data[0].splitlines()]
    acct_number = extract_acct_number(lines)
    if acct_number == "4589":
        metadata = extract_discover_credit_card_metadata(lines)
        metadata["Account number"] = "4589"
    elif acct_number == "8384":
        metadata = extract_discover_bank_metadata(lines)
        metadata["Account number"] = "8384"
    elif acct_number is None:
        raise ValueError("No account number was extracted from this file.")
    else:
        raise ValueError(f"Extracted account number {acct_number} is not supported.")
    return metadata


def process_8384(filepath):
    path = filepath.parent

    before, _ = str(path).rsplit("Statements", 1)
    save_filepath = Path(before + "Processed Data")

    logging.info(f"Processing file {filepath} -> {save_filepath}")

    data = open_pdf(pdf_path=filepath)
    metadata = discover_metadata_extract_from_pdf(data)
    data, metadata = discover_savings_data_extract_from_pdf(data, metadata)
    data, metadata = discover_savings_statement_split(data, metadata)

    if not isinstance(data, list) or not isinstance(metadata, list):
        metadata = [metadata]
        data = [data]

    processed_statements = []
    for data_, metadata_ in zip(data, metadata):
        acct_name = f"Discover-{metadata_['Account number']}"
        year = metadata_["date"].year
        month = metadata_["date"].month
        metadata_["source"] = str(filepath)
        actual_savepath = save_filepath / str(year) / f"{month:02d}"
        processed_statements.append(
            {
                "account_number": metadata_["Account number"],
                "month": month,
                "year": year,
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


def process_4589(filepath):
    path = filepath.parent

    before, after = str(path).rsplit("Statements", 1)
    save_filepath = Path(before + "Processed Data" + after)

    data = open_pdf(pdf_path=filepath)
    metadata = discover_metadata_extract_from_pdf(data)
    metadata["source"] = str(filepath)

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


def process_discover(project_path, acct_number):
    if acct_number == "8384":
        process_discover_statement = process_8384
    elif acct_number == "4589":
        process_discover_statement = process_4589
    else:
        raise ValueError(f"{acct_number} not valid account!")

    acct_folder = project_path / f"Discover-{acct_number}"
    statements_folder = acct_folder / "Statements"

    statements_db = load_statements_db(acct_folder)
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
        processed_statements = process_discover_statement(filepath=file)
        new_statements += processed_statements
    statements_db = pd.concat(
        [statements_db, pd.DataFrame(new_statements)], ignore_index=True
    )
    statements_db["account_number"] = statements_db["account_number"].astype(str)
    statements_db.drop_duplicates().to_csv(
        Path(acct_folder) / "statements_db.csv", index=False
    )
    logging.info(f"Finished processing all Discover statements.")


if __name__ == "__main__":
    project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts")
    setup_logging()
    process_discover(project_path, "8384")
    process_discover(project_path, "4589")
