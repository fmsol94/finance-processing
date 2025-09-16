import os
from pathlib import Path

import pandas as pd
from commons import (
    _deterministic_id,
    load_metadata_from_json,
    previous_year_month,
    process_financial_df,
    save_dict_as_json,
)
from dateutil.relativedelta import relativedelta


def load_discover_csv(filepath, acct_number):
    if acct_number not in ["4589"]:
        raise ValueError(f"Account {acct_number} is not supported!")
    df = pd.read_csv(filepath, index_col=False)

    if "Post date" in df:
        date_col = "Post date"
    elif "Post Date" in df:
        date_col = "Post Date"
    else:
        raise Exception("Date column not found in provided dataframe")

    df = df.rename(columns={date_col: "Date"})
    df = process_financial_df(df=df)
    if acct_number == "4589":
        df["Amount"] = -df["Amount"]
    return df


def extract_discover_data_from_csv_using_metadata(df, metadata):
    min_date = df.Date.min()
    max_date = df.Date.max()
    if min_date > metadata["beginning_date"] or max_date < metadata["ending_date"]:
        df = None
        success = False
    else:
        df = df[
            (df.Date >= metadata["beginning_date"])
            & (df.Date <= metadata["ending_date"])
        ].copy()
        if df.shape[0] > 0:
            success = True
            df["Balance"] = df["Amount"].cumsum() + metadata["Beginning balance"]
        else:
            success = False
    return df, success


def check_discover_data_with_metadata(df, metadata):
    payments_condition = (
        df["Category"]
        .str.lower()
        .isin(["payments and credits", "awards and rebate credits"])
    )
    fees_condition = df["Category"].str.lower().isin(["fees"])
    purchases = df[~payments_condition & ~fees_condition].Amount.sum()
    payments_and_credits = df[payments_condition].Amount.sum()
    fees = df[fees_condition].Amount.sum()
    if metadata["payments_and_credits"] != payments_and_credits:
        raise ValueError(
            f"Extracted 'payments' amount ${metadata['payments_and_credits']} is different from the calculated ${payments_and_credits}"
        )

    if metadata["purchases"] != purchases:
        raise ValueError(
            f"Extracted 'purchases' amount ${metadata['purchases']} is different from the calculated ${purchases}"
        )

    if metadata["fees_charged"] != fees:
        raise ValueError(
            f"Extracted 'fees' amount ${metadata['fees_charged']} is different from the calculated ${fees}"
        )

    if df["Balance"].iloc[-1] != metadata["Ending balance"]:
        raise ValueError(
            f"Extracted 'ending balance' amount ${metadata['Ending balance']} is different from the calculated ${df['Balance'].iloc[-1]}"
        )


def process_data(df, month, year, project_path, acct, csv_path):
    save_filepath = (
        project_path / f"Discover-{acct}" / "Processed Data" / year / f"{month:02d}"
    )
    filepath = save_filepath / "metadata.json"
    metadata = load_metadata_from_json(filepath)
    data, success = extract_discover_data_from_csv_using_metadata(df, metadata)
    if success:
        check_discover_data_with_metadata(data, metadata)
    else:
        raise Exception("Extraction from metadata not successful!")
    data["Category"] = "Undefined"
    data["Flow Type"] = "Undefined"
    data["TransactionID"] = data.apply(lambda r: _deterministic_id(r, acct), axis=1)
    data["Account"] = f"Discover-{acct}"
    data["Source"] = csv_path
    data.to_csv(save_filepath / "transactions.csv", index=False)


def process_no_statement_data(
    df, save_filepath, acct, csv_path, extract_data_from_csv_using_metadata
):
    acct_n = acct.split("-")[-1]
    filepath = save_filepath / "metadata.json"
    metadata = load_metadata_from_json(filepath)
    data, success = extract_data_from_csv_using_metadata(df, metadata)
    if not success:
        if not data.empty:
            raise Exception("Extraction from metadata not successful!")
        else:
            data = pd.DataFrame(
                columns=[
                    "Date",
                    "Description",
                    "Category",
                    "Amount",
                    "Balance",
                    "Flow Type",
                    "TransactionID",
                    "Account",
                    "Source",
                ]
            )
    else:
        data["Category"] = "Undefined"
        data["Flow Type"] = "Undefined"
        data["TransactionID"] = data.apply(
            lambda r: _deterministic_id(r, acct_n), axis=1
        )
        data["Account"] = acct
        data["Source"] = csv_path
    data.to_csv(save_filepath / "transactions.csv", index=False)
    return metadata, data


def statement_not_available(statement_path, csv_path, load_csv):
    acct = statement_path.parents[2].name
    acct_folder = statement_path.parents[2]
    year = int(statement_path.parents[0].name)
    month = int(statement_path.name)

    prev_year, prev_month = previous_year_month(year, month)

    acct_n = acct.split("-")[-1]
    year = str(year)
    month = f"{month:02d}"

    before, after = str(statement_path).rsplit("Statements", 1)
    save_filepath = Path(before + "Processed Data" + after)

    prev_metadata_path = (
        acct_folder
        / "Processed Data"
        / str(prev_year)
        / f"{prev_month:02d}"
        / "metadata.json"
    )
    try:
        prev_metadata = load_metadata_from_json(prev_metadata_path)
    except Exception as e:
        raise FileNotFoundError(
            f"Previous month's metadata file not found at '{prev_metadata_path}'. "
            f"Please ensure it exists before running this script. Original error: {e}"
        ) from e

    partial_metadata = {
        "beginning_date": prev_metadata["ending_date"],
        "ending_date": prev_metadata["ending_date"] + relativedelta(months=1),
        "Beginning balance": prev_metadata["Ending balance"],
        "date": prev_metadata["date"] + relativedelta(months=1),
        "Account number": acct_n,
    }

    os.makedirs(save_filepath, exist_ok=True)
    save_dict_as_json(data=partial_metadata, filepath=save_filepath / "metadata.json")

    df = load_csv(csv_path, acct_n)
    metadata, data = process_no_statement_data(
        df,
        save_filepath,
        acct,
        csv_path,
        extract_data_from_csv_using_metadata=extract_discover_data_from_csv_using_metadata,
    )
    if data.empty:
        metadata["Ending balance"] = metadata["Beginning balance"]
    else:
        metadata["Ending balance"] = data.iloc[-1]["Balance"]
    save_dict_as_json(data=metadata, filepath=save_filepath / "metadata.json")

    # Create statement placeholder
    data = {
        "account": acct,
        "year": int(year),
        "month": int(month),
        "note": "No statement available for this month.",
    }
    statement_path = Path(str(save_filepath).replace("Processed Data", "Statements"))
    os.makedirs(statement_path, exist_ok=True)
    save_dict_as_json(
        data=data,
        filepath=statement_path
        / f"statement-{acct}-{data['year']}-{data['month']}.placeholder.json",
    )


def create_fake_transactions(metadata_path):
    metadata = load_metadata_from_json(metadata_path)
    data = {
        "Date": metadata["date"],
        "Description": "Fake Transaction",
        "Amount": metadata["Ending balance"] - metadata["Beginning balance"],
        "Balance": metadata["Ending balance"],
    }
    data = pd.DataFrame([data])
    acct = metadata["Account number"]

    data["Category"] = "Undefined"
    data["Flow Type"] = "Undefined"
    data["TransactionID"] = data.apply(lambda r: _deterministic_id(r, acct), axis=1)
    data["Account"] = f"Discover-{acct}"
    data["Source"] = "N/A"
    data.to_csv(metadata_path.parent / "transactions.csv", index=False)
    print(f"Transactions created in {metadata_path.parent}!")


def process_4589(project_path):
    acct_number = "4589"
    csv_folder = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/Discover-4589/CSV/"
    )
    csv_paths = list(csv_folder.glob("*.csv"))
    dataframes = {}
    for csv_path in csv_paths:
        df = load_discover_csv(csv_path, acct_number)
        end_date = df.Date.max().strftime("%Y-%m-%d")
        start_date = df.Date.min().strftime("%Y-%m-%d")
        print(start_date, end_date, csv_path.name)
        dataframes[start_date] = {
            "data": df,
            "start_date": start_date,
            "end_date": end_date,
            "csv_path": csv_path,
        }
    ranges = {2021: range(8, 13), 2023: range(1, 4)}
    for year in range(2021, 2024):
        range_obj = ranges.get(year, range(1, 13))
        for month in range_obj:
            date_id = "2021-06-17"
            print(date_id, dataframes[date_id]["end_date"], month, year)
            df = dataframes[date_id]["data"]
            csv_path = dataframes[date_id]["csv_path"]
            process_data(
                df, month, str(year), project_path, acct=acct_number, csv_path=csv_path
            )

    for month in range(4, 11):
        date_id = "2023-01-14"
        print(date_id, dataframes[date_id]["end_date"], month, year)
        df = dataframes[date_id]["data"]
        csv_path = dataframes[date_id]["csv_path"]
        process_data(
            df, month, str(year), project_path, acct=acct_number, csv_path=csv_path
        )

    ranges = {2023: range(11, 13), 2025: range(1, 8)}
    for year in range(2023, 2026):
        range_obj = ranges.get(year, range(1, 13))
        for month in range_obj:
            date_id = "2023-09-08"

            df = dataframes[date_id]["data"]
            csv_path = dataframes[date_id]["csv_path"]

            statement_path = (
                project_path
                / "Discover-4589"
                / "Statements"
                / (str(year))
                / f"{month:02d}"
            )
            processed_path = (
                project_path
                / f"Discover-{acct_number}"
                / "Processed Data"
                / str(year)
                / f"{month:02d}"
            )
            if processed_path.exists():
                if not list(statement_path.glob("*.placeholder.json")):
                    process_data(
                        df,
                        month,
                        str(year),
                        project_path,
                        acct=acct_number,
                        csv_path=csv_path,
                    )
                else:
                    statement_not_available(
                        statement_path=statement_path,
                        csv_path=csv_path,
                        load_csv=load_discover_csv,
                    )

            else:
                statement_not_available(
                    statement_path=statement_path,
                    csv_path=csv_path,
                    load_csv=load_discover_csv,
                )
            print(date_id, dataframes[date_id]["end_date"], month, year)
    folders_with_metadata_and_no_transactions = [
        d
        for d in (project_path / "Discover-4589").rglob("*")
        if d.is_dir()
        and (d / "metadata.json").exists()
        and not (d / "transactions.csv").exists()
    ]

    for d in sorted(folders_with_metadata_and_no_transactions):
        metadata_path = d / "metadata.json"
        create_fake_transactions(metadata_path)


if __name__ == "__main__":
    project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts")
    process_4589(project_path)
