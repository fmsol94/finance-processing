import os
from pathlib import Path

import pandas as pd
from commons import _deterministic_id, load_metadata_from_json, process_financial_df


def load_chase_csv(filepath, acct):
    if acct in ["2425", "0402", "1010"]:
        date_col = "Posting Date"
    elif acct in ["8021", "4106", "1600", "7593"]:
        date_col = "Post Date"
    else:
        raise ValueError(f"Account {acct} is not supported!")
    df = pd.read_csv(filepath, index_col=False)
    df = df.rename(columns={date_col: "Date"})
    df = process_financial_df(
        df=df, date_cols=["Date"], date_formats=["%m/%d/%Y"], amt_cols=["Amount"]
    )
    return df


def extract_chase_data_from_csv_using_metadata(df, metadata):
    min_date = df.Date.min()
    max_date = df.Date.max()
    if min_date > metadata["beginning_date"] or max_date < metadata["ending_date"]:
        df = None
        success = False
        print(
            f"Data outside expected range: min={min_date}, max={max_date}, "
            f"expected {metadata['beginning_date']}–{metadata['ending_date']}"
        )

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
            print("Filtered DataFrame is empty — no matching rows")
    return df, success


def check_chase_data_with_metadata(df, metadata):
    if metadata["Account number"] in ["8021"]:
        # TODO: Implement additional checks for metadata from sapphire credit card.
        pass
    if df["Balance"].iloc[-1] != metadata["Ending balance"]:
        raise ValueError(
            f"Extracted 'ending balance' amount ${metadata['Ending balance']} is different from the calculated ${df['Balance'].iloc[-1]}"
        )


def process_data(df, month, year, project_path, acct, csv_path):
    save_filepath = (
        project_path / f"Chase-{acct}" / "Processed Data" / year / f"{month:02d}"
    )
    filepath = save_filepath / "metadata.json"
    metadata = load_metadata_from_json(filepath)
    data, success = extract_chase_data_from_csv_using_metadata(df, metadata)
    check_chase_data_with_metadata(data, metadata)
    if not success:
        raise Exception("Extraction from metadata not successful!")
    data["Category"] = "Undefined"
    data["Flow Type"] = "Undefined"
    data["TransactionID"] = data.apply(lambda r: _deterministic_id(r, acct), axis=1)
    data["Account"] = f"Chase-{acct}"
    data["Source"] = csv_path
    data.to_csv(save_filepath / "transactions.csv", index=False)


def process_no_statement_data(df, month, year, project_path, acct, csv_path):
    save_filepath = (
        project_path / f"Chase-{acct}" / "Processed Data" / year / f"{month:02d}"
    )
    filepath = save_filepath / "metadata.json"
    metadata = load_metadata_from_json(filepath)
    data, success = extract_chase_data_from_csv_using_metadata(df, metadata)
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
        data["TransactionID"] = data.apply(lambda r: _deterministic_id(r, acct), axis=1)
        data["Account"] = f"Chase-{acct}"
        data["Source"] = csv_path
    data.to_csv(save_filepath / "transactions.csv", index=False)
    return metadata, data


def process_2425(project_path):
    csv_path = "/home/francisco/Documents/Finances/Statements/Accounts/Chase-2425/CSV/Chase2425_Activity_20250906(1).CSV"
    df = load_chase_csv(csv_path, "2425")
    for year in ["2024", "2025"]:
        range_obj = range(1, 7) if year == "2025" else range(1, 13)
        for month in range_obj:
            process_data(df, month, year, project_path, acct="2425", csv_path=csv_path)
    process_data(df, 12, "2023", project_path, acct="2425", csv_path=csv_path)

    csv_path = "/home/francisco/Documents/Finances/Statements/Accounts/Chase-2425/CSV/Chase2425_Activity_20231211.CSV"
    df = load_chase_csv(csv_path, "2425")
    for year in ["2022", "2023"]:
        range_obj = range(2, 13) if year == "2022" else range(1, 12)
        for month in range_obj:
            process_data(df, month, year, project_path, acct="2425", csv_path=csv_path)


def process_8021(project_path):
    csv_folder = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/Chase-8021/CSV"
    )
    csv_paths = list(csv_folder.glob("*.CSV"))
    dataframes = {}
    for csv_path in csv_paths:
        df = load_chase_csv(csv_path, acct="8021")
        end_date = df.Date.max().strftime("%Y-%m-%d")
        start_date = df.Date.min().strftime("%Y-%m-%d")
        print(start_date, end_date, csv_path.name)
        dataframes[start_date] = {
            "data": df,
            "start_date": start_date,
            "end_date": end_date,
            "csv_path": csv_path,
        }

    for year in range(2022, 2024):
        range_obj = range(9, 13) if year == 2022 else range(1, 12)
        for month in range_obj:
            date_id = "2022-08-30"
            print(date_id, dataframes[date_id]["end_date"], month, year)
            df = dataframes[date_id]["data"]
            csv_path = dataframes[date_id]["csv_path"]
            process_data(
                df, month, str(year), project_path, acct="8021", csv_path=csv_path
            )

    for year in range(2023, 2025):
        range_obj = range(12, 13) if year == 2023 else range(1, 6)
        for month in range_obj:
            date_id = "2023-09-10"
            print(date_id, dataframes[date_id]["end_date"], month, year)
            df = dataframes[date_id]["data"]
            csv_path = dataframes[date_id]["csv_path"]
            process_data(
                df, month, str(year), project_path, acct="8021", csv_path=csv_path
            )

    for year in range(2024, 2026):
        range_obj = range(6, 13) if year == 2024 else range(1, 8)
        for month in range_obj:
            date_id = "2024-04-30"
            print(date_id, dataframes[date_id]["end_date"], month, year)
            df = dataframes[date_id]["data"]
            csv_path = dataframes[date_id]["csv_path"]
            process_data(
                df, month, str(year), project_path, acct="8021", csv_path=csv_path
            )


def process_4106(project_path):
    csv_path = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/Chase-4106/CSV/Chase4106_Activity20230906_20250906_20250906.CSV"
    )
    df = load_chase_csv(csv_path, "4106")
    for year in range(2024, 2026):
        range_obj = range(3, 13) if year == 2024 else range(1, 6)
        for month in range_obj:
            print(month, year)
            process_data(
                df, month, str(year), project_path, acct="4106", csv_path=csv_path
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
    data["Account"] = f"Chase-{acct}"
    data["Source"] = "N/A"
    data.to_csv(metadata_path.parent / "transactions.csv", index=False)
    print(f"Transactions created in {metadata_path.parent}!")


def process_0402(project_path):
    csv_path = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/Chase-0402/CSV/Chase0402_Activity_20250906.CSV"
    )
    df = load_chase_csv(csv_path, "0402")
    ranges = {2023: range(10, 13), 2024: range(1, 13), 2025: range(1, 7)}
    for year in [2023, 2024, 2025]:
        for month in ranges[year]:
            process_data(
                df, month, str(year), project_path, acct="0402", csv_path=csv_path
            )

    ranges = {2018: range(8, 13), 2023: range(1, 10)}
    for year in [2018, 2019, 2020, 2021, 2022, 2023]:
        range_obj = ranges.get(year, range(1, 13))
        for month in range_obj:
            metadata_path = (
                project_path
                / "Chase-0402"
                / "Processed Data"
                / str(year)
                / f"{month:02d}"
                / "metadata.json"
            )
            create_fake_transactions(metadata_path)


def process_1010(project_path):
    csv_path = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/Chase-1010/CSV/Chase1010_Activity_20250906.CSV"
    )
    acct = "1010"
    df = load_chase_csv(csv_path, acct)
    ranges = {2023: range(10, 13), 2024: range(1, 13), 2025: range(1, 8)}
    for year in [2023, 2024, 2025]:
        for month in ranges[year]:
            process_data(
                df, month, str(year), project_path, acct=acct, csv_path=csv_path
            )

    ranges = {2018: range(9, 13), 2023: range(1, 10)}
    for year in [2018, 2019, 2020, 2021, 2022, 2023]:
        range_obj = ranges.get(year, range(1, 13))
        for month in range_obj:
            metadata_path = (
                project_path
                / f"Chase-{acct}"
                / "Processed Data"
                / str(year)
                / f"{month:02d}"
                / "metadata.json"
            )
            create_fake_transactions(metadata_path)


def process_1600(project_path):
    csv_path = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/Chase-1600/CSV/Chase1600_Activity20230906_20250906_20250907.CSV"
    )
    acct = "1600"
    df = load_chase_csv(csv_path, acct)
    ranges = {2023: range(11, 13), 2024: range(1, 13), 2025: range(1, 8)}
    for year in [2023, 2024, 2025]:
        for month in ranges[year]:
            process_data(
                df, month, str(year), project_path, acct=acct, csv_path=csv_path
            )

    ranges = {2018: range(1, 13), 2023: range(1, 11)}
    for year in [2018, 2019, 2020, 2021, 2022, 2023]:
        range_obj = ranges.get(year, range(1, 13))
        for month in range_obj:
            metadata_path = (
                project_path
                / f"Chase-{acct}"
                / "Processed Data"
                / str(year)
                / f"{month:02d}"
                / "metadata.json"
            )
            create_fake_transactions(metadata_path)


def process_7593(project_path):
    csv_path = Path(
        "/home/francisco/Documents/Finances/Statements/Accounts/Chase-7593/CSV/Chase7593_Activity20230906_20250906_20250907.CSV"
    )
    acct = "7593"
    df = load_chase_csv(csv_path, acct)
    ranges = {2023: range(10, 13), 2024: range(1, 13), 2025: range(1, 7)}
    for year in [2023, 2024, 2025]:
        for month in ranges[year]:
            process_data(
                df, month, str(year), project_path, acct=acct, csv_path=csv_path
            )

    ranges = {2019: range(5, 13), 2023: range(1, 10)}
    for year in [2019, 2020, 2021, 2022, 2023]:
        range_obj = ranges.get(year, range(1, 13))
        for month in range_obj:
            metadata_path = (
                project_path
                / f"Chase-{acct}"
                / "Processed Data"
                / str(year)
                / f"{month:02d}"
                / "metadata.json"
            )
            create_fake_transactions(metadata_path)


if __name__ == "__main__":
    project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts")
    process_2425(project_path)
    process_8021(project_path)
    process_4106(project_path)
    process_0402(project_path)
    process_1010(project_path)
    process_1600(project_path)
    process_7593(project_path)
