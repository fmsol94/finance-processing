from pathlib import Path

import pandas as pd
from commons import _deterministic_id, load_metadata_from_json, process_financial_df


def load_chase_csv(filepath, date_col):
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
        project_path / "Chase-2425" / "Processed Data" / year / f"{month:02d}"
    )
    filepath = save_filepath / "metadata.json"
    metadata = load_metadata_from_json(filepath)
    data, success = extract_chase_data_from_csv_using_metadata(df, metadata)
    data["Category"] = "Undefined"
    data["Flow Type"] = "Undefined"
    data["TransactionID"] = data.apply(lambda r: _deterministic_id(r, acct), axis=1)
    data["Account"] = "Apple-5843"
    data["Source"] = csv_path
    data.to_csv(save_filepath / "transactions.csv", index=False)


project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts")
csv_path = "/home/francisco/Documents/Finances/Statements/Accounts/Chase-2425/CSV/Chase2425_Activity_20250906(1).CSV"
df = load_chase_csv(csv_path, "Posting Date")
for year in ["2024", "2025"]:
    range_obj = range(1, 7) if year == "2025" else range(1, 13)
    for month in range_obj:
        process_data(df, month, year, project_path, acct="2425", csv_path=csv_path)
process_data(df, 12, "2023", project_path, acct="2425", csv_path=csv_path)

csv_path = "/home/francisco/Documents/Finances/Statements/Accounts/Chase-2425/CSV/Chase2425_Activity_20231211.CSV"
df = load_chase_csv(csv_path, "Posting Date")
for year in ["2022", "2023"]:
    range_obj = range(2, 13) if year == "2022" else range(1, 12)
    for month in range_obj:
        process_data(df, month, year, project_path, acct="2425", csv_path=csv_path)
