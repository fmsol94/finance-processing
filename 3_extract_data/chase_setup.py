import json
import os
import shutil
from pathlib import Path

from commons import find_pdfs


def save_json(data, filepath: str | Path, indent: int = 4):
    """
    Save Python data as JSON to the given filepath.

    Args:
        data (dict | list): The data to save.
        filepath (str | Path): The target file path.
        indent (int): Indentation level for pretty-printing (default=4).
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)  # ensure directory exists
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts")
accounts = [
    "Chase-2425",
    "Chase-0402",
    "Chase-1010",
    "Chase-8021",
    "Chase-1600",
    "Chase-7593",
    "Chase-4106",
]

for acct in accounts:
    os.makedirs(project_path / acct / "Statements", exist_ok=True)
    os.makedirs(project_path / acct / "Processed Data", exist_ok=True)
    details = {
        "name": acct,
        "acct_n": acct.split("-")[-1],
        "latest_balance": "",
        "last_update": "",
        "open_date": "01/2025",
    }
    save_json(details, project_path / acct / "details.json")
files = find_pdfs("/home/francisco/Documents/Finances/Statements/Historical data/CHASE")
for file in files:
    year = file.name[:4]
    month = file.name[4:6]
    acct_number = file.name.split("-")[-2]
    if not acct_number.isdigit():
        acct_number = file.name.split("-")[-1].split(".")[0]
    acct = f"Chase-{acct_number}"
    if acct not in accounts:
        raise Exception(f"{acct} not known account!")
    if year not in [str(y) for y in range(2018, 2026)]:
        raise Exception(f"{year} not known year!")
    if month not in [f"{m:02d}" for m in range(1, 13)]:
        raise Exception(f"{month} not known month!")
    dest_dir = project_path / acct / "Statements" / year / month
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / file.name
    shutil.move(str(file), str(dest))
    print(f"Moved {file.name} to {dest}")
