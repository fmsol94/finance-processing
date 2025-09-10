import re
import shutil
from datetime import datetime
from pathlib import Path

from commons import find_pdfs
from tqdm import tqdm


def parse_bank_statement(filename: str):
    """
    Parses a filename like 'Bank-Statement-20230131-1234'
    and extracts year, month, and account number.

    Returns a dict with keys: year, month, acct_number
    """
    # Regex to find date (YYYYMMDD) and trailing account number
    date_pattern = re.compile(r"\b(\d{8})\b")  # YYYYMMDD
    acct_pattern = re.compile(r"(\d+)$")  # last digits

    # Find date
    date_match = date_pattern.search(filename)
    if not date_match:
        raise ValueError(f"No valid date found in filename: {filename}")

    date_str = date_match.group(1)

    # Validate date
    try:
        date_obj = datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        raise ValueError(f"Invalid date in filename: {date_str}")

    # Find account number
    acct_match = acct_pattern.search(filename)
    if not acct_match:
        raise ValueError(f"No account number found in filename: {filename}")

    acct_number = int(acct_match.group(1))

    return {"year": date_obj.year, "month": date_obj.month, "acct_number": acct_number}


project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts/")
acct_path = Path(
    "/home/francisco/repositories/expenses-tracker-v2/data/raw_documents/DISCOVER/"
)
files = find_pdfs(acct_path)
for file in tqdm(files):
    output = parse_bank_statement(str(file.stem))
    acct = f"Discover-{output['acct_number']}"
    if acct not in ["Discover-4589", "Discover-8384"]:
        raise ValueError(f"{acct} not known account!")
    year = str(output["year"])
    month = f"{output['month']:02d}"
    dest_dir = project_path / acct / "Statements" / year / month
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / file.name
    shutil.move(str(file), str(dest))
    print(f"Moved {file.name} to {dest}")
