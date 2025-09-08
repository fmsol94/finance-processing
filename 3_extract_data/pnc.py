import re
import shutil
from datetime import datetime
from pathlib import Path

from commons import find_pdfs, open_pdf
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
        dest_dir = project_path / f"PNC-{acct_n}" / str(year) / f"{month:02d}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / file.name
        shutil.move(str(file), str(dest))
        print(f"Moved {file.name} to {dest}")


if __name__ == "__main__":
    project_path = Path("/home/francisco/Documents/Finances/Statements/Accounts/")
    historical_pnc_path = Path(
        "/home/francisco/Documents/Finances/Statements/Historical data/PNC/"
    )
    pnc_setup(project_path, historical_pnc_path)
