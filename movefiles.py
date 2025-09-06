import datetime
import shutil
from pathlib import Path


def parse_month_year(date_str: str) -> tuple[int, int]:
    # Parse the input string like "September 2024"
    dt = datetime.datetime.strptime(date_str, "%B %Y")
    return dt.month, dt.year


path = Path("/home/francisco/Documents/Finances/Statements/Accounts/Apple-5843")
statement_path = path / "Statements"
files = list(statement_path.glob("*.pdf"))
for file in files:
    m, y = parse_month_year(file.name.split(" - ")[-1].split(".")[0])
    dest_dir = statement_path / str(y) / f"{m:02d}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / file.name
    shutil.move(str(file), str(dest))
    print(f"Moved {file.name} to {dest}")
