import os
from pathlib import Path

BASE_DIR = Path(".")
BACKUP_PATH = Path(os.environ.get("BACKUP_PATH", BASE_DIR / "backup.json"))
SCHEDULER_SIZE = int(os.environ.get("SCHEDULER_SIZE", 10))
