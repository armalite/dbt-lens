# dbt_lens/utils/git_utils.py

import subprocess
from pathlib import Path

def get_file_from_commit(commit: str, file_path: Path) -> str:
    """
    Retrieve the content of a file from a given commit using git.
    """
    try:
        result = subprocess.run(
            ["git", "show", f"{commit}:{file_path}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise FileNotFoundError(f"Could not retrieve {file_path} from commit {commit}") from e
