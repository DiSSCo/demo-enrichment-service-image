"""Copy the team modules this MAS runs into geographic_locality_mas/ and list them in VENDORED.md.

  python geographic-locality/vendor_team.py ../dissco-geographic-locality-mas

Copies community_context.py, indigenous_names.py and the two CSV files from a checkout of
the team repository (https://github.com/craigmateo/dissco-geographic-locality-mas, branch
feature/indigenous-place-names) and writes their sha256 (a test checks them).
Change the code in the team repository, then copy it again; don't edit the copies here.
"""

import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent / "geographic_locality_mas"
FILES = {  # team repository -> here
    "src/geographic_locality_mas/community_context.py": "community_context.py",
    "src/geographic_locality_mas/indigenous_names.py": "indigenous_names.py",
    "data/indigenous-areas.csv": "data/indigenous-areas.csv",
    "data/community-contacts.csv": "data/community-contacts.csv",
}


def describe(repo: Path) -> str:
    """Branch and commit of the team checkout, with a note if the files are not committed."""

    def git(*args: str) -> str:
        result = subprocess.run(["git", "-C", str(repo), *args], check=False, capture_output=True, text=True)
        return result.stdout.strip()

    dirty = " (with uncommitted changes)" if git("status", "--porcelain", "--", *FILES) else ""
    return f"branch `{git('rev-parse', '--abbrev-ref', 'HEAD')}`, commit `{git('rev-parse', '--short', 'HEAD')}`{dirty}"


def write_manifest(source: str) -> None:
    rows = [f"| `{name}` | `{hashlib.sha256((HERE / name).read_bytes()).hexdigest()}` |" for name in FILES.values()]
    (HERE / "VENDORED.md").write_text(
        "# Vendored team code: don't edit here\n\n"
        "Copies from https://github.com/craigmateo/dissco-geographic-locality-mas "
        f"({source}), made by `geographic-locality/vendor_team.py`.\n"
        "Change them in the team repository and copy them again.\n\n"
        "| File | sha256 |\n|---|---|\n" + "\n".join(rows) + "\n",
        encoding="utf-8",
    )


def main(repo: Path) -> None:
    for source, target in FILES.items():
        (HERE / target).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(repo / source, HERE / target)
    write_manifest(describe(repo))
    print(f"Copied {len(FILES)} files into {HERE}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(Path(sys.argv[1]).resolve())
