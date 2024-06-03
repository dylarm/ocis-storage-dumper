import argparse
import os
from pprint import pprint
from typing import Iterable, List

import msgpack  # type: ignore
from pathlib import Path

from tqdm import tqdm

parser = argparse.ArgumentParser(description="View the contents of a .mpk file")
parser.add_argument("mpkfile_or_dir", nargs="?", help="The .mpk file")
parser.add_argument(
    "-s",
    "--search",
    action="store_true",
    help="Search the given directory for mpk files instead",
)
parser.add_argument(
    "-o", "--output", help="Name of file to write output to (default STDOUT)"
)
parser.add_argument("-w", "--width", default=80, help="Width of output")

ARGS = parser.parse_args()


def load_mpk(file: Path):
    try:
        with open(file, "rb") as f:
            mpk_content = msgpack.unpack(f, raw=True)
            return mpk_content
    except ValueError:
        raise ValueError(f"Unpack failed for file {file}")


def find_all_mpks(path: Path) -> List[Path]:
    mpks: List[Path] = []
    for root, _, files in tqdm(
        os.walk(path), leave=False, desc="Finding all mpk files"
    ):
        for file in files:
            if file.endswith(".mpk"):
                mpks.append(Path(root, file))
    return mpks


def _read_one_mpk(mpkfile: Path):
    if not mpkfile.exists():
        raise FileExistsError(f"File does not exist: {mpkfile}")
    mpk = load_mpk(mpkfile)
    return mpk


def _read_all_mpk(mpkdir: Path):
    all_content = {}
    all_mpks = find_all_mpks(mpkdir)
    for mpk in tqdm(all_mpks, leave=True, desc="Processing all mpk files"):
        all_content[str(mpk)] = _read_one_mpk(mpk)
    return all_content


def main(args=ARGS):
    if args.search and Path(args.mpkfile_or_dir).is_file():
        raise NotADirectoryError("File provided¸ but asked to search directory")
    mpk_content = {}
    mpk_path = Path(args.mpkfile_or_dir)
    if args.search:
        mpk_content = _read_all_mpk(mpk_path)
    else:
        mpk_content = _read_one_mpk(mpk_path)
    if args.output:
        with open(args.output, "w") as g:
            pprint(object=mpk_content, stream=g, width=args.width)
    else:
        pprint(object=mpk_content, width=args.width)


if __name__ == "__main__":
    main()
