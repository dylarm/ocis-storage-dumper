import argparse
import os
import time
from pprint import pprint
from typing import Iterable, List, Optional

import msgpack  # type: ignore
from pathlib import Path

from tqdm import tqdm

METADATA_SUBDIR = "storage/metadata/spaces/"
DATA_SUBDIR = "storage/users/spaces/"


parser = argparse.ArgumentParser(description="Verify (and fix) incorrect symlinks")
parser.add_argument("path", nargs="?", help="Path to OCIS data")
# parser.add_argument("-l", "--log", help="File to store paths of mpk files")
group = parser.add_mutually_exclusive_group()
group.add_argument(
    "-m",
    "--metadata",
    action="store_true",
    help="Process metadata (default b/c it's smaller)",
)
group.add_argument("-d", "--data", action="store_true", help="Process actual data")
parser.set_defaults(metadata=True)

ARGS = parser.parse_args()
if not ARGS.path:
    parser.print_help()
    raise SystemExit(1)


def fourslashes(s: str) -> str:
    if s is None:
        raise TypeError("Cannot convert None to path")
    split_id = [s[i : i + 2] for i in range(0, 8, 2)]
    split_id.append(s[8:])
    return "/".join(split_id)


def mpkfile_to_dir(mpkfile: Path) -> Path:
    return Path(mpkfile.parents[0], mpkfile.stem)


def get_mpk_info(mpk: dict[bytes, bytes]) -> dict[str, str]:
    name = mpk.get(b"user.ocis.name", b"N/A").decode("utf-8")
    parentid = mpk.get(b"user.ocis.parentid", b"N/A").decode("utf-8")
    return {"name": name, "parentid": parentid}


def load_mpk(file: Path):
    try:
        with open(file, "rb") as f:
            mpk_content = msgpack.unpack(f, raw=True)
            return mpk_content
    except ValueError:
        raise ValueError(f"Unpack failed for file {file}")


def find_all_mpks(mpk_path: Path) -> Iterable[Path]:
    mpks: List[Path] = []
    for root, _, files in tqdm(
        os.walk(mpk_path), leave=False, desc="Finding all mpk files"
    ):
        for file in files:
            if file.endswith(".mpk"):
                mpks.append(Path(root, file))
    return mpks


def mpkdir_to_symlink(mpk_content: dict[str, str], mpk_as_dir: Path) -> Path:
    parent_path = fourslashes(mpk_content["parentid"])
    symlink_path = Path(mpk_as_dir, "../../../../../", parent_path, mpk_content["name"])
    return symlink_path


def main(args=ARGS):
    symlinks = 0
    if args.metadata:
        path = Path(args.path, METADATA_SUBDIR)
    elif args.data:
        path = Path(args.path, DATA_SUBDIR)
    else:
        # Should never get here...
        raise SystemExit("Only metadata or user data should be used")
    if not path.exists() or not path.is_dir():
        raise NotADirectoryError(f"Invalid OCIS path: {path}")
    print(f"Fixing files at {path}")
    node_paths = path.glob("*/*/nodes")
    for node_path in node_paths:
        mpks = find_all_mpks(node_path)
        for mpk in mpks:
            mpk_raw = load_mpk(mpk)
            mpk_content = get_mpk_info(mpk_raw)
            directory = mpkfile_to_dir(mpk)
            symlink_path = mpkdir_to_symlink(
                mpk_content=mpk_content, mpk_as_dir=directory
            )
            if symlink_path.exists():
                symlinks += 1
                print(symlink_path)
    print(f"Symlinks: {symlinks}")


if __name__ == "__main__":
    main()
