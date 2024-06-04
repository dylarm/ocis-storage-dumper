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
    help="Process metadata",
)
group.add_argument("-d", "--data", action="store_true", help="Process actual data")
# parser.set_defaults(metadata=True)

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
    mpk_type = mpk.get(b"user.ocis.type", b"N/A").decode("utf-8")
    if mpk_type == "1":
        mpk_type_name = "file"
    elif mpk_type == "2":
        mpk_type_name = "dir"
    else:
        mpk_type_name = "N/A"
    mpk_content = {
        "name": name,
        "parentid": parentid,
        "type": mpk_type,
        "type_name": mpk_type_name,
    }
    return mpk_content


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
    if mpk_content["type_name"] == "dir":
        symlink_path = Path(
            mpk_as_dir, "../../../../../", parent_path, mpk_content["name"]
        )
    elif mpk_content["type_name"] == "file":
        symlink_path = Path(
            mpk_as_dir.parents[0], "../../../../", parent_path, mpk_content["name"]
        )
    else:
        print(f"Weird file: {mpk_as_dir.stat()}")
        raise NotADirectoryError(f"{mpk_as_dir} is neither a file nor a directory.")
    return symlink_path


def main(args=ARGS):
    symlinks_exist, symlinks_actual, symlinks_theoretical = 0, 0, 0
    if args.metadata:
        path = Path(args.path, METADATA_SUBDIR)
    elif args.data:
        path = Path(args.path, DATA_SUBDIR)
    else:
        raise SystemExit("Specify whether to check metadata or user data")
    if not path.exists() or not path.is_dir():
        raise NotADirectoryError(f"Invalid OCIS path: {path}")
    print(f"Fixing files at {path}")
    node_paths = path.glob("*/*/nodes")
    for node_path in node_paths:
        mpks = find_all_mpks(node_path)
        for mpk in mpks:
            mpk_raw = load_mpk(mpk)
            mpk_content = get_mpk_info(mpk_raw)
            if "N/A" in [mpk_content[x] for x in mpk_content]:
                pprint(mpk_content)
                continue
            symlinks_theoretical += 1
            directory = mpkfile_to_dir(mpk)
            symlink_path = mpkdir_to_symlink(
                mpk_content=mpk_content, mpk_as_dir=directory
            )
            if symlink_path.exists():
                symlinks_exist += 1
            if symlink_path.is_symlink():
                symlinks_actual += 1
            print(f"{symlink_path}: {symlink_path.is_symlink()}")
    print(
        f"Symlinks: {symlinks_exist} (Actual: {symlinks_actual}) (Theoretical: {symlinks_theoretical})"
    )


if __name__ == "__main__":
    main()
