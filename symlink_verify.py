import argparse
import os
import shutil
import sys
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
parser.add_argument(
    "-f", "--fix", action="store_true", help="Repair any missing/incorrect symlinks"
)
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
    if mpk_content["type_name"] == "dir" and mpk_as_dir.is_dir():
        symlink_path = Path(
            mpk_as_dir, "../../../../../", parent_path, mpk_content["name"]
        )
    elif mpk_content["type_name"] == "dir" and mpk_as_dir.is_file():
        symlink_path = Path(
            mpk_as_dir.parents[0], "../../../../", parent_path, mpk_content["name"]
        )
    elif mpk_content["type_name"] == "dir":
        # To catch other types of "dir"
        symlink_path = Path(
            mpk_as_dir, "../../../../../", parent_path, mpk_content["name"]
        )
    elif mpk_content["type_name"] == "file":
        symlink_path = Path(
            mpk_as_dir.parents[0], "../../../../", parent_path, mpk_content["name"]
        )
    else:
        print(f"Weird mpk contents: {mpk_content}")
        raise NotADirectoryError(f"{mpk_as_dir} is neither a file nor a directory.")
    return symlink_path


def main(args=ARGS):
    symlinks_exist, symlinks_actual, symlinks_theoretical, symlinks_actual_fixed = (
        0,
        0,
        0,
        0,
    )
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
                # pprint(mpk_content)
                continue
            symlinks_theoretical += 1
            directory = mpkfile_to_dir(mpk)
            symlink_path = mpkdir_to_symlink(
                mpk_content=mpk_content, mpk_as_dir=directory
            )
            # This might be a dangerous operation, but currently os.path.relpath returns with an extra "../" compared
            # to the actual Path.readlink()
            symlink_rel_target = Path(os.path.relpath(directory, symlink_path)[3:])
            symlink_actual_path = None
            if symlink_path.exists():
                symlinks_exist += 1
            if symlink_path.is_symlink():
                symlinks_actual += 1
                symlink_actual_path = symlink_path.readlink()
            # print(f"{symlink_path}: {symlink_path.is_symlink()}")
            if args.fix:
                if symlink_actual_path is None:
                    print(
                        f"{symlink_path} is currently not a symlink.\n\tShould point to\t {symlink_rel_target}"
                    )
                if symlink_path.exists() and not symlink_path.is_symlink():
                    print(f"\tRemoving current not-symlink {symlink_path.name}")
                    if mpk_content["type_name"] == "dir":
                        shutil.rmtree(symlink_path)
                    elif mpk_content["type_name"] == "file":
                        symlink_path.unlink(missing_ok=True)
                elif not directory.exists():
                    # Let's hope this doesn't destroy things...
                    print(f"Creating {directory.name}")
                    directory.touch()
                    # Need to update some paths too
                    symlink_path = mpkdir_to_symlink(
                        mpk_content=mpk_content, mpk_as_dir=directory.parents[0]
                    )
                if mpk_content["type_name"] == "dir":
                    symlink_parent_dir = symlink_path.parents[0].resolve()
                    try:
                        symlink_parent_dir.mkdir(
                            mode=0o600, parents=True, exist_ok=True
                        )
                    except FileExistsError:
                        print(f"\t{symlink_path.name} already exists")
                # print(f"{symlink_path.name} → {symlink_rel_target}")
                # print(f"{symlink_rel_target} ←→ {symlink_actual_path}")
                try:
                    symlink_path.resolve().symlink_to(symlink_rel_target)
                except FileExistsError:
                    if symlink_path.is_symlink():
                        print(f"{symlink_path.name} is already a symlink")
                    else:
                        print(f"\tSkipping {symlink_path.name} for now...")
                except FileNotFoundError:
                    print(f"{mpk_raw}")
                    print(
                        f"{symlink_rel_target} appears to not exist, skipping for now..."
                    )
                except NotADirectoryError:
                    print(f"\tNeed to create directory {symlink_path.parents[0]}")
                    symlink_path.parents[0].unlink(missing_ok=True)
                    symlink_path.parents[0].mkdir(parents=True, exist_ok=True)
                try:
                    new_symlink_path = symlink_path.readlink()
                except:
                    new_symlink_path = None
                print(
                    f"\tNew link: \t{new_symlink_path}\n\tShould be: \t{Path(os.path.relpath(directory, symlink_path)[3:])}"
                )
                if new_symlink_path == Path(
                    os.path.relpath(directory, symlink_path)[3:]
                ):
                    symlinks_actual_fixed += 1
                    print("\tSuccess!")
                else:
                    print("\tFailure")

    print(
        f"Symlinks 'exist': {symlinks_exist}\n\tActual: {symlinks_actual}\n\tTheoretical: {symlinks_theoretical}\n\tFixed: {symlinks_actual_fixed}"
    )
    if (
        symlinks_exist != symlinks_actual
        or symlinks_exist != symlinks_theoretical
        or symlinks_actual != symlinks_theoretical
    ):
        print("There may be some incorrect symlinks")


if __name__ == "__main__":
    main()
