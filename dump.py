#!/usr/bin/python3

# Importing the necessary modules
import os
import datetime
import pickle
import shutil
from pathlib import Path
from typing import Iterable, Union, Tuple, Any, Generator, List

import msgpack  # type: ignore
import sys
import argparse

from tqdm import tqdm


# A function to split a string into parts and join with slashes
def fourslashes(s: str) -> str:
    if s is None:
        return ""
    s = decode_if_bytes(s)
    split_id = [s[i : i + 2] for i in range(0, 8, 2)]
    split_id.append(s[8:])
    return "/".join(split_id)


def decode_if_bytes(s: Union[str, bytes]) -> str:
    if isinstance(s, bytes):
        return s.decode("utf-8")
    else:
        return s


# Create the argument parser
parser = argparse.ArgumentParser(
    description="Version 2.0\nTopdir is the directory of ocis storage. Default is $HOME/.ocis"
)

# Add an argument to the parser - topdir
parser.add_argument(
    "topdir",
    nargs="?",
    default="{0}/.ocis".format(os.getenv("HOME")),
    help="The directory of ocis storage",
)

# Add ability to save and restore progress
parser.add_argument(
    "-p",
    "--prefix",
    default="state-",
    help="Prefix to store current state (in case of resuming)",
)

# Add the new list argument
parser.add_argument(
    "-l", "--list", action="store_true", help="List files without copying"
)

# Add the user argument
parser.add_argument("-u", "--user", help="Filter by user's name")
parser.add_argument("-un", "--username", help="Filter by actual username")

# Add the "only show size" argument
parser.add_argument(
    "-i", "--info", action="store_true", help="Only show basic info, without the tree"
)
# TODO: add ability to verify/fix symlinks in topdir (personal need, from a bad copy operation)
# Parse the command-line arguments
ARGS = parser.parse_args()

# Define the top directory for the output
OUTTOP = "/tmp/ocis-dump-" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")

# Define the storage prefix
SPREFIX = "storage/users/spaces"


# def load_mpk_decoded(file):
#     try:
#         with open(file, "rb") as f:
#             mpk_content = msgpack.unpack(f, raw=True)
#         return mpk_content
#     except ValueError:
#         print(f"Unpack failed for file: {file}")
#         return None


def _load_mpk_decoded(file: Path):
    try:
        with open(file, "rb") as f:
            mpk_content = msgpack.unpack(f, raw=True)
        return mpk_content
    except ValueError:
        raise ValueError(f"Unpack failed for file: {file}")


user_exists = False

# TODO: replace for-loop and "if 'nodes'" with Path.glob()
# Walk through the directory structure starting from 'top'
# for dirpath, dirnames, filenames in os.walk(os.path.join(top, sprefix)):
#     if "nodes" in dirnames:
#         # Get the directory for space nodes
#         space_nodes_dir = dirpath
#         # Construct the spaceid from the directory path
#         spaceid = dirpath.split("/")[-2] + os.path.basename(dirpath)
#         # Construct the root path
#         root = os.path.join(space_nodes_dir, "nodes", fourslashes(spaceid))
#
#         # TODO: use glob'd name from Path
#         mpk_file = f"{root}.mpk"
#         mpk_content = load_mpk_decoded(mpk_file)
#         if mpk_content is not None:
#
#             # Extract space name and type from the msgpack content
#             space_name = mpk_content.get(b"user.ocis.space.name", b"N/A").decode(
#                 "utf-8"
#             )
#             space_type = mpk_content.get(b"user.ocis.space.type", b"N/A").decode(
#                 "utf-8"
#             )
#
#             if args.user and args.user.lower() not in space_name.lower():
#                 continue
#
#             user_exists = True
#
#             # Print the space info
#             print(f"\n[{space_type}/{space_name}]")
#             print(f"\troot = {root}")
#             print(
#                 f"\ttreesize = {mpk_content.get(b'user.ocis.treesize', b'N/A')} bytes"
#             )
#             print("\tsymlink_tree =")
#
#             # Initialize a dictionary to store files and parents
#             files_and_parents = {}
#
#             # Go through the nodes
#             # TODO: this could also be improved with Path.glob()
#             # rationale: some mpk files, for unknown reasons, have a datetime between the filename and the mpk suffix
#             nodes_dir = os.path.join(dirpath, "nodes")
#             for dirpath2, dirnames2, filenames2 in os.walk(nodes_dir):
#                 for filename in filenames2:
#                     if filename.endswith(".mpk"):
#                         # Construct the path to the msgpack file
#                         mpk_file2 = os.path.join(dirpath2, filename)
#                         mpk_content2 = load_mpk_decoded(mpk_file2)
#
#                         # Extract parentid, blobid, and name from the msgpack content
#                         parentid = mpk_content2.get(b"user.ocis.parentid")
#                         blobid = mpk_content2.get(b"user.ocis.blobid", b"N/A")
#                         name = mpk_content2.get(b"user.ocis.name", b"N/A")
#
#                         # Check if blobid is available
#                         if blobid != b"N/A":
#                             # Check if parent is space
#                             if parentid == spaceid:
#                                 files_and_parents[name.decode("utf-8")] = (".", blobid)
#                             elif parentid is not None and parentid != spaceid:
#                                 # Construct the path to the parent
#                                 parent_path = os.path.join(
#                                     space_nodes_dir, "nodes", fourslashes(parentid)
#                                 )
#                                 # Construct the path to the parent's msgpack file
#                                 mpk_file3 = f"{parent_path}.mpk"
#                                 mpk_content3 = load_mpk_decoded(mpk_file3)
#                                 # Extract the parent's name from the msgpack content
#                                 parent_name = mpk_content3.get(
#                                     b"user.ocis.name", b"N/A"
#                                 ).decode("utf-8")
#                                 files_and_parents[name.decode("utf-8")] = (
#                                     f"./{parent_name}",
#                                     blobid,
#                                 )
#
#             # Copy the files to the output directory
#             for i, (name, (parent_path, blobid)) in enumerate(
#                 files_and_parents.items(), start=1
#             ):
#                 # Construct the path to the blob
#                 blob_path = os.path.join(space_nodes_dir, "blobs", fourslashes(blobid))
#                 if os.path.exists(blob_path):
#                     # Remove prefix from the personal space name
#                     if space_type == "personal" and "_" in space_name:
#                         space_name = space_name.split("_")[1]
#                     # Construct the path to the temporary file
#                     tmp_path = os.path.join(
#                         outtop, space_type, space_name, parent_path, name
#                     )
#                     # Print file without copying if 'list' argument is provided
#                     if args.list:
#                         print(f"\t{i}\t{parent_path}/{name} -> blobid={blobid}")
#                     else:
#                         # Create the directories if they do not exist
#                         os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
#                         # Copy the blob to the temporary file
#                         shutil.copy2(blob_path, tmp_path)
#                         print(f"\t{i}\t{parent_path}/{name} -> blobid={blobid}")
#                 else:
#                     # Print a warning if the blob does not exist
#                     print(f"WARNING: Blob file {blob_path} does not exist.")
#


def find_nodes(path: Path) -> Iterable[Path]:
    # Only two directories down
    # This is dirpath + "nodes" in the original code
    return path.glob("*/*/nodes")


def find_all_mpks(path: Path) -> Iterable[Path]:
    # Find all mpk files under the given path
    mpks: List[Path] = []
    for root, dirs, files in tqdm(
        os.walk(path), leave=False, desc="Finding all mpk files"
    ):
        for file in files:
            if file.endswith(".mpk"):
                mpks.append(Path(root, file))
    return mpks


def find_mpk(path: Path) -> Path:
    # Find the mpk for a given root_id
    trivial_mpk = Path(str(path) + ".mpk")
    if trivial_mpk.exists():
        return trivial_mpk
    # Only go up one more step to find it
    possible_mpk = [f for f in path.parents[0].glob("*.mpk") if f.exists()]
    if possible_mpk:
        return possible_mpk[0]
    else:
        raise FileNotFoundError(f"No file with root {path} found")


def mpk_info(mpk_file) -> Iterable[str]:
    # Will return space_name, space_type, size, parentid, blodid, and name
    s_name: str = mpk_file.get(b"user.ocis.space.name", b"N/A").decode("utf-8")
    s_alias: str = mpk_file.get(b"user.ocis.space.alias", b"N/A").decode("utf-8")
    s_type: str = mpk_file.get(b"user.ocis.space.type", b"N/A").decode("utf-8")
    s_size_bytes = int(mpk_file.get(b"user.ocis.treesize", b"N/A"))
    s_user = s_alias.split("/")[1]
    if 1024 < s_size_bytes < 1024 * 1024:
        s_size = s_size_bytes / 1024
        size_type = "KiB"
    elif 1024 * 1024 < s_size_bytes < 1024 * 1024 * 1024:
        s_size = s_size_bytes / (1024 * 1024)
        size_type = "MiB"
    elif s_size_bytes > 1024 * 1024 * 1024:
        s_size = s_size_bytes / (1024 * 1024 * 1024)
        size_type = "GiB"
    else:
        s_size = s_size_bytes
        size_type = "bytes"
    return s_name, s_type, str(round(s_size, 2)), size_type, s_user


def gen_node_info(path: Path) -> Iterable[Path]:
    node_dir = path.parent
    space_id = Path(node_dir.parts[-2] + node_dir.parts[-1])
    root_id = Path(path, fourslashes(str(space_id)))
    return node_dir, space_id, root_id


def gen_mpk_info(path: Path) -> Iterable[str]:
    mpk = _load_mpk_decoded(path)
    parent_id = mpk.get(b"user.ocis.parentid")
    blob_id = mpk.get(b"user.ocis.blobid", b"N/A")
    name = mpk.get(b"user.ocis.name", b"N/A")
    return parent_id, blob_id, name.decode("utf-8")


def check_for_saved_file(file: Path) -> Any:
    if file.exists() and file.is_file():
        with open(file, "rb") as f:
            try:
                data = pickle.load(f)
            except EOFError:
                raise FileNotFoundError(f"File {file} is empty")
        return data
    else:
        raise FileNotFoundError(f"File {file} does not exist")


def save_state(file: Path, obj: Any):
    if isinstance(obj, Generator):
        obj = list(obj)
    with open(file, "wb") as f:
        pickle.dump(obj, f)


def find_files_and_parents(
    node_mpks: Iterable[Path], space_id: str, parent_node: Path
) -> dict[str, Tuple[str, str]]:
    files_and_parents: dict[str, Tuple[str, str]] = {}
    # i = 0
    for individual_mpk in tqdm(node_mpks, leave=False, desc="Finding all files"):
        # print(f"file {i} at {datetime.datetime.now()}")
        parent_id, blob_id, name = gen_mpk_info(individual_mpk)
        # Make sure blobid is available
        if blob_id == "N/A":
            continue
        if parent_id == space_id:
            # If the parent is the space, easy
            files_and_parents[name] = (".", blob_id)
        elif parent_id is not None and parent_id != space_id:
            # Create the path to the parent
            parent_path = Path(parent_node, fourslashes(parent_id))
            parent_mpk = find_mpk(parent_path)
            _, _, parent_name = gen_mpk_info(parent_mpk)
            files_and_parents[name] = ("./{0}".format(parent_name), blob_id)
        #  += 1
    return files_and_parents


def find_parents(
    individual_mpk: Path, space_id: str, parent_node: Path
) -> dict[str, Tuple[str, str]]:
    files_and_parents: dict[str, Tuple[str, str]] = {}
    # Base
    parent_id, blob_id, name = gen_mpk_info(individual_mpk)
    if parent_id == space_id:
        files_and_parents[name] = (".", blob_id)
    elif parent_id is not None and parent_id != space_id:
        parent_path = Path(parent_node, fourslashes(parent_id))
        parent_mpk = find_mpk(parent_path)
        files_and_parents = find_parents(parent_mpk, space_id, parent_node)
    return files_and_parents


def main(sprefix: str = SPREFIX, args: argparse.Namespace = ARGS) -> None:
    # TODO: make "global" variables into arguments
    # x1. Find the nodes
    # x2. For each node, find the mpk files under it
    # x3. Extract pertinent info (mpk_info)
    # x4. Create file+parent dict
    # 5. Copy files to outtop (optional)
    # 6. Fix any symlinks that have been resolved (optional, stretch goal)
    #
    # Check if the directory supplied is good
    top = args.topdir
    if not Path(top, "storage").is_dir():
        raise NotADirectoryError(f"'storage' folder not found in {top}")
    print(f"top is: {top}")
    user_exists = False

    # Get all nodes
    nodes = find_nodes(path=Path(top, sprefix))
    for node in nodes:
        root_id: Path
        node_dir, space_id, root_id = gen_node_info(node)
        try:
            root_mpk = find_mpk(root_id)
            root_mpk_contents = _load_mpk_decoded(root_mpk)
        except FileNotFoundError:
            print(f"No mpk for {root_id}")
            continue
        except ValueError:
            print(f"Unpack failed for mpk for {root_id}")
            continue
        space_name, space_type, tree_size, size_type, space_user = mpk_info(
            root_mpk_contents
        )
        # See if we're actually looking for this user
        if args.user and args.user.lower() not in space_name.lower():
            print(f"Not parsing for {space_name}")
            continue
        if args.username and args.username.lower() not in space_user.lower():
            print(f"Not parsing for {space_user}")
            continue
        user_exists = True
        # Show info so far
        print(f"Space type & name: [{space_type}/{space_name}]")
        print(f"\tusername: {space_user}")
        print(f"\troot = {root_id}")
        print(f"\ttree size = {tree_size} {size_type}")
        if args.info:
            continue
        print("\tsymlink_tree =")

        # Go through the node and match all files
        node_prefix = Path(args.prefix + f"node_{space_user}")
        files_prefix = Path(args.prefix + f"files_{space_user}")
        try:
            node_mpks = check_for_saved_file(file=node_prefix)
        except FileNotFoundError:
            node_mpks = find_all_mpks(node_dir)
            save_state(file=node_prefix, obj=node_mpks)
        try:
            files_and_parents = check_for_saved_file(file=files_prefix)
        except FileNotFoundError:
            files_and_parents = find_files_and_parents(
                node_mpks=node_mpks, space_id=str(space_id), parent_node=node
            )
            save_state(file=files_prefix, obj=files_and_parents)
        blob_exist = 0
        blob_noexist = 0
        for i, (name, (parent_path, blob_id)) in tqdm(
            enumerate(files_and_parents.items(), start=1),
            leave=False,
            desc="Constructing paths",
            disable=True,
        ):
            blob_path = Path(node_dir, "blobs", fourslashes(blob_id))

            if blob_path.exists():
                blob_exist += 1
                if space_type == "personal" and "_" in space_name:
                    space_name = space_name.split("_")[1]
                print(f"\t{i}\t{parent_path}/{name}")
            else:
                blob_noexist += 1
                print(f"\t{i}\t{parent_path}/{name}\t(DNE)")
        print(f"Exist: {blob_exist}\nNot: {blob_noexist}")
    return


if __name__ == "__main__":
    main()


# Print the location of the copied files
# if not args.list:
#     print(f"\nFiles were copied to: {outtop}")
#
# if args.user and not user_exists:
#     print(f"No user found with the username: {args.user}")
