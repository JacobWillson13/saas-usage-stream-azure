#!/usr/bin/env python3
"""Fail if a tracked file or prepared Parquet data contains a blocked term.

Scans every file `git ls-files` reports (any type), plus Parquet column names,
key-value metadata, and string values. Text is split into alphanumeric words
(snake_case, kebab-case, camelCase, paths, and dotted names all split), and each
lowercased word is compared by SHA-256 against BLOCKED_SHA256, so the blocked
vocabulary never appears in plain text.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Hashes keep the blocked vocabulary out of tracked files.
# Print the hash for a new term with: check_names.py --hash TERM
BLOCKED_SHA256 = frozenset(
    {
        "a43d9701ff924f6929bf1d7d1b2b6ec2a9f07a51e6425f61f5ad91c7832f6a54",
        "8597d34ca4c425f56468dd5d90f6d16296f6dd185de1483c22f7b93e4cd409c2",
        "2bed2032ed3d8300b7714ff77101e6c43637597977337c699191dd844c86a72d",
        "b35a1639496c6390b20b56071a161be9bafc110be3cf28a68be59affef17ddeb",
        "ec6f049478046f72becb006a2339202eae93911281dd610a7c869adec1ba3297",
        "8c22d8168998b4d6e7357f76a14d787fa122d21ac715e99b95f93178d0cc6432",
        "795b104abe3e4134960ca245ded0e617f162c751209347b7f003bc35e062f43e",
        "cffc8d42491b55c5902e6df8fb5a6001dd952df758c3b21039c3e2e8c434c933",
    }
)

WORD = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|[0-9]+")
ALNUM = re.compile(r"[A-Za-z0-9]+")
PRINTABLE_RUN = re.compile(rb"[\x20-\x7e]{4,}")


def hash_token(token: str) -> str:
    return hashlib.sha256(token.lower().encode("utf-8")).hexdigest()


def tokens(text: str) -> set[str]:
    found: set[str] = set()
    for run in ALNUM.findall(text):
        found.add(run.lower())
        found.update(word.lower() for word in WORD.findall(run))
    return found


def blocked_hits(text: str, blocked: frozenset[str]) -> bool:
    return any(hash_token(token) in blocked for token in tokens(text))


def tracked_files(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached"],
        check=True,
        capture_output=True,
    )
    names = [name for name in result.stdout.decode("utf-8").split("\0") if name]
    return [root / name for name in names]


def file_text(path: Path) -> str:
    data = path.read_bytes()
    if b"\0" not in data:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            pass
    # Binary: scan printable ASCII runs, like `strings`.
    return "\n".join(run.decode("ascii") for run in PRINTABLE_RUN.findall(data))


def parquet_strings(path: Path) -> Iterable[str]:
    import duckdb

    con = duckdb.connect()
    try:
        source = str(path).replace("'", "''")
        relation = f"read_parquet('{source}')"
        for key, value in con.execute(
            f"SELECT decode(key), decode(value) FROM parquet_kv_metadata('{source}')"
        ).fetchall():
            yield f"{key} {value}"
        for name, column_type, *_ in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall():
            yield name
            if "VARCHAR" not in column_type and "STRUCT" not in column_type:
                continue
            quoted = '"' + name.replace('"', '""') + '"'
            query = (
                f"SELECT DISTINCT CAST({quoted} AS VARCHAR) FROM {relation} "
                f"WHERE {quoted} IS NOT NULL"
            )
            for (value,) in con.execute(query).fetchall():
                yield value
    finally:
        con.close()


def scan_file(path: Path, blocked: frozenset[str]) -> bool:
    if path.suffix == ".parquet":
        return any(blocked_hits(text, blocked) for text in parquet_strings(path))
    return blocked_hits(file_text(path), blocked)


def data_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(item for item in path.rglob("*") if item.is_file())


def scan(
    root: Path = ROOT,
    data: Iterable[Path] = (),
    blocked: frozenset[str] = BLOCKED_SHA256,
) -> list[str]:
    files = [path for path in tracked_files(root) if path.is_file()]
    for path in data:
        if not path.exists():
            raise FileNotFoundError(path)
        files.extend(data_files(path))

    failures: list[str] = []
    for path in dict.fromkeys(files):
        if scan_file(path, blocked):
            display = path.relative_to(root) if path.is_relative_to(root) else path
            failures.append(f"{display}: blocked term hash match")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
        action="append",
        default=[],
        help="extra file or directory to scan, including Parquet columns and values",
    )
    parser.add_argument("--hash", metavar="TERM", help="print the hash for a new blocked term")
    args = parser.parse_args(argv)

    if args.hash:
        print(hash_token(args.hash))
        return 0

    failures = scan(ROOT, [path.resolve() for path in args.data])
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1

    print("name check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
