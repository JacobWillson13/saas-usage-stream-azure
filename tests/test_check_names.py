from __future__ import annotations

import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("check_names", ROOT / "scripts" / "check_names.py")
check_names = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_names)

PLANTED = "zqxplanted"
BLOCKED = frozenset({check_names.hash_token(PLANTED)})


class CheckNamesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        (self.root / "clean.md").write_text("a clean tracked file\n")
        self.track("clean.md")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def track(self, *names: str) -> None:
        subprocess.run(["git", "-C", str(self.root), "add", *names], check=True)

    def scan(self, data: list[Path] | None = None) -> list[str]:
        return check_names.scan(self.root, data or [], BLOCKED)

    def write_parquet(self, path: Path, select: str) -> None:
        con = duckdb.connect()
        con.execute(f"COPY ({select}) TO '{path}' (FORMAT PARQUET)")
        con.close()

    def test_clean_repo_passes(self) -> None:
        self.assertEqual(self.scan(), [])

    def test_planted_term_in_tracked_txt_fails(self) -> None:
        for text in [PLANTED, f"x/{PLANTED}.y", f"{PLANTED}_id", "Zqxplanted", "ZQXPLANTED"]:
            with self.subTest(text=text):
                (self.root / "notes.txt").write_text(f"some notes\n{text}\n")
                self.track("notes.txt")
                self.assertEqual(self.scan(), ["notes.txt: blocked term hash match"])

    def test_planted_term_in_camel_case_fails(self) -> None:
        (self.root / "app.js").write_text("const ZqxplantedId = 1;\n")
        self.track("app.js")
        self.assertEqual(self.scan(), ["app.js: blocked term hash match"])

    def test_planted_term_in_tracked_binary_fails(self) -> None:
        (self.root / "blob.bin").write_bytes(b"\x00\x01\xff " + PLANTED.encode() + b" \x00")
        self.track("blob.bin")
        self.assertEqual(self.scan(), ["blob.bin: blocked term hash match"])

    def test_untracked_file_is_ignored(self) -> None:
        (self.root / "scratch.txt").write_text(PLANTED)
        self.assertEqual(self.scan(), [])

    def test_planted_term_in_parquet_column_name_fails(self) -> None:
        data = self.root / "data"
        data.mkdir()
        self.write_parquet(data / "t.parquet", f"SELECT 1 AS {PLANTED}_id")
        self.assertEqual(self.scan([data]), ["data/t.parquet: blocked term hash match"])

    def test_planted_term_in_parquet_string_value_fails(self) -> None:
        data = self.root / "data"
        data.mkdir()
        self.write_parquet(
            data / "t.parquet",
            f"SELECT * FROM (VALUES ('ok'), ('{PLANTED}_value')) AS t(feature)",
        )
        self.assertEqual(self.scan([data]), ["data/t.parquet: blocked term hash match"])

    def test_tracked_parquet_is_scanned_without_data_flag(self) -> None:
        self.write_parquet(self.root / "t.parquet", f"SELECT '{PLANTED}' AS feature")
        self.track("t.parquet")
        self.assertEqual(self.scan(), ["t.parquet: blocked term hash match"])

    def test_clean_parquet_passes(self) -> None:
        data = self.root / "data"
        data.mkdir()
        self.write_parquet(data / "t.parquet", "SELECT 'acc_0123' AS account_id, 3 AS n")
        self.assertEqual(self.scan([data]), [])


if __name__ == "__main__":
    unittest.main()
