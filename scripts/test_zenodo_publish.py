#!/usr/bin/env python3
"""Tests for zenodo_publish.py: packaging, and the upload flow against a fake
InvenioRDM files API.

    python3 scripts/test_zenodo_publish.py
"""

import csv
import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import threading
import unittest
import zipfile
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import zenodo_publish as zp

TODAY = date(2026, 10, 2)


def make_tree(root: str, table: str, first: date, last: date, skip=()) -> None:
    """One daily file per day in [first, last], v2 layout."""
    d = first
    while d <= last:
        if d not in skip:
            folder = os.path.join(root, "v2", f"table={table}", f"year={d.year}", f"month={d.month:02d}")
            os.makedirs(folder, exist_ok=True)
            nxt = d + timedelta(days=1)
            with open(os.path.join(folder, f"{table}_{d}_{nxt}.parquet"), "wb") as f:
                f.write(f"{table} {d}".encode() * 100)
        d += timedelta(days=1)


class PackageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.src = os.path.join(self.tmp, "exports")
        make_tree(self.src, "metadata", date(2026, 2, 4), date(2026, 3, 31))

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_months_parse(self):
        self.assertEqual(zp.parse_months("2026-11..2027-02"), ["2026-11", "2026-12", "2027-01", "2027-02"])
        self.assertEqual(zp.parse_months("2026-02,2026-05"), ["2026-02", "2026-05"])

    def test_first_month_starts_at_first_file(self):
        files = zp.month_files(self.src, "metadata", "2026-02", TODAY)
        self.assertEqual(len(files), 25)  # Feb 4..28

    def test_gap_is_rejected(self):
        os.remove(os.path.join(self.src, "v2/table=metadata/year=2026/month=03/metadata_2026-03-10_2026-03-11.parquet"))
        with self.assertRaisesRegex(ValueError, "missing 2026-03-10 .. 2026-03-10"):
            zp.month_files(self.src, "metadata", "2026-03", TODAY)

    def test_trailing_gap_is_rejected(self):
        os.remove(os.path.join(self.src, "v2/table=metadata/year=2026/month=03/metadata_2026-03-31_2026-04-01.parquet"))
        with self.assertRaisesRegex(ValueError, "missing 2026-03-31"):
            zp.month_files(self.src, "metadata", "2026-03", TODAY)

    def test_unfinished_month_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "not over"):
            zp.month_files(self.src, "metadata", "2026-03", date(2026, 3, 31))

    def test_zip_contents_and_manifest(self):
        path, md5 = zp.package(self.src, os.path.join(self.tmp, "out"), "metadata", "2026-03", TODAY)
        self.assertEqual(os.path.basename(path), "metadata_2026-03.zip")
        self.assertEqual(md5, hashlib.md5(Path(path).read_bytes()).hexdigest())
        self.assertEqual(zp.read_md5_sidecar(path), md5)
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
            self.assertEqual(len(names), 32)
            self.assertTrue(all(i.compress_type == zipfile.ZIP_STORED for i in zf.infolist()))
            self.assertEqual(names[0], "v2/table=metadata/year=2026/month=03/metadata_2026-03-01_2026-03-02.parquet")
            rows = list(csv.DictReader(io.StringIO(zf.read("v2/table=metadata/year=2026/month=03/MANIFEST.csv").decode())))
            self.assertEqual(len(rows), 31)
            for r in rows:
                data = zf.read(r["path"])
                self.assertEqual(Path(self.src, r["path"]).read_bytes(), data)
                self.assertEqual(hashlib.md5(data).hexdigest(), r["md5"])
                self.assertEqual(len(data), int(r["bytes"]))

    def test_zip_is_deterministic(self):
        _, a = zp.package(self.src, os.path.join(self.tmp, "a"), "metadata", "2026-03", TODAY)
        for root, _, files in os.walk(self.src):
            for f in files:
                os.utime(os.path.join(root, f), (1, 1))
        _, b = zp.package(self.src, os.path.join(self.tmp, "b"), "metadata", "2026-03", TODAY)
        self.assertEqual(a, b)


class FakeZenodo:
    """Just enough of the InvenioRDM draft/files API, in memory."""

    def __init__(self):
        self.drafts: dict[str, dict[str, dict]] = {}
        self.published: dict[str, dict[str, dict]] = {}
        self.corrupt_next_puts = 0
        self.puts: list[str] = []
        self.user_agents: list[str | None] = []
        self.inits: list[dict] = []
        self.init_504_once = False  # create the entry, then answer 504 (seen on zenodo.org)
        self.next_id = 100
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code, obj=None):
                body = b"" if obj is None else json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _route(self):
                fake.user_agents.append(self.headers.get("User-Agent"))
                return [unquote(p) for p in self.path.split("?")[0].strip("/").split("/")]

            def do_GET(self):
                p = self._route()  # api records ID draft files | api records ID files
                if p[3:] == ["draft", "files"]:
                    files = fake.drafts.get(p[2])
                    if files is None:
                        return self._send(404)
                    public = [{k: v for k, v in e.items() if not k.startswith("_")} for e in files.values()]
                    return self._send(200, {"entries": public})
                if p[3:] == ["files"]:
                    return self._send(200, {"entries": list(fake.published[p[2]].values())})
                self._send(404)

            def do_POST(self):
                p = self._route()
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n)) if n else None
                rid = p[2]
                if p[3:] == ["draft", "files"]:
                    for e in body:
                        fake.inits.append(e)
                        if e["key"] in fake.drafts[rid]:
                            return self._send(400, {"status": 400, "message": f"File with key {e['key']} already exists."})
                        fake.drafts[rid][e["key"]] = {"key": e["key"], "status": "pending"}
                    if fake.init_504_once:
                        fake.init_504_once = False
                        return self._send(504, {"message": "Gateway Time-out"})
                    return self._send(201, {})
                if len(p) == 7 and p[6] == "commit":
                    e = fake.drafts[rid][p[5]]
                    e["checksum"] = "md5:" + hashlib.md5(e.pop("_data")).hexdigest()
                    e["status"] = "completed"
                    return self._send(200, {k: v for k, v in e.items() if not k.startswith("_")})
                if p[3:] == ["versions"]:
                    new = str(fake.next_id)
                    fake.next_id += 1
                    fake.drafts[new] = {}
                    return self._send(201, {"id": new})
                if p[3:] == ["draft", "actions", "files-import"]:
                    fake.drafts[rid] = {k: dict(v) for k, v in fake.published[fake.parent[rid]].items()}
                    return self._send(201, {})
                self._send(404)

            def do_PUT(self):
                p = self._route()
                data = self.rfile.read(int(self.headers["Content-Length"]))
                fake.puts.append(p[5])
                if fake.corrupt_next_puts:
                    fake.corrupt_next_puts -= 1
                    data = data[:-1] + bytes([data[-1] ^ 0xFF])
                e = fake.drafts[p[2]][p[5]]
                e["_data"], e["size"] = data, len(data)
                self._send(200, {})

            def do_DELETE(self):
                p = self._route()
                if fake.drafts[p[2]].pop(p[5], None) is None:
                    return self._send(404)
                self._send(204)

        self.parent: dict[str, str] = {}
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/api"

    def close(self):
        self.server.shutdown()


class UploadTest(unittest.TestCase):
    def setUp(self):
        self.fake = FakeZenodo()
        self.fake.drafts["1"] = {}
        self.z = zp.Zenodo(self.fake.url, "tok", timeout=10, retries=2, backoff=0)
        self.tmp = tempfile.mkdtemp()
        self.file = os.path.join(self.tmp, "timings_2026-03.zip")
        with open(self.file, "wb") as f:
            f.write(os.urandom(300_000))

    def tearDown(self):
        self.fake.close()
        shutil.rmtree(self.tmp)

    def test_upload_verifies_and_is_idempotent(self):
        self.assertTrue(zp.upload_file(self.z, "1", self.file))
        e = self.fake.drafts["1"]["timings_2026-03.zip"]
        self.assertEqual(e["checksum"], "md5:" + zp.md5_file(self.file))
        self.assertFalse(zp.upload_file(self.z, "1", self.file))
        self.assertEqual(len(self.fake.puts), 1)

    def test_every_request_sends_a_user_agent(self):
        # zenodo.org blocks UA-less requests with an HTML 403; the sandbox does not.
        zp.upload_file(self.z, "1", self.file)
        self.assertTrue(self.fake.user_agents)
        self.assertTrue(all(ua and ua.startswith("gossip_observer") for ua in self.fake.user_agents))

    def test_create_504_that_created_the_entry_recovers(self):
        # zenodo.org: the create POST times out at the gateway but the entry
        # exists, so call()'s retry gets "already exists". The attempt must be
        # cleaned up and redone, not end the upload.
        self.fake.init_504_once = True
        self.assertTrue(zp.upload_file(self.z, "1", self.file))
        self.assertEqual(len(self.fake.puts), 1)
        self.assertEqual(self.fake.drafts["1"]["timings_2026-03.zip"]["checksum"], "md5:" + zp.md5_file(self.file))

    def test_backoff_doubles_up_to_cap(self):
        z = zp.Zenodo(self.fake.url, "tok", backoff=30, max_backoff=600)
        self.assertEqual([z.wait(a) for a in range(7)], [30, 60, 120, 240, 480, 600, 600])

    def test_retries_flag_sets_attempts(self):
        self.fake.corrupt_next_puts = 99
        argv = ["--api", self.fake.url, "--backoff", "0", "--retries", "3", "upload", "--record-id", "1", self.file]
        os.environ["ZENODO_TOKEN"] = "tok"
        with self.assertRaisesRegex(RuntimeError, "giving up after 4 attempts"):
            zp.main(argv)
        self.assertEqual(len(self.fake.puts), 4)

    def test_checksum_mismatch_is_retried(self):
        self.fake.corrupt_next_puts = 1
        self.assertTrue(zp.upload_file(self.z, "1", self.file))
        self.assertEqual(len(self.fake.puts), 2)
        self.assertEqual(self.fake.drafts["1"]["timings_2026-03.zip"]["checksum"], "md5:" + zp.md5_file(self.file))

    def test_persistent_mismatch_gives_up(self):
        self.fake.corrupt_next_puts = 99
        with self.assertRaisesRegex(RuntimeError, "giving up after 3 attempts"):
            zp.upload_file(self.z, "1", self.file)
        self.assertNotIn("timings_2026-03.zip", self.fake.drafts["1"])

    def test_changed_file_replaces_entry(self):
        zp.upload_file(self.z, "1", self.file)
        with open(self.file, "ab") as f:
            f.write(b"more")
        self.assertTrue(zp.upload_file(self.z, "1", self.file))
        self.assertEqual(self.fake.drafts["1"]["timings_2026-03.zip"]["size"], 300_004)

    def test_capacity_check(self):
        for i in range(99):
            self.fake.drafts["1"][f"f{i}"] = {"key": f"f{i}", "status": "completed"}
        zp.check_capacity(self.z, "1", ["f0", "new"])
        with self.assertRaisesRegex(RuntimeError, "101 files"):
            zp.check_capacity(self.z, "1", ["new", "new2"])

    def test_new_version_imports_previous_files(self):
        self.fake.published["1"] = {"a.zip": {"key": "a.zip", "checksum": "md5:aa", "status": "completed"}}
        self.fake.parent["100"] = "1"

        class A:
            record_id = "1"

        self.assertEqual(zp.cmd_new_version(self.z, A()), 0)
        self.assertEqual(set(self.fake.drafts["100"]), {"a.zip"})

    def test_run_packages_uploads_and_resumes(self):
        src = os.path.join(self.tmp, "exports")
        for t in zp.TABLES:
            make_tree(src, t, date(2026, 2, 4), date(2026, 3, 31))
        extra = os.path.join(self.tmp, "collectors.csv")
        with open(extra, "w") as f:
            f.write("collector_id\nc01\n")
        argv = ["--api", self.fake.url, "--backoff", "0", "run", "--record-id", "1", "--src", src,
                "--work-dir", os.path.join(self.tmp, "work"), "--months", "2026-02..2026-03",
                "--today", "2026-10-02", "--replace", extra]
        os.environ["ZENODO_TOKEN"] = "tok"
        self.assertEqual(zp.main(argv), 0)
        self.assertEqual(len(self.fake.drafts["1"]), 11)
        self.assertEqual(os.listdir(os.path.join(self.tmp, "work")), ["uploaded-1.json"])
        # A resumed run must neither re-upload nor re-package finished months.
        puts = len(self.fake.puts)
        packaged = []
        real = zp.package
        zp.package = lambda *a: packaged.append(a) or real(*a)
        try:
            self.assertEqual(zp.main(argv), 0)
        finally:
            zp.package = real
        self.assertEqual(len(self.fake.puts), puts)
        self.assertEqual(packaged, [])


if __name__ == "__main__":
    unittest.main()
