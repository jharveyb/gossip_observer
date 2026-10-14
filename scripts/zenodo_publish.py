#!/usr/bin/env python3
"""Package the v2 Parquet exports into monthly ZIPs and upload them to a Zenodo draft.

    export ZENODO_TOKEN=...            # or --token-file; scope deposit:write
    zenodo_publish.py [--sandbox] status      --record-id ID
    zenodo_publish.py [--sandbox] package     --src EXPORTS --out DIR --months 2026-02..2026-08
    zenodo_publish.py [--sandbox] upload      --record-id ID FILE...
    zenodo_publish.py [--sandbox] new-version --record-id PUBLISHED_ID
    zenodo_publish.py [--sandbox] run         --record-id ID --src EXPORTS --work-dir DIR \\
                                              --months 2026-02..2026-08 [--replace collectors.csv]

Stdlib only, so it runs on any cluster host without installing anything.

Zenodo constraints this is shaped around (see the plan / Zenodo docs):
- max 100 files per record, no folders: one store-mode ZIP per table per month,
  whose entries keep the v2 layout (v2/table=../year=../month=../*.parquet), so
  unzipping every ZIP into one directory rebuilds the tree;
- files are frozen once published: this script never publishes. Review the
  draft in the web UI and publish there;
- zenodo.org's proxy drops long uploads at random (nginx 502 / 504): files go up
  one at a time in a single request each, verified against Zenodo's md5, and a
  dropped or mismatched upload is deleted and retried (--retries).
  InvenioRDM multipart ("M") transfers are not usable there: Zenodo's EOS
  storage preallocates the whole file inside the init request, which outlasts
  the 30 s gateway timeout, and its staged-upload path excludes multipart.

Uses the InvenioRDM draft-files API (/api/records/{id}/draft/files); the legacy
/api/deposit API is slated for deprecation.

ZIPs are deterministic (sorted entries, timestamps taken from the file names),
so re-packaging a month after an interrupted run yields the same md5.
"""

import argparse
import csv
import glob
import hashlib
import http.client
import io
import json
import os
import re
import sys
import time
import urllib.parse
import zipfile
from datetime import date, datetime, timedelta, timezone

TABLES = ["timings", "messages", "metadata", "message_hashes", "message_first_seen"]
MAX_FILES = 100
CHUNK = 8 << 20
_FILE_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.parquet$")

PROD_API = "https://zenodo.org/api"
SANDBOX_API = "https://sandbox.zenodo.org/api"
# http.client sends no User-Agent at all, and zenodo.org (not the sandbox)
# answers UA-less requests with an HTML 403 "unusual traffic from your network".
# Opt-in to any rate limiting or throttling of scripted usage.
# https://blog.zenodo.org/2026/09/15/2026-09-15-stability-and-performance-updates/
USER_AGENT = (
    "gossip_observer-zenodo_publish/1.0 (+https://github.com/jharveyb/gossip_observer)"
)


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%SZ} {msg}", flush=True)


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Packaging
# ---------------------------------------------------------------------------


def parse_months(spec: str) -> list[str]:
    """'2026-02..2026-04' or '2026-02,2026-05' -> ['2026-02', ...]."""
    out = []
    for part in spec.split(","):
        if ".." in part:
            lo, hi = part.split("..")
            y, m = map(int, lo.split("-"))
            while f"{y:04d}-{m:02d}" <= hi:
                out.append(f"{y:04d}-{m:02d}")
                y, m = (y + 1, 1) if m == 12 else (y, m + 1)
        else:
            out.append(part)
    for ym in out:
        if not re.fullmatch(r"\d{4}-\d{2}", ym):
            raise ValueError(f"bad month {ym!r}")
    return out


def _month_bounds(ym: str) -> tuple[date, date]:
    y, m = map(int, ym.split("-"))
    return date(y, m, 1), date(y + (m == 12), m % 12 + 1, 1)


def table_files(src: str, table: str) -> list[tuple[str, date, date]]:
    """(path, start, end) for every v2 file of `table`, sorted by start."""
    out = []
    pattern = os.path.join(
        src, "v2", f"table={table}", "year=*", "month=*", "*.parquet"
    )
    for path in glob.glob(pattern):
        m = _FILE_RE.search(path)
        if not m:
            raise ValueError(f"unexpected file name: {path}")
        out.append(
            (path, date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2)))
        )
    return sorted(out, key=lambda f: f[1])


def month_files(
    src: str, table: str, ym: str, today: date
) -> list[tuple[str, date, date]]:
    """The files of one table-month, after checking the month is complete.

    Complete means the month has ended and its files cover every day of it with
    no gap. The archive's first month counts from the table's first file, not
    from the 1st. A file belongs to the month folder of its start day (that is
    where the exporter writes it), so selection is by folder.
    """
    all_files = table_files(src, table)
    if not all_files:
        raise ValueError(f"{table}: no files under {src}")
    y, m = ym.split("-")
    folder = os.path.join(src, "v2", f"table={table}", f"year={y}", f"month={m}")
    files = [f for f in all_files if os.path.dirname(f[0]) == folder]
    month_start, month_end = _month_bounds(ym)
    if month_end > today:
        raise ValueError(f"{table} {ym}: month not over yet")
    cursor = max(month_start, all_files[0][1])
    for _, start, end in files:
        if start > cursor:
            raise ValueError(
                f"{table} {ym}: missing {cursor} .. {start - timedelta(days=1)}"
            )
        cursor = max(cursor, end)
    if cursor < month_end:
        raise ValueError(
            f"{table} {ym}: missing {cursor} .. {month_end - timedelta(days=1)}"
        )
    return files


def zip_name(table: str, ym: str) -> str:
    return f"{table}_{ym}.zip"


def package(
    src: str, out_dir: str, table: str, ym: str, today: date
) -> tuple[str, str]:
    """Write {table}_{ym}.zip (+ .md5) into out_dir. Returns (path, md5)."""
    files = month_files(src, table, ym, today)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, zip_name(table, ym))
    tmp = path + ".part"
    manifest = io.StringIO()
    w = csv.writer(manifest, lineterminator="\n")
    w.writerow(["path", "bytes", "md5"])
    month_dir = None
    with zipfile.ZipFile(
        tmp, "w", compression=zipfile.ZIP_STORED, allowZip64=True
    ) as zf:
        for fpath, start, _ in files:
            arcname = os.path.relpath(fpath, src).replace(os.sep, "/")
            month_dir = arcname.rsplit("/", 1)[0]
            info = zipfile.ZipInfo(
                arcname, date_time=(start.year, start.month, start.day, 0, 0, 0)
            )
            info.compress_type = zipfile.ZIP_STORED
            info.external_attr = 0o644 << 16
            info.file_size = os.path.getsize(fpath)
            h = hashlib.md5()
            with open(fpath, "rb") as fin, zf.open(info, "w") as fout:
                while chunk := fin.read(CHUNK):
                    h.update(chunk)
                    fout.write(chunk)
            w.writerow([arcname, info.file_size, h.hexdigest()])
        first = _month_bounds(ym)[0]
        info = zipfile.ZipInfo(
            f"{month_dir}/MANIFEST.csv", date_time=(first.year, first.month, 1, 0, 0, 0)
        )
        info.external_attr = 0o644 << 16
        zf.writestr(info, manifest.getvalue())
    digest = md5_file(tmp)
    os.replace(tmp, path)
    with open(path + ".md5", "w") as f:
        f.write(f"{digest}  {os.path.basename(path)}\n")
    log(
        f"packaged {os.path.basename(path)}: {len(files)} files, {os.path.getsize(path):,} bytes, md5 {digest}"
    )
    return path, digest


def read_md5_sidecar(path: str) -> str | None:
    try:
        with open(path + ".md5") as f:
            return f.read().split()[0]
    except FileNotFoundError:
        return None


# ---------------------------------------------------------------------------
# Zenodo (InvenioRDM) client
# ---------------------------------------------------------------------------


class ApiError(Exception):
    def __init__(self, status: int, body: str, what: str):
        super().__init__(f"{what}: HTTP {status}: {body[:500]}")
        self.status = status


class Zenodo:
    def __init__(
        self,
        base: str,
        token: str,
        timeout: float = 300,
        retries: int = 10,
        backoff: float = 30,
        max_backoff: float = 600,
    ):
        u = urllib.parse.urlsplit(base)
        self.scheme, self.netloc, self.prefix = u.scheme, u.netloc, u.path.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.max_backoff = max_backoff

    def wait(self, attempt: int) -> float:
        """Delay before retry `attempt + 1`: doubling, capped."""
        return min(self.backoff * 2**attempt, self.max_backoff)

    def _conn(self) -> http.client.HTTPConnection:
        cls = (
            http.client.HTTPSConnection
            if self.scheme == "https"
            else http.client.HTTPConnection
        )
        return cls(self.netloc, timeout=self.timeout, blocksize=1 << 20)

    def _once(self, method: str, path: str, body=None, headers=None):
        h = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        h.update(headers or {})
        conn = self._conn()
        try:
            try:
                conn.request(method, self.prefix + path, body=body, headers=h)
            except (BrokenPipeError, ConnectionResetError) as e:
                # The server hung up mid-body. It often says why first (413,
                # 408, a proxy's HTML page); surface that instead of EPIPE.
                try:
                    resp = conn.getresponse()
                    data = resp.read().decode("utf-8", "replace")
                except (OSError, http.client.HTTPException):
                    raise e from None
                raise ApiError(
                    resp.status,
                    data,
                    f"{method} {path} (server closed the connection mid-upload)",
                ) from e
            resp = conn.getresponse()
            data = resp.read().decode("utf-8", "replace")
        finally:
            conn.close()
        if resp.status >= 400:
            raise ApiError(resp.status, data, f"{method} {path}")
        return json.loads(data) if data.strip() else None

    def call(self, method: str, path: str, payload=None):
        """JSON request, retried on connection errors, 429 and 5xx."""
        body = None if payload is None else json.dumps(payload)
        headers = {} if payload is None else {"Content-Type": "application/json"}
        for attempt in range(self.retries + 1):
            try:
                return self._once(method, path, body, headers)
            except ApiError as e:
                if e.status != 429 and e.status < 500 or attempt == self.retries:
                    raise
                err = e
            except (OSError, http.client.HTTPException) as e:
                if attempt == self.retries:
                    raise
                err = e
            wait = self.wait(attempt)
            log(f"{method} {path} failed ({err}); retrying in {wait:.0f}s")
            time.sleep(wait)

    def put_content(self, path: str, local: str) -> None:
        """Stream one file. Not retried here: the caller restarts the whole file."""
        size = os.path.getsize(local)
        with open(local, "rb") as f:
            self._once(
                "PUT",
                path,
                body=_Progress(f, size, os.path.basename(local)),
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(size),
                },
            )

    # -- endpoints ---------------------------------------------------------

    @staticmethod
    def _key(key: str) -> str:
        return urllib.parse.quote(key, safe="")

    def draft_files(self, rid: str) -> dict[str, dict]:
        res = self.call("GET", f"/records/{rid}/draft/files")
        return {e["key"]: e for e in res.get("entries", [])}

    def published_files(self, rid: str) -> dict[str, dict]:
        res = self.call("GET", f"/records/{rid}/files")
        return {e["key"]: e for e in res.get("entries", [])}

    def delete_file(self, rid: str, key: str) -> None:
        self.call("DELETE", f"/records/{rid}/draft/files/{self._key(key)}")

    def new_version(self, rid: str) -> str:
        return self.call("POST", f"/records/{rid}/versions")["id"]

    def import_files(self, rid: str) -> None:
        self.call("POST", f"/records/{rid}/draft/actions/files-import")


class _Progress(io.RawIOBase):
    """File wrapper that logs upload progress every ~5% / 60s."""

    def __init__(self, f, size: int, name: str):
        self.f, self.size, self.name = f, size, name
        self.sent, self.t0, self.last = 0, time.monotonic(), 0.0

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        chunk = self.f.read(n)
        self.sent += len(chunk)
        now = time.monotonic()
        if chunk and (now - self.last > 60 or self.sent == self.size):
            self.last = now
            rate = self.sent / max(now - self.t0, 1e-6) / 1e6
            log(
                f"  {self.name}: {self.sent / max(self.size, 1):6.1%} of {self.size:,} bytes, {rate:.1f} MB/s"
            )
        return chunk


def _file_path(z: Zenodo, rid: str, key: str) -> str:
    return f"/records/{rid}/draft/files/{z._key(key)}"


def _transfer_single(z: Zenodo, rid: str, key: str, local: str, size: int) -> None:
    z.call("POST", f"/records/{rid}/draft/files", [{"key": key}])
    log(f"{key}: uploading {size:,} bytes in one request")
    z.put_content(_file_path(z, rid, key) + "/content", local)


def upload_file(
    z: Zenodo, rid: str, local: str, md5: str | None = None, key: str | None = None
) -> bool:
    """Upload `local` to draft `rid` as `key`, verified against Zenodo's md5.

    Idempotent: an existing completed entry with the same md5 and size is left
    alone (returns False). Any other existing entry under the key is deleted
    first, so this also replaces a file. Returns True if it uploaded.
    """
    key = key or os.path.basename(local)
    md5 = md5 or read_md5_sidecar(local) or md5_file(local)
    size = os.path.getsize(local)
    want = f"md5:{md5}"
    existing = z.draft_files(rid).get(key)
    if existing:
        if (
            existing.get("status") == "completed"
            and existing.get("checksum") == want
            and existing.get("size") == size
        ):
            log(f"{key}: already in draft {rid} with matching md5, skipping")
            return False
        log(
            f"{key}: replacing existing draft entry ({existing.get('status')}, {existing.get('checksum')})"
        )
        z.delete_file(rid, key)
    for attempt in range(z.retries + 1):
        try:
            log(f"{key}: attempt {attempt + 1} of {z.retries + 1}")
            _transfer_single(z, rid, key, local, size)
            z.call("POST", _file_path(z, rid, key) + "/commit")
            got = z.draft_files(rid).get(key) or {}
            if got.get("checksum") == want and got.get("size") == size:
                log(f"{key}: verified {want}")
                return True
            err = f"verification failed: zenodo has {got.get('checksum')} / {got.get('size')}, local {want} / {size}"
        except (ApiError, OSError, http.client.HTTPException) as e:
            err = str(e)
        # Never leave a failed entry behind: a committed file with the wrong
        # checksum looks "completed" in the UI and could be published.
        log(f"{key}: {err}; deleting the draft entry")
        try:
            z.delete_file(rid, key)
        except ApiError as e:
            if e.status != 404:
                log(f"{key}: delete failed: {e}")
        if attempt == z.retries:
            raise RuntimeError(f"{key}: giving up after {attempt + 1} attempts: {err}")
        wait = z.wait(attempt)
        log(f"{key}: retrying in {wait:.0f}s")
        time.sleep(wait)
    raise AssertionError("unreachable")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_status(z: Zenodo, args) -> int:
    files = z.draft_files(args.record_id)
    total = 0
    for key, e in sorted(files.items()):
        total += e.get("size") or 0
        print(
            f"{key:40} {e.get('size') or 0:>16,} {e.get('status', ''):10} {e.get('checksum', '')}"
        )
    print(
        f"{len(files)} files ({MAX_FILES} max), {total:,} bytes ({total / 1e9:.1f} GB)"
    )
    return 0


def cmd_package(args) -> int:
    for ym in parse_months(args.months):
        for table in args.tables:
            package(args.src, args.out, table, ym, args.today)
    return 0


def cmd_upload(z: Zenodo, args) -> int:
    check_capacity(z, args.record_id, [os.path.basename(p) for p in args.files])
    for p in args.files:
        upload_file(z, args.record_id, p)
    return 0


def cmd_new_version(z: Zenodo, args) -> int:
    """Create a new-version draft and carry the previous version's files over.

    A new-version draft starts EMPTY: Zenodo does not import the previous files
    on its own. files-import links them (no re-upload, no extra quota).
    """
    prev = z.published_files(args.record_id)
    new = z.new_version(args.record_id)
    log(f"new version draft {new} of {args.record_id}")
    if not z.draft_files(new):
        z.import_files(new)
    got = z.draft_files(new)
    if set(got) != set(prev):
        raise RuntimeError(
            f"draft {new} files differ from {args.record_id}: "
            f"missing {sorted(set(prev) - set(got))}, extra {sorted(set(got) - set(prev))}"
        )
    for k, e in prev.items():
        if got[k].get("checksum") != e.get("checksum"):
            raise RuntimeError(
                f"draft {new}: {k} checksum {got[k].get('checksum')} != {e.get('checksum')}"
            )
    log(f"draft {new}: imported {len(got)} files from {args.record_id}")
    print(new)
    return 0


def check_capacity(z: Zenodo, rid: str, keys: list[str]) -> None:
    present = set(z.draft_files(rid))
    total = len(present | set(keys))
    if total > MAX_FILES:
        raise RuntimeError(
            f"draft {rid} would hold {total} files; Zenodo allows {MAX_FILES}"
        )


def cmd_run(z: Zenodo, args) -> int:
    months = parse_months(args.months)
    # Fail fast: every month must be complete before anything is uploaded.
    for ym in months:
        for table in args.tables:
            month_files(args.src, table, ym, args.today)
    keys = [zip_name(t, ym) for ym in months for t in args.tables]
    keys += [os.path.basename(p) for p in args.replace]
    check_capacity(z, args.record_id, keys)

    state_path = os.path.join(args.work_dir, f"uploaded-{args.record_id}.json")
    os.makedirs(args.work_dir, exist_ok=True)
    try:
        with open(state_path) as f:
            state = json.load(f)
    except FileNotFoundError:
        state = {}

    for ym in months:
        for table in args.tables:
            key = zip_name(table, ym)
            entry = z.draft_files(args.record_id).get(key)
            done = state.get(key)
            if (
                done
                and entry
                and entry.get("status") == "completed"
                and entry.get("checksum") == f"md5:{done['md5']}"
            ):
                log(f"{key}: already uploaded and verified, skipping")
                continue
            local = os.path.join(args.work_dir, key)
            md5 = read_md5_sidecar(local) if os.path.exists(local) else None
            if md5 is None:
                local, md5 = package(args.src, args.work_dir, table, ym, args.today)
            upload_file(z, args.record_id, local, md5)
            state[key] = {"md5": md5, "size": os.path.getsize(local)}
            with open(state_path + ".tmp", "w") as f:
                json.dump(state, f, indent=1, sort_keys=True)
            os.replace(state_path + ".tmp", state_path)
            if not args.keep_zips:
                os.remove(local)
                os.remove(local + ".md5")
    for p in args.replace:
        upload_file(z, args.record_id, p)
    log("done; review the draft in the web UI and publish it there")
    return cmd_status(z, args)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--sandbox", action="store_true", help="use sandbox.zenodo.org")
    ap.add_argument(
        "--retries",
        type=int,
        default=10,
        help="retries per file and per API call after the first try (default 10)",
    )
    ap.add_argument("--api", help="API base URL (overrides --sandbox)")
    ap.add_argument(
        "--token-file", help="file holding the token (default: $ZENODO_TOKEN)"
    )
    ap.add_argument(
        "--backoff",
        type=float,
        default=30,
        help="first retry delay in seconds, doubling up to 600 (default 30)",
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    def months_args(p):
        p.add_argument("--src", required=True, help="export root containing v2/")
        p.add_argument(
            "--months", required=True, help="2026-02..2026-08 or 2026-02,2026-03"
        )
        p.add_argument("--tables", nargs="+", default=TABLES, choices=TABLES)
        p.add_argument(
            "--today",
            type=date.fromisoformat,
            default=datetime.now(timezone.utc).date(),
            help=argparse.SUPPRESS,
        )

    p = sub.add_parser("status", help="list the draft's files")
    p.add_argument("--record-id", required=True)
    p = sub.add_parser("package", help="build monthly ZIPs locally")
    months_args(p)
    p.add_argument("--out", required=True)
    p = sub.add_parser("upload", help="upload files to a draft")
    p.add_argument("--record-id", required=True)
    p.add_argument("files", nargs="+")
    p = sub.add_parser(
        "new-version", help="new-version draft with the previous files imported"
    )
    p.add_argument("--record-id", required=True, help="the latest PUBLISHED version")
    p = sub.add_parser(
        "run", help="package + upload + verify month by month, resumable"
    )
    p.add_argument("--record-id", required=True)
    months_args(p)
    p.add_argument(
        "--work-dir", required=True, help="scratch for ZIPs and upload state"
    )
    p.add_argument(
        "--replace",
        nargs="*",
        default=[],
        help="extra files (e.g. collectors.csv) to add or replace",
    )
    p.add_argument("--keep-zips", action="store_true")
    args = ap.parse_args(argv)

    if args.cmd == "package":
        return cmd_package(args)
    if args.token_file:
        with open(args.token_file) as f:
            token = f.read().strip()
    else:
        token = os.environ.get("ZENODO_TOKEN", "")
    if not token:
        ap.error("no token: set ZENODO_TOKEN or pass --token-file")
    z = Zenodo(
        args.api or (SANDBOX_API if args.sandbox else PROD_API),
        token,
        retries=args.retries,
        backoff=args.backoff,
    )
    return {
        "status": cmd_status,
        "upload": cmd_upload,
        "new-version": cmd_new_version,
        "run": cmd_run,
    }[args.cmd](z, args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, RuntimeError, ApiError) as e:
        log(f"error: {e}")
        sys.exit(1)
