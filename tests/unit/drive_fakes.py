"""In-memory fake of the raw Google Drive v3 service (method-chain style).

It understands just enough of the API for the attachment pipeline: the
``q`` clauses the code sends, pagination, roles, ownership transfer,
permission listing, shared drives and batch requests.  Every call is logged
in ``calls`` as ``(method, kwargs)`` so tests can count round trips.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from googleapiclient.errors import HttpError
from httplib2 import Response

FOLDER_MIME = "application/vnd.google-apps.folder"
VALID_ROLES = {"owner", "organizer", "fileOrganizer", "writer", "commenter", "reader"}


def http_error(status: int, message: str = "error") -> HttpError:
    return HttpError(Response({"status": str(status)}), message.encode())


class Req:
    """A request whose ``execute()`` runs the stored function."""

    def __init__(self, fn, method: str, kwargs: dict[str, Any]):
        self._fn = fn
        self.method = method
        self.kwargs = kwargs

    def execute(self):
        return self._fn()


class FakeBatch:
    def __init__(self, drive: FakeDrive, callback=None):
        self.drive = drive
        self.callback = callback
        self.requests: list[tuple[str, Req, Any]] = []

    def add(self, request, callback=None, request_id=None):
        rid = request_id or str(len(self.requests))
        self.requests.append((rid, request, callback or self.callback))

    def execute(self):
        self.drive.calls.append(("batch.execute", {"size": len(self.requests)}))
        self.drive.batch_sizes.append(len(self.requests))
        for rid, request, callback in self.requests:
            try:
                response, error = request.execute(), None
            except HttpError as e:
                response, error = None, e
            if callback is not None:
                callback(rid, response, error)


class FakeDrive:
    """Raw Drive service fake: ``drive.files().list(q=...).execute()``."""

    def __init__(
        self,
        drives: list[dict[str, Any]] | None = None,
        *,
        fail_transfer: bool = False,
        drive_page_cap: int | None = None,
        file_page_cap: int | None = None,
        fail_conversion: bool = False,
        reject_inherited_flag_on_create: bool = False,
    ):
        self.files_db: dict[str, dict[str, Any]] = {}
        self.permissions_db: dict[str, list[dict[str, Any]]] = {}
        self.drives_db = list(drives or [])
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.batch_sizes: list[int] = []
        self.counter = 0
        self.fail_transfer = fail_transfer
        self.drive_page_cap = drive_page_cap
        self.file_page_cap = file_page_cap
        self.fail_conversion = fail_conversion
        self.reject_inherited_flag_on_create = reject_inherited_flag_on_create

    # -- helpers ------------------------------------------------------------

    def add_file(self, file_id: str, **fields: Any) -> dict[str, Any]:
        record = {"id": file_id, "parents": [], "trashed": False, **fields}
        record.setdefault("webViewLink", f"https://drive.example/{file_id}")
        self.files_db[file_id] = record
        return record

    def add_folder(self, file_id: str, name: str, parent: str, **fields: Any):
        return self.add_file(
            file_id, name=name, parents=[parent], mimeType=FOLDER_MIME, **fields
        )

    def count(self, method: str) -> int:
        return sum(1 for name, _ in self.calls if name == method)

    def permission_calls(self) -> list[dict[str, Any]]:
        return [kw for name, kw in self.calls if name == "permissions.create"]

    # -- resources ----------------------------------------------------------

    def files(self):
        return _Files(self)

    def permissions(self):
        return _Permissions(self)

    def drives(self):
        return _Drives(self)

    def new_batch_http_request(self, callback=None):
        return FakeBatch(self, callback)


def _matches(record: dict[str, Any], q: str) -> bool:
    m = re.search(r"'((?:[^'\\]|\\.)*)' in parents", q)
    if m and m.group(1).replace("\\'", "'") not in record.get("parents", []):
        return False
    m = re.search(r"md5Checksum ?= ?'([^']+)'", q)
    if m and record.get("md5Checksum") != m.group(1):
        return False
    m = re.search(r"name ?= ?'((?:[^'\\]|\\.)*)'", q)
    if m and record.get("name") != m.group(1).replace("\\'", "'").replace("\\\\", "\\"):
        return False
    if re.search(r"mimeType ?= ?'application/vnd.google-apps.folder'", q):
        if record.get("mimeType") != FOLDER_MIME:
            return False
    if re.search(r"trashed ?= ?false", q) and record.get("trashed"):
        return False
    return True


def _page(items: list[Any], kw: dict[str, Any], default: int, cap: int | None):
    size = kw.get("pageSize", default)
    if cap is not None:
        size = min(size, cap)
    start = int(kw.get("pageToken") or 0)
    page = items[start : start + size]
    token = str(start + size) if start + size < len(items) else None
    return page, token


class _Files:
    def __init__(self, d: FakeDrive):
        self.d = d

    def list(self, **kw):
        self.d.calls.append(("files.list", kw))

        def run():
            q = kw.get("q", "")
            matched = [dict(f) for f in self.d.files_db.values() if _matches(f, q)]
            page, token = _page(matched, kw, 100, self.d.file_page_cap)
            out: dict[str, Any] = {"files": page}
            if token:
                out["nextPageToken"] = token
            return out

        return Req(run, "files.list", kw)

    def create(self, **kw):
        self.d.calls.append(("files.create", kw))

        def run():
            body = kw["body"]
            target = body.get("mimeType")
            if (
                self.d.fail_conversion
                and target
                and target.startswith("application/vnd.google-apps.")
                and target != FOLDER_MIME
            ):
                raise http_error(400, "conversion failed")
            if (
                self.d.reject_inherited_flag_on_create
                and "inheritedPermissionsDisabled" in body
            ):
                raise http_error(400, "inheritedPermissionsDisabled not allowed")
            self.d.counter += 1
            file_id = f"drv{self.d.counter}"
            record = {
                "id": file_id,
                "name": body["name"],
                "parents": body.get("parents", []),
                "mimeType": target or "application/octet-stream",
                "webViewLink": f"https://drive.example/{file_id}",
                "trashed": False,
            }
            for key in ("appProperties", "inheritedPermissionsDisabled"):
                if key in body:
                    record[key] = body[key]
            media = kw.get("media_body")
            if media is not None:
                record["media_mimetype"] = media.mimetype()
                record["resumable"] = media.resumable()
                record["chunksize"] = media.chunksize()
                if not target or not target.startswith("application/vnd.google-apps."):
                    record["mimeType"] = media.mimetype()
                    md5 = hashlib.md5()  # noqa: S324 — fake checksum
                    offset = 0
                    while True:
                        chunk = media.getbytes(offset, 1024 * 1024)
                        if not chunk:
                            break
                        md5.update(chunk)
                        offset += len(chunk)
                    record["md5Checksum"] = md5.hexdigest()
            self.d.files_db[file_id] = record
            return {k: v for k, v in record.items() if k != "media_mimetype"}

        return Req(run, "files.create", kw)

    def get(self, **kw):
        self.d.calls.append(("files.get", kw))

        def run():
            record = self.d.files_db.get(kw["fileId"])
            if not record:
                raise http_error(404, "not found")
            return dict(record)

        return Req(run, "files.get", kw)

    def update(self, **kw):
        self.d.calls.append(("files.update", kw))

        def run():
            record = self.d.files_db.get(kw["fileId"])
            if not record:
                raise http_error(404, "not found")
            record.update(kw.get("body", {}))
            return dict(record)

        return Req(run, "files.update", kw)

    def delete(self, **kw):
        self.d.calls.append(("files.delete", kw))
        return Req(
            lambda: self.d.files_db.pop(kw["fileId"], None) and {}, "files.delete", kw
        )


class _Permissions:
    def __init__(self, d: FakeDrive):
        self.d = d

    def create(self, **kw):
        self.d.calls.append(("permissions.create", kw))

        def run():
            body = kw["body"]
            if body["role"] not in VALID_ROLES:
                raise http_error(400, "The provided role is not valid")
            if kw.get("transferOwnership") and self.d.fail_transfer:
                raise http_error(403, "forbidden")
            perms = self.d.permissions_db.setdefault(kw["fileId"], [])
            perm = {
                "id": f"perm{len(perms) + 1}-{kw['fileId']}",
                "type": body.get("type", "user"),
                "role": body["role"],
                "emailAddress": body.get("emailAddress"),
                "permissionDetails": [{"inherited": False}],
            }
            perms.append(perm)
            return {"id": perm["id"]}

        return Req(run, "permissions.create", kw)

    def list(self, **kw):
        self.d.calls.append(("permissions.list", kw))

        def run():
            perms = list(self.d.permissions_db.get(kw["fileId"], []))
            page, token = _page(perms, kw, 100, None)
            out: dict[str, Any] = {"permissions": page}
            if token:
                out["nextPageToken"] = token
            return out

        return Req(run, "permissions.list", kw)

    def delete(self, **kw):
        self.d.calls.append(("permissions.delete", kw))

        def run():
            perms = self.d.permissions_db.get(kw["fileId"], [])
            self.d.permissions_db[kw["fileId"]] = [
                p for p in perms if p["id"] != kw["permissionId"]
            ]
            return {}

        return Req(run, "permissions.delete", kw)


class _Drives:
    def __init__(self, d: FakeDrive):
        self.d = d

    def list(self, **kw):
        self.d.calls.append(("drives.list", kw))

        def run():
            drives = list(self.d.drives_db)
            m = re.search(r"name = '((?:[^'\\]|\\.)*)'", kw.get("q", ""))
            if m:
                wanted = m.group(1).replace("\\'", "'").replace("\\\\", "\\")
                drives = [d for d in drives if d["name"] == wanted]
            page, token = _page(drives, kw, 10, self.d.drive_page_cap)
            out: dict[str, Any] = {"drives": page}
            if token:
                out["nextPageToken"] = token
            return out

        return Req(run, "drives.list", kw)

    def get(self, **kw):
        self.d.calls.append(("drives.get", kw))

        def run():
            for drive in self.d.drives_db:
                if drive["id"] == kw["driveId"]:
                    return dict(drive)
            raise http_error(404, "Shared drive not found")

        return Req(run, "drives.get", kw)

    def create(self, **kw):
        self.d.calls.append(("drives.create", kw))

        def run():
            drive = {
                "id": f"NEWDRIVE{len(self.d.drives_db)}",
                "name": kw["body"]["name"],
            }
            self.d.drives_db.append(drive)
            return dict(drive)

        return Req(run, "drives.create", kw)
