"""In-memory fake of the raw Google Drive v3 service (method-chain style).

It understands just enough of the API for the attachment pipeline: the
``q`` clauses the code sends, pagination, roles, ownership transfer,
permission listing, shared drives and batch requests.  Every call is logged
in ``calls`` as ``(method, kwargs)`` so tests can count round trips.

Like Drive, it answers 404 for a shared drive's file (or for creating a file
in a shared drive's folder) unless ``supportsAllDrives=True`` is passed,
lists shared drive items only with ``includeItemsFromAllDrives=True``, and
rejects ``corpora="drive"`` without ``driveId`` and both of those flags.

Files outside shared drives are owned by ``admin`` (the impersonated user)
until a transfer.  As in Drive, only the owner can transfer a file, and the
previous owner becomes a writer.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from typing import Any

from googleapiclient.errors import HttpError
from httplib2 import Response

FOLDER_MIME = "application/vnd.google-apps.folder"
#: A list request repeated this often with the same page token is a
#: pagination loop: fail the test instead of hanging it.
MAX_SAME_PAGE_REQUESTS = 100
VALID_ROLES = {"owner", "organizer", "fileOrganizer", "writer", "commenter", "reader"}


def http_error(status: int, message: str = "error") -> HttpError:
    return HttpError(Response({"status": str(status)}), message.encode())


class Req:
    """A request whose ``execute()`` runs the stored function."""

    def __init__(self, fn, method: str, kwargs: dict[str, Any], drive=None):
        self._fn = fn
        self.method = method
        self.kwargs = kwargs
        self._drive = drive

    def execute(self):
        error = self._drive.failing.get(self.method) if self._drive else None
        if error is not None:
            raise error
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
        honor_drive_query: bool = True,
        reject_grants_to: set[str] | None = None,
        fail_restriction: bool = False,
        admin: str = "admin@example.com",
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
        self.honor_drive_query = honor_drive_query
        # Emails whose grants fail, as Drive does for a blocked domain.
        self.reject_grants_to = set(reject_grants_to or ())
        # Restricting a folder (files.update) fails with 403 while set.
        self.fail_restriction = fail_restriction
        # Method name ("files.list", ...) -> error every call raises while set.
        self.failing: dict[str, BaseException] = {}
        # files.list query -> error to raise, or None to answer it.
        self.fail_query: Callable[[str], BaseException | None] | None = None
        # The impersonated user: the owner of new files outside shared drives.
        self.admin = admin
        # (method, request) -> how often it was listed; see check_page_loop.
        self._page_requests: dict[tuple[str, str], int] = {}
        # Uploads (not folders) whose files.create is carried out but whose
        # response is lost, as with a dropped connection.
        self.lose_create_responses = 0

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

    def drive_of(self, file_id: str) -> str | None:
        """The shared drive that holds *file_id* (or is it), if any."""
        drive_ids = {d["id"] for d in self.drives_db}
        seen: set[str] = set()
        pending = [file_id]
        while pending:
            current = pending.pop()
            if current in drive_ids:
                return current
            if current in seen:
                continue
            seen.add(current)
            pending.extend((self.files_db.get(current) or {}).get("parents", []))
        return None

    def owners_of(self, file_id: str) -> list[str]:
        """The file's owners: none in a shared drive, else the admin by default."""
        if self.drive_of(file_id):
            return []
        record = self.files_db.get(file_id) or {}
        return list(record.get("owners_emails", [self.admin]))

    def with_owners(self, record: dict[str, Any]) -> dict[str, Any]:
        """A copy of *record* as Drive returns it, with owners and ownedByMe."""
        out = {k: v for k, v in record.items() if k != "owners_emails"}
        if not self.drive_of(record["id"]):
            owners = self.owners_of(record["id"])
            out["owners"] = [{"emailAddress": email} for email in owners]
            out["ownedByMe"] = self.admin in owners
        return out

    def check_access(self, file_id: str, kw: dict[str, Any]) -> None:
        """Answer 404 for a shared drive's file without supportsAllDrives."""
        if self.drive_of(file_id) and kw.get("supportsAllDrives") is not True:
            raise http_error(404, f"File not found: {file_id}.")

    def check_page_loop(self, method: str, kw: dict[str, Any]) -> None:
        """Fail fast when the same page is requested over and over."""
        key = (method, repr(sorted(kw.items(), key=lambda item: item[0])))
        seen = self._page_requests.get(key, 0) + 1
        self._page_requests[key] = seen
        if seen > MAX_SAME_PAGE_REQUESTS:
            raise AssertionError(
                f"{method} was asked for the same page {seen} times: a"
                " pagination loop that doesn't pass nextPageToken"
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


#: Drive's file query terms (developers.google.com/workspace/drive/api/
#: guides/ref-search-terms), plus ``key``/``value`` inside ``has { }``.
FILE_QUERY_TERMS = frozenset(
    {
        "name",
        "fullText",
        "mimeType",
        "modifiedTime",
        "viewedByMeTime",
        "trashed",
        "starred",
        "parents",
        "owners",
        "writers",
        "readers",
        "sharedWithMe",
        "createdTime",
        "properties",
        "appProperties",
        "visibility",
        "shortcutDetails.targetId",
        "key",
        "value",
    }
)
_QUOTED = re.compile(r"'(?:[^'\\]|\\.)*'")
_TERM = re.compile(r"([A-Za-z_][\w.]*)\s*(?:!=|<=|>=|=|<|>|\bcontains\b|\bhas\b)")


def _check_file_query(q: str) -> None:
    """Reject a files.list query term Drive doesn't know, as Drive does."""
    for term in _TERM.findall(_QUOTED.sub("''", q)):
        if term not in FILE_QUERY_TERMS:
            raise http_error(400, f"Invalid Value: unknown query term {term!r}")


def _matches(record: dict[str, Any], q: str) -> bool:
    m = re.search(r"'((?:[^'\\]|\\.)*)' in parents", q)
    if m and m.group(1).replace("\\'", "'") not in record.get("parents", []):
        return False
    m = re.search(r"name ?= ?'((?:[^'\\]|\\.)*)'", q)
    if m and record.get("name") != m.group(1).replace("\\'", "'").replace("\\\\", "\\"):
        return False
    if re.search(r"mimeType ?= ?'application/vnd.google-apps.folder'", q):
        if record.get("mimeType") != FOLDER_MIME:
            return False
    if re.search(r"trashed ?= ?false", q) and record.get("trashed"):
        return False
    m = re.search(
        r"appProperties has \{ key='([^']+)' and value='((?:[^'\\]|\\.)*)' \}", q
    )
    if m:
        props = record.get("appProperties") or {}
        if props.get(m.group(1)) != m.group(2).replace("\\'", "'"):
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
        self.d.check_page_loop("files.list", kw)

        def run():
            q = kw.get("q", "")
            _check_file_query(q)
            all_drives = kw.get("includeItemsFromAllDrives") is True
            corpus_drive = None
            if kw.get("corpora") == "drive":
                if not (
                    kw.get("driveId")
                    and all_drives
                    and kw.get("supportsAllDrives") is True
                ):
                    raise http_error(
                        400,
                        "corpora=drive needs driveId, includeItemsFromAllDrives"
                        " and supportsAllDrives",
                    )
                corpus_drive = kw["driveId"]
            error = self.d.fail_query(q) if self.d.fail_query else None
            if error is not None:
                raise error
            matched = []
            for f in self.d.files_db.values():
                drive = self.d.drive_of(f["id"])
                if corpus_drive is not None and drive != corpus_drive:
                    continue
                if drive and not all_drives:
                    continue
                if _matches(f, q):
                    matched.append(self.d.with_owners(f))
            page, token = _page(matched, kw, 100, self.d.file_page_cap)
            out: dict[str, Any] = {"files": page}
            if token:
                out["nextPageToken"] = token
            return out

        return Req(run, "files.list", kw, self.d)

    def create(self, **kw):
        self.d.calls.append(("files.create", kw))

        def run():
            body = kw["body"]
            for parent in body.get("parents", []):
                self.d.check_access(parent, kw)
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
            if self.d.lose_create_responses and record["mimeType"] != FOLDER_MIME:
                self.d.lose_create_responses -= 1
                raise ConnectionResetError("connection reset; the file was created")
            return {k: v for k, v in record.items() if k != "media_mimetype"}

        return Req(run, "files.create", kw, self.d)

    def get(self, **kw):
        self.d.calls.append(("files.get", kw))

        def run():
            record = self.d.files_db.get(kw["fileId"])
            if not record:
                raise http_error(404, "not found")
            self.d.check_access(kw["fileId"], kw)
            return self.d.with_owners(record)

        return Req(run, "files.get", kw, self.d)

    def update(self, **kw):
        self.d.calls.append(("files.update", kw))

        def run():
            record = self.d.files_db.get(kw["fileId"])
            if not record:
                raise http_error(404, "not found")
            self.d.check_access(kw["fileId"], kw)
            body = kw.get("body", {})
            if self.d.fail_restriction and "inheritedPermissionsDisabled" in body:
                raise http_error(403, "insufficient role to limit access")
            body = dict(body)
            if "appProperties" in body:
                # Drive merges appProperties; a null value deletes the key.
                props = {**(record.get("appProperties") or {})}
                for key, value in body.pop("appProperties").items():
                    if value is None:
                        props.pop(key, None)
                    else:
                        props[key] = value
                record["appProperties"] = props
            record.update(body)
            return self.d.with_owners(record)

        return Req(run, "files.update", kw, self.d)

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
            self.d.check_access(kw["fileId"], kw)
            body = kw["body"]
            if body["role"] not in VALID_ROLES:
                raise http_error(400, "The provided role is not valid")
            if kw.get("transferOwnership"):
                # Drive: sendNotificationEmail "must not be disabled for
                # ownership transfers".
                if kw.get("sendNotificationEmail") is False:
                    raise http_error(
                        400, "Notification required for ownership transfer"
                    )
                if self.d.fail_transfer:
                    raise http_error(403, "forbidden")
                if self.d.admin not in self.d.owners_of(kw["fileId"]):
                    # Drive: only the owner can transfer ownership.
                    raise http_error(403, "insufficientFilePermissions")
            if body.get("emailAddress") in self.d.reject_grants_to:
                raise http_error(400, "Sharing outside the domain is not allowed")
            perms = self.d.permissions_db.setdefault(kw["fileId"], [])
            if kw.get("transferOwnership"):
                # The previous owner is downgraded to writer.
                record = self.d.files_db.setdefault(kw["fileId"], {"id": kw["fileId"]})
                record["owners_emails"] = [body.get("emailAddress")]
                perms.append(
                    {
                        "id": f"perm{len(perms) + 1}-{kw['fileId']}",
                        "type": "user",
                        "role": "writer",
                        "emailAddress": self.d.admin,
                        "permissionDetails": [{"inherited": False}],
                    }
                )
            perm = {
                "id": f"perm{len(perms) + 1}-{kw['fileId']}",
                "type": body.get("type", "user"),
                "role": body["role"],
                "emailAddress": body.get("emailAddress"),
                "permissionDetails": [{"inherited": False}],
            }
            perms.append(perm)
            return {"id": perm["id"]}

        return Req(run, "permissions.create", kw, self.d)

    def list(self, **kw):
        self.d.calls.append(("permissions.list", kw))
        self.d.check_page_loop("permissions.list", kw)

        def run():
            self.d.check_access(kw["fileId"], kw)
            perms = list(self.d.permissions_db.get(kw["fileId"], []))
            page, token = _page(perms, kw, 100, None)
            out: dict[str, Any] = {"permissions": page}
            if token:
                out["nextPageToken"] = token
            return out

        return Req(run, "permissions.list", kw, self.d)

    def delete(self, **kw):
        self.d.calls.append(("permissions.delete", kw))

        def run():
            self.d.check_access(kw["fileId"], kw)
            perms = self.d.permissions_db.get(kw["fileId"], [])
            self.d.permissions_db[kw["fileId"]] = [
                p for p in perms if p["id"] != kw["permissionId"]
            ]
            return {}

        return Req(run, "permissions.delete", kw, self.d)


#: What a Manager may do; a drive record's own ``capabilities`` override it.
MANAGER_CAPABILITIES = {"canAddChildren": True, "canManageMembers": True}


def _with_capabilities(drive: dict[str, Any]) -> dict[str, Any]:
    """A copy of *drive* as Drive returns it, with the admin's capabilities."""
    return {
        **drive,
        "capabilities": {**MANAGER_CAPABILITIES, **drive.get("capabilities", {})},
    }


class _Drives:
    def __init__(self, d: FakeDrive):
        self.d = d

    def list(self, **kw):
        self.d.calls.append(("drives.list", kw))
        self.d.check_page_loop("drives.list", kw)

        def run():
            drives = [_with_capabilities(d) for d in self.d.drives_db]
            m = re.search(r"name = '((?:[^'\\]|\\.)*)'", kw.get("q", ""))
            if m and self.d.honor_drive_query:
                wanted = m.group(1).replace("\\'", "'").replace("\\\\", "\\")
                drives = [d for d in drives if d["name"] == wanted]
            page, token = _page(drives, kw, 10, self.d.drive_page_cap)
            out: dict[str, Any] = {"drives": page}
            if token:
                out["nextPageToken"] = token
            return out

        return Req(run, "drives.list", kw, self.d)

    def get(self, **kw):
        self.d.calls.append(("drives.get", kw))

        def run():
            for drive in self.d.drives_db:
                if drive["id"] == kw["driveId"]:
                    return _with_capabilities(drive)
            raise http_error(404, "Shared drive not found")

        return Req(run, "drives.get", kw, self.d)

    def create(self, **kw):
        self.d.calls.append(("drives.create", kw))

        def run():
            drive = {
                "id": f"NEWDRIVE{len(self.d.drives_db)}",
                "name": kw["body"]["name"],
            }
            self.d.drives_db.append(drive)
            return dict(drive)

        return Req(run, "drives.create", kw, self.d)
