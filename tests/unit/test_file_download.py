"""Unit tests for streaming, validated Slack downloads (file_download)."""

from __future__ import annotations

import hashlib
import http.server
import io
import os
import threading
import tracemalloc
from typing import ClassVar

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from slack_chat_migrator.constants import FILE_READ_CHUNK_BYTES
from slack_chat_migrator.services.files import file_download as fd
from slack_chat_migrator.services.files.file_download import (
    DownloadStatus,
    build_download_session,
    create_drive_reference,
    download_file,
    expected_size,
    get_download_session,
    google_link_status,
    is_allowed_file_url,
    redact_url,
    select_download_url,
    unavailable_reason,
)

SLACK_URL = "https://files.slack.com/files-pri/T1-F1/photo.png"
SLACK_DL_URL = "https://files.slack.com/files-pri/T1-F1/download/photo.png"
HTML_PAGE = b"<!DOCTYPE html><html><title>Sign in to Slack</title></html>"

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


def make_response(
    status=200, body=b"", content_type="application/octet-stream", headers=None
):
    """Build a real requests.Response backed by an in-memory body."""
    response = requests.Response()
    response.status_code = status
    response.raw = io.BytesIO(body)
    response.headers = CaseInsensitiveDict(headers or {})
    if content_type:
        response.headers["Content-Type"] = content_type
    return response


class FakeSession:
    """Records GETs and answers from a URL -> response (or exception) map."""

    def __init__(self, responses=None):
        self.responses = dict(responses or {})
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        answer = self.responses.get(url)
        if answer is None:
            raise AssertionError(f"unexpected GET {url}")
        if isinstance(answer, BaseException):
            raise answer
        return answer() if callable(answer) else answer


def png_file(**overrides):
    file_obj = {
        "id": "F1",
        "name": "photo.png",
        "mimetype": "image/png",
        "url_private": SLACK_URL,
    }
    file_obj.update(overrides)
    return file_obj


@pytest.fixture
def dest(tmp_path):
    directory = tmp_path / "dl"
    directory.mkdir()
    return directory


def leftover_files(directory):
    return sorted(p.name for p in directory.iterdir())


# ---------------------------------------------------------------------------
# Successful downloads
# ---------------------------------------------------------------------------


class TestSuccessfulDownload:
    def test_streams_to_temp_file_with_md5_and_size(self, dest):
        body = b"\x89PNG" + os.urandom(5000)
        session = FakeSession(
            {SLACK_URL: make_response(body=body, content_type="image/png")}
        )

        result = download_file(
            png_file(size=len(body)), str(dest), "general", session=session
        )

        assert result.status is DownloadStatus.OK
        assert result.ok
        assert result.size == len(body)
        assert result.md5 == hashlib.md5(body).hexdigest()  # noqa: S324
        assert result.path is not None
        assert os.path.dirname(result.path) == str(dest)
        with open(result.path, "rb") as fh:
            assert fh.read() == body

    def test_request_is_streamed_without_redirects(self, dest):
        session = FakeSession({SLACK_URL: make_response(body=b"x")})
        download_file(png_file(), str(dest), session=session)

        _url, kwargs = session.calls[0]
        assert kwargs["stream"] is True
        assert kwargs["allow_redirects"] is False
        assert kwargs["timeout"]

    def test_body_is_read_in_bounded_chunks(self, dest):
        body = b"a" * (3 * FILE_READ_CHUNK_BYTES + 7)
        response = make_response(body=body)
        sizes = []
        original = response.iter_content

        def spy(chunk_size=1, decode_unicode=False):
            sizes.append(chunk_size)
            return original(chunk_size=chunk_size, decode_unicode=decode_unicode)

        response.iter_content = spy
        result = download_file(
            png_file(size=len(body)),
            str(dest),
            session=FakeSession({SLACK_URL: response}),
        )
        assert result.ok
        assert sizes == [FILE_READ_CHUNK_BYTES]

    def test_prefers_url_private_download(self, dest):
        session = FakeSession({SLACK_DL_URL: make_response(body=b"x")})
        result = download_file(
            png_file(url_private_download=SLACK_DL_URL), str(dest), session=session
        )
        assert result.ok
        assert [url for url, _ in session.calls] == [SLACK_DL_URL]

    def test_falls_back_to_url_private_download_only(self, dest):
        file_obj = png_file(url_private_download=SLACK_DL_URL)
        del file_obj["url_private"]
        session = FakeSession({SLACK_DL_URL: make_response(body=b"x")})

        result = download_file(file_obj, str(dest), session=session)

        assert result.ok
        assert len(session.calls) == 1

    def test_explicit_url_overrides_choice(self, dest):
        csv_url = "https://files.slack.com/files-pri/T1-F1/list.csv"
        session = FakeSession({csv_url: make_response(body=b"a,b\n")})
        result = download_file(
            png_file(), str(dest), url=csv_url, check_size=False, session=session
        )
        assert result.ok
        assert session.calls[0][0] == csv_url

    def test_html_accepted_when_slack_says_html(self, dest):
        session = FakeSession(
            {
                SLACK_URL: make_response(
                    body=HTML_PAGE, content_type="text/html; charset=utf-8"
                )
            }
        )
        result = download_file(
            png_file(name="page.html", mimetype="text/html"), str(dest), session=session
        )
        assert result.ok


# ---------------------------------------------------------------------------
# Validation (I10)
# ---------------------------------------------------------------------------


class TestValidation:
    def test_html_sign_in_page_for_png_is_unavailable(self, dest):
        session = FakeSession(
            {
                SLACK_URL: make_response(
                    body=HTML_PAGE, content_type="text/html; charset=utf-8"
                )
            }
        )
        result = download_file(png_file(size=12345), str(dest), session=session)

        assert result.status is DownloadStatus.UNAVAILABLE
        assert result.reason == fd.REASON_HTML_PAGE
        assert result.path is None
        assert leftover_files(dest) == []

    def test_shorter_body_than_size_is_rejected(self, dest):
        session = FakeSession({SLACK_URL: make_response(body=b"short")})
        result = download_file(png_file(size=12345), str(dest), session=session)

        assert result.status is DownloadStatus.UNAVAILABLE
        assert result.reason == fd.REASON_SIZE_MISMATCH
        assert leftover_files(dest) == []

    def test_longer_body_than_size_stops_early(self, dest):
        body = b"x" * (2 * FILE_READ_CHUNK_BYTES)
        session = FakeSession({SLACK_URL: make_response(body=body)})
        result = download_file(png_file(size=10), str(dest), session=session)

        assert result.reason == fd.REASON_SIZE_MISMATCH
        assert leftover_files(dest) == []

    def test_size_check_can_be_disabled(self, dest):
        session = FakeSession({SLACK_URL: make_response(body=b"different")})
        result = download_file(
            png_file(size=1), str(dest), check_size=False, session=session
        )
        assert result.ok

    def test_zero_or_missing_size_is_not_checked(self, dest):
        for size in (0, None, "n/a", True):
            session = FakeSession({SLACK_URL: make_response(body=b"abc")})
            result = download_file(png_file(size=size), str(dest), session=session)
            assert result.ok, size

    @pytest.mark.parametrize("status", [401, 403, 404, 410])
    def test_client_errors_are_unavailable(self, dest, status):
        session = FakeSession({SLACK_URL: make_response(status=status)})
        result = download_file(png_file(), str(dest), session=session)

        assert result.status is DownloadStatus.UNAVAILABLE
        assert result.reason == f"http_{status}"
        assert result.http_status == status
        assert len(session.calls) == 1

    @pytest.mark.parametrize("status", [429, 500, 503])
    def test_server_errors_after_retries_are_failed(self, dest, status):
        session = FakeSession({SLACK_URL: make_response(status=status)})
        result = download_file(png_file(), str(dest), session=session)

        assert result.status is DownloadStatus.FAILED
        assert result.reason == f"http_{status}"

    def test_connection_error_is_failed(self, dest):
        session = FakeSession({SLACK_URL: requests.ConnectionError("reset")})
        result = download_file(png_file(), str(dest), session=session)

        assert result.status is DownloadStatus.FAILED
        assert result.reason == "network_error:ConnectionError"

    def test_http_error_with_falsy_response_keeps_status(self, dest):
        """A 4xx Response is falsy; the status must still be read (I26)."""
        error_response = make_response(status=403)
        assert not error_response
        session = FakeSession(
            {SLACK_URL: requests.HTTPError("403", response=error_response)}
        )
        result = download_file(png_file(), str(dest), session=session)

        assert result.status is DownloadStatus.FAILED
        assert result.http_status == 403

    def test_broken_stream_is_failed_and_cleaned_up(self, dest):
        response = make_response(body=b"")

        def broken(chunk_size=1, decode_unicode=False):
            yield b"partial"
            raise requests.exceptions.ChunkedEncodingError("cut off")

        response.iter_content = broken
        result = download_file(
            png_file(), str(dest), session=FakeSession({SLACK_URL: response})
        )

        assert result.status is DownloadStatus.FAILED
        assert result.reason == "network_error:ChunkedEncodingError"
        assert leftover_files(dest) == []

    def test_write_error_is_failed(self, tmp_path):
        session = FakeSession({SLACK_URL: make_response(body=b"x")})
        result = download_file(
            png_file(), str(tmp_path / "missing-dir"), session=session
        )
        assert result.status is DownloadStatus.FAILED
        assert result.reason == "write_error"


# ---------------------------------------------------------------------------
# Files that are never downloaded
# ---------------------------------------------------------------------------


class TestNotDownloaded:
    def test_external_file_keeps_link_and_makes_no_request(self, dest):
        file_obj = {
            "id": "F2",
            "name": "deck.pdf",
            "mode": "external",
            "is_external": True,
            "external_type": "dropbox",
            "url_private": "https://www.dropbox.com/s/abc/deck.pdf?dl=0",
        }
        session = FakeSession()
        result = download_file(file_obj, str(dest), session=session)

        assert result.status is DownloadStatus.UNAVAILABLE
        assert result.reason == fd.REASON_EXTERNAL
        assert result.url == "https://www.dropbox.com/s/abc/deck.pdf?dl=0"
        assert session.calls == []

    @pytest.mark.parametrize(
        ("mode", "reason"),
        [
            ("tombstone", fd.REASON_DELETED),
            ("hidden_by_limit", fd.REASON_HIDDEN_BY_LIMIT),
        ],
    )
    def test_deleted_and_hidden_files(self, dest, mode, reason):
        session = FakeSession()
        result = download_file({"id": "F3", "mode": mode}, str(dest), session=session)
        assert result.status is DownloadStatus.UNAVAILABLE
        assert result.reason == reason
        assert session.calls == []

    def test_missing_or_null_url(self, dest):
        for file_obj in (
            {"id": "F4", "name": "x.pdf"},
            {"id": "F5", "url_private": None},
        ):
            result = download_file(file_obj, str(dest), session=FakeSession())
            assert result.status is DownloadStatus.UNAVAILABLE
            assert result.reason == fd.REASON_NO_URL

    @pytest.mark.parametrize(
        "url",
        [
            "https://docs.google.com/document/d/abc/edit",
            "https://docs.google.com/spreadsheets/d/abc/edit",
            "https://docs.google.com/presentation/d/abc/edit",
            "https://sheets.google.com/abc/edit",
            "https://slides.google.com/abc/edit",
        ],
    )
    def test_google_docs_links(self, dest, url):
        result = download_file(
            {"id": "F1", "url_private": url, "mode": "external"},
            str(dest),
            session=FakeSession(),
        )
        assert result.status is DownloadStatus.GOOGLE_DOCS_LINK
        assert result.url == url

    @pytest.mark.parametrize(
        "url",
        [
            "https://drive.google.com/file/d/abc123/view",
            "https://drive.google.com/open?id=abc123",
        ],
    )
    def test_google_drive_files(self, dest, url):
        result = download_file(
            {"id": "F1", "url_private": url}, str(dest), session=FakeSession()
        )
        assert result.status is DownloadStatus.GOOGLE_DRIVE_FILE

    def test_google_strings_in_slack_query_are_not_links(self):
        url = SLACK_URL + "?next=https://docs.google.com/document/d/x"
        assert google_link_status({"url_private": url}) is None


# ---------------------------------------------------------------------------
# SSRF guard and redirects (I10)
# ---------------------------------------------------------------------------


BLOCKED_URLS = [
    "https://127.0.0.1/x",
    "https://10.0.0.5/x",
    "https://[::1]/x",
    "https://169.254.169.254/latest/meta-data/",
    "https://127.1/x",
    "https://0x7f000001/x",
    "https://2130706433/x",
    "https://localhost/x",
    "https://metadata.google.internal/computeMetadata/v1/",
    "http://files.slack.com/x",
    "ftp://files.slack.com/x",
    "https://evil.example.com/x",
    "https://files.slack.com.evil.example.com/x",
    "https://notslack.com/x",
]


class TestHostAllowList:
    @pytest.mark.parametrize("url", BLOCKED_URLS)
    def test_blocked_urls_are_never_requested(self, dest, url):
        session = FakeSession()
        result = download_file(
            {"id": "F", "name": "n", "url_private": url}, str(dest), session=session
        )
        assert result.status is DownloadStatus.UNAVAILABLE
        assert result.reason == fd.REASON_DISALLOWED_URL
        assert session.calls == []

    @pytest.mark.parametrize(
        "url",
        [
            "https://files.slack.com/files-pri/T-F/a.png",
            "https://acme.slack.com/files/U1/F1/a.png",
            "https://FILES.SLACK.COM/x",
            "https://slack-files.com/files-pri/T-F/a",
            "https://a.slack-edge.com/x.png",
        ],
    )
    def test_slack_hosts_allowed(self, url):
        assert is_allowed_file_url(url)

    def test_redirect_within_slack_is_followed_with_token(self, dest):
        hop = "https://files-origin.slack.com/files-pri/T1-F1/photo.png"
        session = FakeSession(
            {
                SLACK_URL: make_response(status=302, headers={"Location": hop}),
                hop: make_response(body=b"img"),
            }
        )
        token = "xoxe-test-value"  # noqa: S105 — test value
        result = download_file(
            png_file(), str(dest), slack_token=token, session=session
        )

        assert result.ok
        assert [url for url, _ in session.calls] == [SLACK_URL, hop]
        for _url, kwargs in session.calls:
            assert kwargs["headers"] == {"Authorization": f"Bearer {token}"}

    def test_relative_redirect_is_resolved(self, dest):
        target = "https://files.slack.com/other/photo.png"
        session = FakeSession(
            {
                SLACK_URL: make_response(
                    status=301, headers={"Location": "/other/photo.png"}
                ),
                target: make_response(body=b"img"),
            }
        )
        assert download_file(png_file(), str(dest), session=session).ok

    @pytest.mark.parametrize(
        "location",
        [
            "https://169.254.169.254/latest",
            "http://files.slack.com/x",
            "https://evil.example.com/",
        ],
    )
    def test_redirect_off_the_allow_list_is_refused(self, dest, location):
        session = FakeSession(
            {SLACK_URL: make_response(status=302, headers={"Location": location})}
        )
        result = download_file(
            png_file(), str(dest), slack_token="tok", session=session
        )

        assert result.status is DownloadStatus.UNAVAILABLE
        assert result.reason == fd.REASON_DISALLOWED_URL
        assert [url for url, _ in session.calls] == [SLACK_URL]

    def test_redirect_loop_is_cut_off(self, dest):
        session = FakeSession(
            {
                SLACK_URL: lambda: make_response(
                    status=302, headers={"Location": SLACK_URL}
                )
            }
        )
        result = download_file(png_file(), str(dest), session=session)

        assert result.reason == fd.REASON_TOO_MANY_REDIRECTS
        assert len(session.calls) == fd.DOWNLOAD_MAX_REDIRECTS + 1

    def test_no_authorization_header_without_token(self, dest):
        session = FakeSession({SLACK_URL: make_response(body=b"x")})
        download_file(png_file(), str(dest), session=session)
        assert session.calls[0][1]["headers"] == {}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_redact_url_drops_query(self):
        assert (
            redact_url("https://files.slack.com/a/b.png?t=xoxe-123")
            == "https://files.slack.com/a/b.png"
        )
        assert redact_url(None) == ""

    def test_select_download_url(self):
        assert select_download_url({"url_private": "a"}) == "a"
        assert (
            select_download_url({"url_private_download": "b", "url_private": "a"})
            == "b"
        )
        assert select_download_url({"url_private": None}) is None
        assert select_download_url({"url_private": 5}) is None

    def test_unavailable_reason(self):
        assert unavailable_reason({"mode": "tombstone"}) == fd.REASON_DELETED
        assert (
            unavailable_reason({"mode": "hidden_by_limit"}) == fd.REASON_HIDDEN_BY_LIMIT
        )
        assert (
            unavailable_reason({"is_external": True, "url_private": "x"})
            == fd.REASON_EXTERNAL
        )
        assert unavailable_reason({"mode": "hosted"}) == fd.REASON_NO_URL
        assert unavailable_reason({"mode": "hosted", "url_private": SLACK_URL}) is None

    def test_expected_size(self):
        assert expected_size({"size": 10}) == 10
        assert expected_size({"size": "10"}) == 10
        assert expected_size({"size": 0}) is None
        assert expected_size({"size": False}) is None
        assert expected_size({}) is None

    def test_canvas_counts_as_html(self):
        assert fd.expects_html({"filetype": "quip"})
        assert fd.expects_html({"mimetype": "application/vnd.slack-docs"})
        assert fd.expects_html({"name": "Index.HTM"})
        assert not fd.expects_html({"name": "photo.png", "mimetype": "image/png"})


class TestCreateDriveReferenceParsing:
    def test_file_d_url(self):
        result = create_drive_reference(
            {
                "id": "F1",
                "name": "r.pdf",
                "url_private": "https://drive.google.com/file/d/ABC/view",
            },
            "general",
        )
        assert result is not None
        assert result.drive_id == "ABC"

    def test_open_id_url(self):
        result = create_drive_reference(
            {"id": "F1", "url_private": "https://drive.google.com/open?id=XYZ&x=1"},
            None,
        )
        assert result is not None
        assert result.drive_id == "XYZ"


# ---------------------------------------------------------------------------
# Shared session and retries (I26, P3)
# ---------------------------------------------------------------------------


class TestSession:
    def test_shared_session_is_reused(self):
        assert get_download_session() is get_download_session()

    def test_retry_policy(self):
        session = build_download_session(pool_size=8)
        adapter = session.get_adapter("https://files.slack.com/x")
        retry = adapter.max_retries
        assert retry.total == fd.DOWNLOAD_RETRY_TOTAL
        assert set(retry.status_forcelist) == {429, 500, 502, 503, 504}
        assert 401 not in retry.status_forcelist
        assert 403 not in retry.status_forcelist
        assert set(retry.allowed_methods) == {"GET"}
        assert retry.respect_retry_after_header is True
        assert adapter._pool_maxsize == 8


class _ScriptedHandler(http.server.BaseHTTPRequestHandler):
    """Serves a scripted sequence of statuses; records request count."""

    script: ClassVar[list[int]] = []
    hits: ClassVar[list[str]] = []

    def do_GET(self):
        type(self).hits.append(self.path)
        status = type(self).script.pop(0) if type(self).script else 200
        body = b"payload" if status == 200 else b"error"
        self.send_response(status)
        if status in (429, 503):
            self.send_header("Retry-After", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def scripted_server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _ScriptedHandler)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()
    _ScriptedHandler.hits = []
    yield server, _ScriptedHandler
    server.shutdown()
    server.server_close()


class TestRetriesAgainstLocalServer:
    """Exercise the real urllib3 retry policy (plain HTTP, local only)."""

    def _get(self, server):
        session = build_download_session(backoff_factor=0)
        host, port = server.server_address[:2]
        return session.get(f"http://{host}:{port}/f", timeout=5)

    def test_5xx_and_429_are_retried(self, scripted_server):
        server, handler = scripted_server
        handler.script = [503, 429, 500, 200]
        response = self._get(server)
        assert response.status_code == 200
        assert response.content == b"payload"
        assert len(handler.hits) == 4

    @pytest.mark.parametrize("status", [401, 403, 404])
    def test_auth_and_not_found_are_not_retried(self, scripted_server, status):
        server, handler = scripted_server
        handler.script = [status, 200]
        response = self._get(server)
        assert response.status_code == status
        assert len(handler.hits) == 1


# ---------------------------------------------------------------------------
# Memory (I28, P3)
# ---------------------------------------------------------------------------


class _RepeatingRaw(io.RawIOBase):
    """A large fake body that reuses one buffer, so it allocates nothing."""

    def __init__(self, total):
        self.remaining = total
        self.block = b"\xab" * FILE_READ_CHUNK_BYTES

    def readable(self):
        return True

    def read(self, size=-1):
        if self.remaining <= 0:
            return b""
        n = min(
            size if size and size > 0 else len(self.block),
            len(self.block),
            self.remaining,
        )
        self.remaining -= n
        return self.block if n == len(self.block) else self.block[:n]


def test_peak_memory_is_independent_of_file_size(dest):
    total = 256 * 1024 * 1024
    response = make_response()
    response.raw = _RepeatingRaw(total)
    session = FakeSession({SLACK_URL: response})

    tracemalloc.start()
    try:
        result = download_file(png_file(size=total), str(dest), session=session)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert result.ok
    assert result.size == total
    assert peak < 64 * 1024 * 1024
    os.unlink(result.path)
