"""Unit tests for MIME helpers, including Slack canvas/list detection."""

from __future__ import annotations

import pytest

from slack_chat_migrator.services.files.file import _document_title
from slack_chat_migrator.utils.mime import is_slack_canvas, is_slack_list


@pytest.mark.parametrize(
    "file_obj",
    [
        {"filetype": "quip"},
        {"filetype": "canvas"},
        {"mimetype": "application/vnd.slack-docs"},
    ],
)
def test_canvas_detection(file_obj):
    assert is_slack_canvas(file_obj)
    assert not is_slack_list(file_obj)


@pytest.mark.parametrize(
    "file_obj",
    [
        {"filetype": "list"},
        {"filetype": "slack_list"},
        {"mimetype": "application/vnd.slack-list"},
        {"list_csv_download_url": "https://files.slack.com/x.csv"},
    ],
)
def test_list_detection(file_obj):
    assert is_slack_list(file_obj)
    assert not is_slack_canvas(file_obj)


def test_ordinary_files_are_neither():
    for file_obj in (
        {"filetype": "csv", "mimetype": "text/csv"},
        {"filetype": "html"},
        {},
    ):
        assert not is_slack_canvas(file_obj)
        assert not is_slack_list(file_obj)


class TestDocumentTitle:
    def test_prefers_title_and_collapses_whitespace(self):
        assert (
            _document_title({"title": "  Q3\n  plan\t", "name": "Q3_plan"}, ".html")
            == "Q3 plan"
        )

    def test_strips_duplicate_extension(self):
        assert _document_title({"title": "Export.CSV"}, ".csv") == "Export"

    def test_drops_control_characters(self):
        assert _document_title({"title": "a\x00b\x07c"}, ".html") == "abc"

    def test_falls_back_to_name_then_id(self):
        assert _document_title({"name": "Roadmap"}, ".csv") == "Roadmap"
        assert _document_title({"id": "F9"}, ".csv") == "file_F9"

    def test_long_titles_are_truncated(self):
        assert len(_document_title({"title": "x" * 500}, ".html")) == 200
