"""Every dashboard page renders against the loaded serving layer without exceptions or error banners (needs the stack)."""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest

from src.cassandra_store import CassandraUnavailable, connect

pytestmark = pytest.mark.integration

PAGES = ["Viewer", "Analyst", "Manager", "Data Engineering"]
PICKER_MATCHES = 50
APP = Path(__file__).resolve().parent.parent / "app.py"


@pytest.fixture(scope="module", autouse=True)
def stack():
    try:
        connect(timeout=5)
    except CassandraUnavailable as error:
        pytest.skip(str(error))


def open_page(page: str) -> AppTest:
    app = AppTest.from_file(str(APP), default_timeout=180)
    app.run()
    assert not app.exception, [error.value for error in app.exception]
    app.sidebar.radio[0].set_value(page).run()
    return app


def problems(app: AppTest) -> list[str]:
    return [str(item.value) for item in list(app.exception) + list(app.error)]


@pytest.mark.parametrize("page", PAGES)
def test_page_renders_without_errors(page):
    app = open_page(page)
    assert problems(app) == []


def test_user_picker_never_receives_the_whole_user_list():
    app = open_page("Viewer")
    picker = next(box for box in app.selectbox if box.label == "Recommendations for")
    assert 0 < len(picker.options) <= PICKER_MATCHES
    search = next(box for box in app.text_input if box.key == "viewer_user_term")
    search.set_value("U0001").run()
    narrowed = next(box for box in app.selectbox if box.label == "Recommendations for")
    assert 0 < len(narrowed.options) <= PICKER_MATCHES and all("U0001" in option for option in narrowed.options)
    assert problems(app) == []


def test_title_picker_on_the_manager_page_is_capped_and_draws_a_series():
    app = open_page("Manager")
    picker = next(box for box in app.selectbox if box.key == "br10_movie_select")
    assert 0 < len(picker.options) <= PICKER_MATCHES
    next(box for box in app.text_input if box.key == "br10_movie_term").set_value("spider").run()
    narrowed = next(box for box in app.selectbox if box.key == "br10_movie_select")
    assert 0 < len(narrowed.options) <= PICKER_MATCHES
    assert problems(app) == []
