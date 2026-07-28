import json
from datetime import UTC, date, datetime

import pytest

from pnu_event_gate.cli import _print_json


def test_print_json_serializes_database_dates(capsys) -> None:
    _print_json(
        {
            "created_at": datetime(2026, 7, 28, 4, 28, 44, tzinfo=UTC),
            "run_date": date(2026, 7, 28),
        },
        pretty=False,
    )

    assert json.loads(capsys.readouterr().out) == {
        "created_at": "2026-07-28T04:28:44+00:00",
        "run_date": "2026-07-28",
    }


def test_print_json_rejects_unknown_types() -> None:
    with pytest.raises(TypeError, match="set is not JSON serializable"):
        _print_json({"unsupported": set()}, pretty=False)
