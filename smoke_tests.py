import contextlib
from pathlib import Path
import importlib.util

import pandas as pd


APP_PATH = Path(__file__).with_name("app.py")
spec = importlib.util.spec_from_file_location("tactical_cloud_app", APP_PATH)
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class DummyConn:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.executed = []
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        self.executed.append((str(sql), tuple(params)))
        response = self.responses.pop(0) if self.responses else None
        return DummyCursor(response)

    def commit(self):
        self.committed = True


class DummyCursor:
    def __init__(self, response=None):
        self.response = response
        self.lastrowid = None
        if isinstance(response, dict) and "lastrowid" in response:
            self.lastrowid = response["lastrowid"]

    def fetchone(self):
        if isinstance(self.response, list):
            return self.response[0] if self.response else None
        if isinstance(self.response, dict) and "fetchone" in self.response:
            return self.response["fetchone"]
        return self.response

    def fetchall(self):
        if isinstance(self.response, dict) and "fetchall" in self.response:
            return self.response["fetchall"]
        if isinstance(self.response, list):
            return self.response
        return []


@contextlib.contextmanager
def patched(obj, name, value):
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


def test_translate_sql_preserves_types():
    translator = app.PgConnCompat.__new__(app.PgConnCompat)
    translated = translator._translate_sql("SELECT CAST(created_at AS TEXT) AS Data FROM commands WHERE id = ?")
    assert 'CAST(created_at AS TEXT)' in translated
    assert 'AS "TEXT"' not in translated
    assert "%s" in translated


def test_next_command_number_uses_max_plus_one():
    def fake_query_df(sql, params=()):
        return pd.DataFrame([{"next_number": 145}])

    with patched(app, "query_df", fake_query_df):
        assert app.next_command_number() == 145


def test_cash_expected_amount_sums_opening_and_movements():
    conn = DummyConn(
        responses=[
            {"opening_amount": 100.0},
            [
                {"movement_type": "Entrada", "total": 55.0},
                {"movement_type": "Saída", "total": 20.0},
            ],
        ]
    )

    def fake_get_conn():
        return conn

    with patched(app, "get_conn", fake_get_conn):
        assert app.cash_expected_amount(1) == 135.0


def test_create_command_locks_table_and_uses_next_number():
    conn = DummyConn(
        responses=[
            None,
            {"next_number": 101},
            {"lastrowid": 88, "fetchone": {"id": 88}},
        ]
    )

    def fake_get_conn():
        return conn

    with (
        patched(app, "get_conn", fake_get_conn),
        patched(app, "existing_command_for_operator", lambda event_id, operator_id: None),
        patched(app, "sync_event_from_commands", lambda event_id: None),
        patched(app, "log_action", lambda *args, **kwargs: None),
        patched(app, "_bump_cache_version", lambda: None),
    ):
        command_id = app.create_command(
            opened_at="2026-05-15 10:00:00",
            event_id=1,
            operator_id=2,
            customer_name="Jogador 1",
            entry_type="Aluguel",
            entry_value=50.0,
            notes="",
        )

    assert command_id == 88
    assert conn.committed
    assert "LOCK TABLE commands IN EXCLUSIVE MODE" in conn.executed[0][0]
    assert "COALESCE(MAX(number), 99) + 1" in conn.executed[1][0]


def test_close_command_blocks_duplicate_receipt():
    conn = DummyConn(
        responses=[
            {"id": 1, "status": "Aberto"},
            {"id": 9, "number": 120, "status": "Aberta", "event_id": 3, "entry_value": 10.0, "entry_original_value": 10.0},
            {"subtotal": 45.0, "number": 120, "event_id": 3},
            {"id": 77},
        ]
    )

    def fake_get_conn():
        return conn

    with patched(app, "get_conn", fake_get_conn):
        try:
            app.close_command(9)
        except ValueError as exc:
            assert "recebimento registrado" in str(exc)
        else:
            raise AssertionError("close_command deveria bloquear recebimento duplicado")


def run():
    tests = [
        test_translate_sql_preserves_types,
        test_next_command_number_uses_max_plus_one,
        test_cash_expected_amount_sums_opening_and_movements,
        test_create_command_locks_table_and_uses_next_number,
        test_close_command_blocks_duplicate_receipt,
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")


if __name__ == "__main__":
    run()
