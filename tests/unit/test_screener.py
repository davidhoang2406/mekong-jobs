"""Unit tests for screener_job — no Spark, no MinIO, no vnstock network calls."""
import pandas as pd
import pytest

pytestmark = pytest.mark.unit


def _make_df(item_en_rows: list, year_cols: list, data: dict[str, list]) -> pd.DataFrame:
    """Build a mock Finance.ratio() DataFrame matching the real pivoted vnstock structure.

    Columns: item | item_en | item_id | <year cols (all named '2018' like the real API)>
    Each row is a metric; year columns hold the yearly values.
    """
    rows = []
    for label in item_en_rows:
        row = {"item": label, "item_en": label, "item_id": 0}
        vals = data.get(str(label), [None] * len(year_cols))
        for j in range(len(year_cols)):
            row[f"_c{j}"] = vals[j] if j < len(vals) else None
        rows.append(row)

    df = pd.DataFrame(rows)
    # Rename to match real vnstock: year columns all named '2018'
    df.columns = ["item", "item_en", "item_id"] + ["2018"] * len(year_cols)
    return df


def _typical_df(years: list[int] = None) -> pd.DataFrame:
    years = years or [2021, 2022, 2023, 2024, 2025]
    return _make_df(
        item_en_rows=["P/E", "P/B", "ROE (%)", "Debt/Equity", "Current Ratio", "Ratio Year Id"],
        year_cols=years,
        data={
            "P/E":           [12.0, 13.5, 15.0, 14.2, 10.5],
            "P/B":           [1.5,  1.8,  2.0,  2.1,  1.9],
            "ROE (%)":       [18.0, 20.0, 22.5, 21.0, 19.5],
            "Debt/Equity":   [0.8,  0.9,  1.0,  0.8,  0.7],
            "Current Ratio": [1.2,  1.3,  1.4,  1.4,  1.5],
            "Ratio Year Id": [str(y) for y in years],
        },
    )


# ── _latest_year_col_idx ──────────────────────────────────────────────────────

class TestLatestYearColIdx:
    def test_returns_last_year_column(self):
        from jobs.batch.screener_job import _latest_year_col_idx
        df = _typical_df([2021, 2022, 2023, 2024, 2025])
        assert _latest_year_col_idx(df) == 3 + 4  # 2025 at offset 4

    def test_skips_ratio_ttm_string(self):
        from jobs.batch.screener_job import _latest_year_col_idx
        df = _make_df(
            ["Ratio Year Id"],
            ["c0", "c1", "c2", "c3"],
            {"Ratio Year Id": ["RATIO_TTM", "RATIO_YEAR", 2023, 2024]},
        )
        assert _latest_year_col_idx(df) == 3 + 3  # 2024 at offset 3

    def test_all_non_numeric_falls_back_to_last_col(self):
        from jobs.batch.screener_job import _latest_year_col_idx
        df = _make_df(
            ["Ratio Year Id"],
            ["c0", "c1"],
            {"Ratio Year Id": ["RATIO_TTM", "RATIO_YEAR"]},
        )
        assert _latest_year_col_idx(df) == len(df.columns) - 1

    def test_no_ratio_year_id_row_falls_back(self):
        from jobs.batch.screener_job import _latest_year_col_idx
        df = _make_df(["P/E"], ["c0", "c1"], {"P/E": [14.2, 10.5]})
        assert _latest_year_col_idx(df) == len(df.columns) - 1

    def test_single_year(self):
        from jobs.batch.screener_job import _latest_year_col_idx
        df = _typical_df([2025])
        assert _latest_year_col_idx(df) == 3  # only one year col at offset 0


# ── _fetch_fundamentals (via monkeypatch) ─────────────────────────────────────

class TestFetchFundamentals:
    """Patch vnstock.Finance inside the function to avoid network calls."""

    def _patch(self, monkeypatch, df):
        """Patch `from vnstock import Finance` inside screener_job."""
        import sys
        import types

        mock_vnstock = types.ModuleType("vnstock")

        class MockFinance:
            def __init__(self, **kwargs): pass
            def ratio(self): return df

        mock_vnstock.Finance = MockFinance
        monkeypatch.setitem(sys.modules, "vnstock", mock_vnstock)

    def test_extracts_pe_ratio_from_latest_year(self, monkeypatch):
        from jobs.batch.screener_job import _fetch_fundamentals
        df = _typical_df([2021, 2022, 2023, 2024, 2025])
        self._patch(monkeypatch, df)
        records = _fetch_fundamentals(["VCB"], "VCI")
        assert len(records) == 1
        assert records[0]["pe_ratio"] == pytest.approx(10.5)  # 2025 value

    def test_extracts_roe(self, monkeypatch):
        from jobs.batch.screener_job import _fetch_fundamentals
        # Use full 5-year df so latest year (2025) has value 19.5
        df = _typical_df([2021, 2022, 2023, 2024, 2025])
        self._patch(monkeypatch, df)
        records = _fetch_fundamentals(["VCB"], "VCI")
        assert records[0]["roe"] == pytest.approx(19.5)

    def test_nan_value_stored_as_none(self, monkeypatch):
        from jobs.batch.screener_job import _fetch_fundamentals
        df = _typical_df([2025])
        df.loc[df["item_en"] == "P/E", "2018"] = float("nan")
        self._patch(monkeypatch, df)
        records = _fetch_fundamentals(["VCB"], "VCI")
        assert records[0]["pe_ratio"] is None

    def test_empty_df_returns_empty(self, monkeypatch):
        import sys, types
        mock_vnstock = types.ModuleType("vnstock")

        class MockEmpty:
            def __init__(self, **kwargs): pass
            def ratio(self): return pd.DataFrame()

        mock_vnstock.Finance = MockEmpty
        monkeypatch.setitem(sys.modules, "vnstock", mock_vnstock)

        from jobs.batch.screener_job import _fetch_fundamentals
        assert _fetch_fundamentals(["VCB"], "VCI") == []

    def test_api_exception_skips_symbol(self, monkeypatch):
        import sys, types
        mock_vnstock = types.ModuleType("vnstock")

        class MockError:
            def __init__(self, **kwargs): pass
            def ratio(self): raise RuntimeError("API error")

        mock_vnstock.Finance = MockError
        monkeypatch.setitem(sys.modules, "vnstock", mock_vnstock)

        from jobs.batch.screener_job import _fetch_fundamentals
        assert _fetch_fundamentals(["VCB"], "VCI") == []

    def test_multiple_symbols(self, monkeypatch):
        from jobs.batch.screener_job import _fetch_fundamentals
        df = _typical_df([2025])
        self._patch(monkeypatch, df)
        records = _fetch_fundamentals(["VCB", "ACB", "TCB"], "VCI")
        assert len(records) == 3
        assert {r["symbol"] for r in records} == {"VCB", "ACB", "TCB"}
