from amfi_lakehouse.parse import parse_amfi_lines


def test_parse_daily_8_col_layout_with_headers():
    sample_lines = [
        "Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Plan;Option;Net Asset Value;Date",
        "",
        "Open Ended Schemes(Equity Scheme - Large Cap Fund)",
        "",
        "HDFC Mutual Fund",
        "100033;INF179K01BE2;-;HDFC Top 100 Fund - Growth Plan;Regular Plan;Growth Option;1125.4500;17-Nov-2026",
        "100034;INF179K01BF9;-;HDFC Top 100 Fund - IDCW;Regular Plan;IDCW Option;N.A.;17-Nov-2026",
    ]
    rows = parse_amfi_lines(sample_lines, ingestion_date="2026-11-18")
    assert len(rows) == 2

    first = rows[0]
    assert first["scheme_code"] == 100033
    assert first["isin_growth"] == "INF179K01BE2"
    assert first["isin_reinvest"] is None
    assert first["scheme_name"] == "HDFC Top 100 Fund - Growth Plan"
    assert first["plan"] == "Regular Plan"
    assert first["option"] == "Growth Option"
    assert first["nav"] == 1125.45
    assert first["nav_date"] == "2026-11-17"
    assert first["amc_name"] == "HDFC Mutual Fund"
    assert first["scheme_category"] == "Open Ended Schemes(Equity Scheme - Large Cap Fund)"

    second = rows[1]
    assert second["nav"] is None


def test_parse_history_8_col_layout():
    sample_lines = [
        "Scheme Code;NAV Name;Plan;Option;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Date",
        "Open Ended Schemes ( Equity Scheme - Multi Cap Fund )",
        "Aditya Birla Sun Life Mutual Fund",
        "148921;Aditya Birla Sun Life Multi-Cap Fund-Direct Growth;Direct Plan;GROWTH;INF209KB1Y49;;23.20;11-Nov-2026",
    ]
    rows = parse_amfi_lines(sample_lines, ingestion_date="2026-11-18")
    assert len(rows) == 1
    row = rows[0]
    assert row["scheme_code"] == 148921
    assert row["scheme_name"] == "Aditya Birla Sun Life Multi-Cap Fund-Direct Growth"
    assert row["plan"] == "Direct Plan"
    assert row["option"] == "GROWTH"
    assert row["isin_growth"] == "INF209KB1Y49"
    assert row["isin_reinvest"] is None
    assert row["nav"] == 23.20
    assert row["nav_date"] == "2026-11-11"