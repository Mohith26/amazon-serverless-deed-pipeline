from deedstream import data_gen


def test_generation_is_deterministic():
    a = data_gen.generate_day("HARRIS", "2026-08-01", 50)
    b = data_gen.generate_day("HARRIS", "2026-08-01", 50)
    assert a == b


def test_different_days_differ():
    a = data_gen.generate_day("HARRIS", "2026-08-01", 50)
    b = data_gen.generate_day("HARRIS", "2026-08-02", 50)
    assert a != b


def test_count_and_unique_doc_ids():
    recs = data_gen.generate_day("DALLAS", "2026-08-01", 200)
    assert len(recs) == 200
    assert len({r.doc_id for r in recs}) == 200


def test_required_fields_present():
    rec = data_gen.generate_day("HARRIS", "2026-08-01", 1)[0]
    for field in ("county", "filing_date", "doc_id", "doc_type", "grantor", "grantee"):
        assert getattr(rec, field)
