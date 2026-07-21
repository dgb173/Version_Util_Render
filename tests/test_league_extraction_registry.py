from modules import league_extraction_registry as registry


def test_registry_persists_round_outside_sql(tmp_path, monkeypatch):
    registry_path = tmp_path / "league_extractions.json"
    monkeypatch.setattr(registry, "REGISTRY_PATH", registry_path)

    extraction = registry.create_extraction(
        league_id="381",
        league_name="Iceland Division 1",
        season="2025",
        company_id=8,
        target_ah=None,
        label="Islandia 2025",
        matches=[
            {
                "id": "2710535",
                "sub_id": "44",
                "sub_name": "League",
                "round": "9",
                "date": "2025-06-14",
                "home": "Local",
                "away": "Visitante",
                "visible_ah": 0.75,
            }
        ],
    )

    registry.update_extraction_status(extraction["extraction_id"], "running")
    registry.update_match(
        extraction["extraction_id"],
        "2710535",
        {"status": "saved", "bucket": "data_precacheo.json"},
    )
    stored = registry.get_extraction(extraction["extraction_id"])

    assert stored["status"] == "running"
    assert stored["label"] == "Islandia 2025"
    assert stored["matches"][0]["round"] == "9"
    assert stored["matches"][0]["sub_id"] == "44"
    assert stored["matches"][0]["status"] == "saved"
    assert registry.list_extractions()[0]["counts"] == {"saved": 1}


def test_register_existing_league_is_idempotent_and_marks_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(registry, "REGISTRY_PATH", tmp_path / "league_extractions.json")
    kwargs = {
        "league_id": "1063",
        "league_name": "National Premier Leagues Capital Football",
        "season": "2025",
        "company_id": 8,
        "target_ah": None,
        "label": "Capital Football 2025",
        "matches": [
            {"id": "1", "round": "1", "already_in_sql": True, "sql_bucket": "data_precacheo.json"},
            {"id": "2", "round": "2", "already_in_sql": False},
        ],
    }
    first = registry.register_existing_league(**kwargs)
    second = registry.register_existing_league(**kwargs)

    assert first["extraction_id"] == second["extraction_id"]
    assert second["label"] == "Capital Football 2025"
    assert [match["status"] for match in second["matches"]] == ["exists", "missing"]
