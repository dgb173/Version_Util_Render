from pathlib import Path

from src.modules import estudio_scraper


def test_analysis_cache_is_bounded(monkeypatch):
    monkeypatch.setattr(estudio_scraper, "ANALYSIS_CACHE_MAX_ENTRIES", 2)
    estudio_scraper._analysis_cache.clear()

    estudio_scraper._write_cache(
        estudio_scraper._analysis_cache,
        "one",
        {"match_id": "one"},
        estudio_scraper._analysis_cache_lock,
    )
    estudio_scraper._write_cache(
        estudio_scraper._analysis_cache,
        "two",
        {"match_id": "two"},
        estudio_scraper._analysis_cache_lock,
    )
    estudio_scraper._write_cache(
        estudio_scraper._analysis_cache,
        "three",
        {"match_id": "three"},
        estudio_scraper._analysis_cache_lock,
    )

    assert len(estudio_scraper._analysis_cache) == 2
    assert "one" not in estudio_scraper._analysis_cache


def test_render_runtime_has_memory_guards_and_no_startup_fast_store():
    root = Path(__file__).resolve().parents[1]
    render_config = (root / "render.yaml").read_text(encoding="utf-8")
    wsgi_source = (root / "wsgi.py").read_text(encoding="utf-8")

    assert "--max-requests 100" in render_config
    assert "ANALYSIS_MAX_CONCURRENCY" in render_config
    assert "PLAYWRIGHT_MAX_CONCURRENCY" in render_config
    assert "SCRAPE_MAX_CONCURRENCY" in render_config
    assert "build_precache_fast_store" not in wsgi_source
