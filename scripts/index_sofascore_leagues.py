"""Rastreador e indexador maestro de ligas de SofaScore.

Descarga y cataloga todas las categorías de fútbol y competiciones mundiales
de SofaScore en data/sofascore_master_registry.json para mapeo automático instantáneo (0 ms).
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_HOSTS = [
    "https://api.sofascore.com/api/v1",
    "https://www.sofascore.com/api/v1",
]

OUTPUT_FILE = Path(__file__).resolve().parent.parent / "data" / "sofascore_master_registry.json"
ALIASES_FILE = Path(__file__).resolve().parent.parent / "data" / "sofascore_league_aliases.json"


def get_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Referer": "https://www.sofascore.com/",
        "Origin": "https://www.sofascore.com",
        "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    })
    return session


def api_get(session: requests.Session, path: str) -> Dict[str, Any]:
    for host in API_HOSTS:
        try:
            r = session.get(f"{host}{path}", timeout=10)
            if r.status_code == 200:
                payload = r.json()
                return payload if isinstance(payload, dict) else {}
        except Exception:
            continue
    return {}


def normalize(text: Any) -> str:
    s = unicodedata.normalize("NFKD", str(text or ""))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"\b(fc|cf|sc|afc|club|deportivo|futbol|football)\b", " ", s)
    s = re.sub(r"\b(w|women|woman|f|femenino|femenina|ladies)\b", " ", s)
    return " ".join(re.sub(r"[^a-z0-9]+", " ", s).split())


def crawl_categories(session: requests.Session) -> List[Dict[str, Any]]:
    print("[*] Obteniendo categorias de futbol...")
    payload = api_get(session, "/sport/football/categories")
    categories = payload.get("categories") or []
    print(f"[+] {len(categories)} categorias encontradas.")
    return categories


def crawl_tournaments_for_category(session: requests.Session, cat: Dict[str, Any]) -> List[Dict[str, Any]]:
    cat_id = cat.get("id")
    cat_name = cat.get("name") or ""
    country_name = (cat.get("country") or {}).get("name") or cat_name
    tournaments = []

    # Unique tournaments
    payload_unique = api_get(session, f"/category/{cat_id}/unique-tournaments")
    for ut in payload_unique.get("uniqueTournaments") or []:
        ut_id = ut.get("id")
        if not ut_id:
            continue
        tournaments.append({
            "id": ut_id,
            "name": ut.get("name"),
            "slug": ut.get("slug"),
            "category_id": cat_id,
            "category_name": cat_name,
            "country_name": country_name,
            "is_unique": True,
        })

    # Standard tournaments
    payload_tourneys = api_get(session, f"/category/{cat_id}/tournaments")
    for t in payload_tourneys.get("tournaments") or []:
        t_id = t.get("id")
        if not t_id:
            continue
        ut = t.get("uniqueTournament")
        tournaments.append({
            "id": ut.get("id") if ut else t_id,
            "tournament_id": t_id,
            "name": t.get("name"),
            "slug": t.get("slug"),
            "category_id": cat_id,
            "category_name": cat_name,
            "country_name": country_name,
            "is_unique": bool(ut),
        })

    return tournaments


def run_indexing() -> None:
    session = get_session()
    categories = crawl_categories(session)
    if not categories:
        print("[!] No se pudieron obtener categorias de SofaScore.")
        return

    all_tournaments: Dict[str, Dict[str, Any]] = {}
    print("[*] Indexando torneos y ligas de todo el mundo...")
    for idx, cat in enumerate(categories):
        cat_name = cat.get("name")
        print(f"[{idx+1}/{len(categories)}] Procesando {cat_name}...")
        try:
            tourneys = crawl_tournaments_for_category(session, cat)
            for t in tourneys:
                t_name = t.get("name") or ""
                country = t.get("country_name") or ""
                norm_full = normalize(f"{country} {t_name}")
                norm_name = normalize(t_name)
                all_tournaments[str(t["id"])] = t
                if norm_full:
                    all_tournaments[norm_full] = t
                if norm_name:
                    all_tournaments[norm_name] = t
            time.sleep(0.15)
        except Exception as e:
            print(f"[!] Error en categoria {cat_name}: {e}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(all_tournaments, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[+] {len(all_tournaments)} entradas indexadas en {OUTPUT_FILE}")


if __name__ == "__main__":
    run_indexing()
