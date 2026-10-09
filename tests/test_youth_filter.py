import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest
from modules.youth_filter import (
    is_unbettable_youth_match,
    is_unbettable_youth_league,
    is_unbettable_youth_team,
    filter_youth_matches,
)


def test_czech_u19_match_filtered():
    match = {
        "match_id": "3008855",
        "league_name": "Czech Republic U19 League",
        "home_name": "Viktoria Plzen U19",
        "away_name": "Dukla Praha U19",
    }
    assert is_unbettable_youth_match(match) is True


def test_user_requested_excluded_leagues():
    leagues = [
        # Ligas U19 / Sub-19
        "Czech Republic U19 League",
        "Hungary U19 A League",
        "Turkey A2 League U19",
        "Turkey U19 League",
        "Norwegian Junior U19",
        "Mexico Liga MX U19",
        "Mexico Liga MX U19 Femenil",
        "Portugal Juniores A1 U19",
        "Portugal Juniores A2 U19",
        "Portugal U19 League B",
        "Spain Youth League",
        "Spain Youth Cup",
        "UEFA Youth League U19",
        "France Youth U19 League",
        "Croatia U19 League",
        "Denmark Youth U19",
        "Slovakia U19 League",
        "Slovenia U19",
        "Serbia U19 League",
        "Greece U19",
        "Qatar U19 League",
        "Jordan U19 League",
        "United Arab Emirates U19",
        "Vietnam Championship U19",
        "Switzerland U19 Elite",
        "Bolivia Liga Nacional U19",
        "Bosnia Herzegovina U19 Liga",
        # Específicos por país
        "Italian Campionato Primavera 1",
        "Italy Campionato Primavera 2",
        "Italian Youth Cup",
        "Poland Mloda Ekstraklasa",
        "Israel Youth League",
        "Russia Youth Championship",
        "Saudi Arabia Youth League",
        "Cambodia Development League",
        # Ligas U20 / Brasil / etc.
        "Brazil national youth (U20) Football Championship",
        "Brazil Campeonato Paulista Youth",
        "Brazil Campeonato Mineiro U20",
        "Brazil Campeonato Carioca U20 Women",
        "Brazil Campeonato Catarinense U20",
        "Brazil Campeonato Gaucho Youth",
        "Brazil Campeonato Paraense U20",
        "Brazil Copa Nordeste U20",
        "Brasil Copa SP Juniores",
        "Brazil Youth cup",
        "Sub-20 Copa do Brasil",
        "Qatar U20 League",
        "Colombia U20 League",
        "Ireland U20 League",
    ]
    for league in leagues:
        assert is_unbettable_youth_league(league) is True, f"Should be EXCLUDED: {league}"
        assert is_unbettable_youth_match({"league_name": league}) is True, f"Should be EXCLUDED: {league}"


def test_user_requested_kept_leagues_allowed():
    # El usuario indicó explícitamente: "El resto debe seguir que si se puede apostar"
    # Ligas U21, U23, Reservas, etc. deben pasar sin ser filtradas.
    bettable_matches = [
        {"league_name": "Belgian U21", "home_name": "Beerschot Wilrijk U21", "away_name": "Hasselt U21"},
        {"league_name": "England U21 Professional Development League 2", "home_name": "Barnsley U21", "away_name": "Hull U21"},
        {"league_name": "England U21 League Cup", "home_name": "Southampton U21", "away_name": "Queens Park R U21"},
        {"league_name": "England U21 Premier League", "home_name": "Arsenal U21", "away_name": "Chelsea U21"},
        {"league_name": "Mexico Youth U21", "home_name": "Puebla U21", "away_name": "Leon U21"},
        {"league_name": "Thailand U21 League", "home_name": "Team A U21", "away_name": "Team B U21"},
        {"league_name": "United Arab Emirates U21", "home_name": "Al Ain U21", "away_name": "Al Jazira U21"},
        {"league_name": "Oman U21 Championship", "home_name": "Seeb U21", "away_name": "Dhofar U21"},
        {"league_name": "Netherlands U21 League", "home_name": "Feyenoord U21", "away_name": "Ajax U21"},
        {"league_name": "Liga Next Gen U23", "home_name": "Estoril U23", "away_name": "Sporting Lisbon Sad U23"},
        {"league_name": "Portugal Liga Revelacao U23", "home_name": "Benfica U23", "away_name": "Braga U23"},
        {"league_name": "Mexico Liga MX U23", "home_name": "America U23", "away_name": "Chivas U23"},
        {"league_name": "Argentina Reserve League", "home_name": "Boca Juniors Reserves", "away_name": "River Plate Reserves"},
        {"league_name": "Uruguay Reserve League", "home_name": "Penarol Reserves", "away_name": "Nacional Reserves"},
        {"league_name": "England Reserves League", "home_name": "Chelsea Reserves", "away_name": "Arsenal Reserves"},
        {"league_name": "Algeria U20 League", "home_name": "Team A", "away_name": "Team B"},
        {"league_name": "Jordan Championship U20", "home_name": "Al Wehdat U20", "away_name": "Al Ahli U20"},
        {"league_name": "Spanish La Liga", "home_name": "Real Madrid", "away_name": "Barcelona"},
        {"league_name": "Holland Eerste Divisie", "home_name": "De Graafschap", "away_name": "VVV Venlo"},
    ]
    for match in bettable_matches:
        assert is_unbettable_youth_match(match) is False, f"Should be KEPT (false positive): {match['league_name']}"


def test_filter_youth_matches_preserves_bettable():
    matches = [
        {"match_id": "1", "league_name": "Czech Republic U19 League"},
        {"match_id": "2", "league_name": "Belgian U21", "home_name": "Beerschot Wilrijk U21", "away_name": "Hasselt U21"},
        {"match_id": "3", "league_name": "Argentina Reserve League", "home_name": "Boca Reserves", "away_name": "River Reserves"},
        {"match_id": "4", "league_name": "Italian Campionato Primavera 1"},
        {"match_id": "5", "league_name": "Liga Next Gen U23", "home_name": "Estoril U23", "away_name": "Sporting U23"},
        {"match_id": "6", "home_name": "Viktoria Plzen U19", "away_name": "Dukla Praha U19"},
    ]
    clean = filter_youth_matches(matches)
    # Deben quedar exactamente 2, 3 y 5
    assert [m["match_id"] for m in clean] == ["2", "3", "5"]
