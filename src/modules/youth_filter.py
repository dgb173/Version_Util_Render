"""Módulo para identificar y filtrar ligas y partidos juveniles no apostables solicitados por el usuario.

Filtra ÚNICAMENTE:
1. Ligas U19 / Sub-19 (y categorías menores U15-U19):
   - Czech Republic U19 League
   - Hungary U19 A League
   - Turkey A2 League U19 / Turkey U19 League
   - Norwegian Junior U19
   - Mexico Liga MX U19 / U19 Femenil
   - Portugal Juniores A1 U19 / A2 U19 / Portugal U19 League B
   - Spain Youth League / Spain Youth Cup
   - UEFA Youth League U19
   - France Youth U19 League
   - Croatia U19 League
   - Denmark Youth U19
   - Slovakia U19 League
   - Slovenia U19
   - Serbia U19 League
   - Greece U19
   - Qatar U19 League
   - Jordan U19 League
   - United Arab Emirates U19
   - Vietnam Championship U19
   - Switzerland U19 Elite
   - Bolivia Liga Nacional U19 / Bosnia Herzegovina U19 Liga

2. Torneos juveniles específicos por país:
   - Italian Campionato Primavera 1, 2 y 4 / Italian Youth Cup
   - Poland Młoda Ekstraklasa
   - Israel Youth League
   - Russia Youth Championship
   - Saudi Arabia Youth League
   - Cambodia Development League

3. Ligas U20 / Juveniles específicas solicitadas:
   - Brazil national youth (U20) Football Championship
   - Brazil Campeonato Paulista Youth / Mineiro U20 / Carioca U20 / Catarinense U20 / Gaucho Youth / Paraense U20 / Nordeste U20
   - Brasil Copa SP Juniores / Brazil Youth Cup
   - Sub-20 Copa do Brasil
   - Qatar U20 League
   - Colombia U20 League
   - Ireland U20 League

TODAS las demás ligas (U21, U22, U23, Reservas como Argentina/Uruguay/Inglaterra, otras U20 apostables)
se CONSERVAN sin tocar porque sí se puede apostar en ellas.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional

# 1. Todas las ligas U19 / Sub-19 (y menores U15-U19)
_U19_PATTERNS = [
    r"\bu[\-_]?(?:1[5-9])\b",
    r"\bsub[\s\-_]?(?:1[5-9])\b",
    r"\bunder[\s\-_]?(?:1[5-9])\b",
    r"\bspain\s+youth\b",
    r"\byouth\s+league\s+u19\b",
    r"\bnorwegian\s+junior\s+u19\b",
    r"\bswitzerland\s+u19\b",
    r"\bportugal\s+juniores\b",
]

# 2. Torneos específicos por país
_SPECIFIC_COUNTRY_PATTERNS = [
    r"\bprimavera\b",
    r"\bitalian\s+youth\s+cup\b",
    r"\bmloda\b",
    r"\bisrael\s+youth\s+league\b",
    r"\brussia\s+youth\s+championship\b",
    r"\bsaudi\s+arabia\s+youth\s+league\b",
    r"\bcambodia\s+development\s+league\b",
]

# 3. Ligas U20 / Brasil / Qatar / Colombia / Irlanda pedidas expresamente
_SPECIFIC_U20_PATTERNS = [
    r"brazil.*national\s+youth",
    r"brazil.*paulista\s+youth",
    r"brazil.*mineiro\s+u20",
    r"brazil.*carioca\s+u20",
    r"brazil.*catarinense\s+u20",
    r"brazil.*gaucho\s+youth",
    r"brazil.*paraense\s+u20",
    r"brazil.*nordeste\s+u20",
    r"copa\s+sp\s+juniores",
    r"brazil\s+youth",
    r"copa\s+do\s+brasil.*sub[\s\-_]?20",
    r"sub[\s\-_]?20.*copa\s+do\s+brasil",
    r"qatar\s+u20\s+league",
    r"colombia\s+u20\s+league",
    r"ireland\s+u20\s+league",
]

UNBETTABLE_LEAGUE_PATTERNS = [
    *_U19_PATTERNS,
    *_SPECIFIC_COUNTRY_PATTERNS,
    *_SPECIFIC_U20_PATTERNS,
]

# Equipos que delatan un partido U19 o Primavera cuando no hay liga identificada
UNBETTABLE_TEAM_PATTERNS = [
    r"\bu[\-_]?(?:1[5-9])\b",
    r"\bsub[\s\-_]?(?:1[5-9])\b",
    r"\bunder[\s\-_]?(?:1[5-9])\b",
    r"\bprimavera\b",
]

_RE_LEAGUE = re.compile("|".join(UNBETTABLE_LEAGUE_PATTERNS), re.IGNORECASE)
_RE_TEAM = re.compile("|".join(UNBETTABLE_TEAM_PATTERNS), re.IGNORECASE)


def is_unbettable_youth_league(league_name: Optional[str]) -> bool:
    """Devuelve True si la liga pertenece al listado estricto de torneos no apostables."""
    if not league_name:
        return False
    return bool(_RE_LEAGUE.search(str(league_name)))


def is_unbettable_youth_team(team_name: Optional[str]) -> bool:
    """Devuelve True si el equipo contiene sufijo U19/Sub-19 o Primavera."""
    if not team_name:
        return False
    return bool(_RE_TEAM.search(str(team_name)))


def is_unbettable_youth_match(match: Optional[Dict[str, Any]]) -> bool:
    """Verifica si un partido corresponde al listado de competiciones o equipos excluidos."""
    if not isinstance(match, dict):
        return False

    league = (
        match.get("league")
        or match.get("league_name")
        or match.get("liga")
        or match.get("competition")
        or match.get("competition_name")
        or ""
    )
    if is_unbettable_youth_league(league):
        return True

    home = (
        match.get("home_team")
        or match.get("home_name")
        or match.get("home")
        or ""
    )
    if is_unbettable_youth_team(home):
        return True

    away = (
        match.get("away_team")
        or match.get("away_name")
        or match.get("away")
        or ""
    )
    if is_unbettable_youth_team(away):
        return True

    return False


def filter_youth_matches(matches: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filtra y excluye únicamente las ligas y partidos solicitados por el usuario."""
    if not matches:
        return []
    return [m for m in matches if isinstance(m, dict) and not is_unbettable_youth_match(m)]
