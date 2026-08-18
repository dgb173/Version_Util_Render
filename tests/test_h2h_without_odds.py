from bs4 import BeautifulSoup

from modules.estudio_scraper import (
    _analizar_precedente_handicap,
    get_match_details_from_row_of,
)


def test_h2h_row_without_odds_is_kept_with_na_handicap():
    soup = BeautifulSoup(
        """
        <table>
          <tr id="tr3_2969997" index="2969997">
            <td>ICE LD1</td>
            <td><span name="timeData" data-t="2026-05-20 19:00:00"></span></td>
            <td><a onclick="team(10)">Tindastoll Neisti (W)</a></td>
            <td><span class="fscore_3">0-3 (0-1)</span></td>
            <td><a onclick="team(20)">Volsungur Husavik (W)</a></td>
          </tr>
        </table>
        """,
        "lxml",
    )

    parsed = get_match_details_from_row_of(
        soup.find("tr"),
        score_class_selector="fscore_3",
    )

    assert parsed is not None
    assert parsed["matchIndex"] == "2969997"
    assert parsed["date"] == "2026-05-20"
    assert parsed["score"] == "0:3"
    assert parsed["ahLine"] == "N/A"


def test_missing_historical_ah_preserves_score_for_precacheo():
    result = _analizar_precedente_handicap(
        {
            "res_raw": "0-3",
            "ah_raw": "N/A",
            "home": "Tindastoll Neisti (W)",
            "away": "Volsungur Husavik (W)",
        },
        ah_actual_num=2.5,
        favorito_actual_name="Volsungur Husavik (W)",
        main_home_team_name="Volsungur Husavik (W)",
    )

    assert result["movement"] == "N/A"
    assert result["result"] == "0:3"
    assert result["evaluation"] == "CUBIERTO"
    assert result["is_covered"] is True
    assert "cuota histórica no estaba disponible" in result["html"]
