import math
import logging

LOGGER = logging.getLogger(__name__)

def _parse_float(val, default=0.0):
    if val is None:
        return default
    try:
        val_str = str(val).split("->")[0].strip()
        val_str = val_str.replace("AH", "").replace("ah", "").strip()
        if not val_str or val_str in ["-", "?", "N/A"]:
            return default
        return float(val_str)
    except (TypeError, ValueError):
        return default

def evaluar_ah(goles_favor, goles_contra, ah_favor):
    """
    Evalúa si un hándicap asiático fue cubierto o no.
    ah_favor: el hándicap asignado al equipo (ej: -1.0, 0.5, etc.)
    Retorna (label, color, cubrio)
    """
    diff = goles_favor - goles_contra
    net = diff + ah_favor
    if net >= 0.35:
        return "CUBRE ✅", "#10b981", True  # Verde, cubierto
    elif math.isclose(net, 0.25, abs_tol=0.05):
        return "MITAD GANADO ✅", "#10b981", True  # Verde, cubierto
    elif math.isclose(net, 0.0, abs_tol=0.05):
        return "PUSH / VOID 🤝", "#6b7280", False  # Gris, void (no cuenta como cubierto para tasa de éxito)
    elif math.isclose(net, -0.25, abs_tol=0.05):
        return "MITAD PERDIDO ❌", "#dc2626", False  # Rojo, no cubierto
    else:
        return "NO CUBRE ❌", "#dc2626", False  # Rojo, no cubierto

def analizar_partido_handicap_similar(match_data):
    """
    Analizador de Hándicaps del Favorito.
    Identifica quién es el favorito del partido actual y analiza su rendimiento
    bajo la línea de hándicap actual y otras líneas en la misma condición (Casa/Fuera).
    """
    if not isinstance(match_data, dict):
        return {
            "alerta": "DATOS INVÁLIDOS",
            "veredicto": "ERROR",
            "explicacion": "No se recibieron datos de partido válidos.",
            "color": "gray"
        }

    home_name = match_data.get("home_name") or match_data.get("home_team") or "Local"
    away_name = match_data.get("away_name") or match_data.get("away_team") or "Visitante"

    # Obtener el hándicap de la línea principal
    ah_actual_raw = match_data.get("main_match_odds", {}).get("ah_linea")
    if ah_actual_raw is None:
        ah_actual_raw = match_data.get("handicap")

    ah_actual = _parse_float(ah_actual_raw)

    # Determinar quién es el favorito según el hándicap
    # AH < 0: Visitante favorito (juega fuera)
    # AH > 0: Local favorito (juega en casa)
    # AH = 0: Sin favorito claro (usamos el local por defecto)
    if ah_actual < 0:
        favorito_name = away_name
        es_local = False
        condicion_label = "Fuera de Casa"
        partidos_recientes = match_data.get("recent_away_matches") or []
        ah_favorito_hoy = ah_actual # ej: -1.0
    elif ah_actual > 0:
        favorito_name = home_name
        es_local = True
        condicion_label = "en Casa"
        partidos_recientes = match_data.get("recent_home_matches") or []
        ah_favorito_hoy = -ah_actual # ej: -1.0 (se expresa como negativo para el favorito)
    else:
        # AH = 0.0 (Empate técnico)
        # Por defecto tomamos al local como favorito de referencia para el análisis
        favorito_name = home_name
        es_local = True
        condicion_label = "en Casa (Ref)"
        partidos_recientes = match_data.get("recent_home_matches") or []
        ah_favorito_hoy = 0.0

    # Rango de hándicaps similares
    ah_favorito_hoy_abs = abs(ah_favorito_hoy)

    def es_handicap_similar(ah_hist_val):
        ah_hist_abs = abs(ah_hist_val)
        if ah_favorito_hoy_abs <= 0.75:
            return ah_hist_abs <= 0.75
        else:
            return abs(ah_hist_abs - ah_favorito_hoy_abs) <= 0.55

    precedentes_similares = []
    resumen_handicaps = {} # ah_hist -> {jugados, cubiertos}

    for m in partidos_recientes:
        # Determinar el hándicap del favorito en este partido histórico
        # En historical_matches, ahLine es el AH con respecto al Local.
        ah_line_raw = m.get('ahLine') or m.get('handicap_line_raw')
        ah_val = _parse_float(ah_line_raw)

        # Hándicap real del favorito en el partido histórico:
        # Si el favorito juega en casa (Local): es ah_val
        # Si el favorito juega fuera (Visitante): es -ah_val
        ah_hist = ah_val if es_local else -ah_val

        rival = m.get('away') if es_local else m.get('home')
        score = m.get('score')
        date = m.get('date')

        # Parsear goles
        goles_l, goles_v = 0, 0
        if score and ":" in score:
            try:
                parts = score.split(":")
                goles_l = int(parts[0])
                goles_v = int(parts[1])
            except Exception:
                pass

        goles_favor = goles_l if es_local else goles_v
        goles_contra = goles_v if es_local else goles_l

        result_label, color, cubrio = evaluar_ah(goles_favor, goles_contra, ah_hist)

        # 1. Almacenar en la tabla de hándicap similar si corresponde
        if es_handicap_similar(ah_hist):
            precedentes_similares.append({
                "date": date,
                "rival": rival,
                "ah": ah_hist,
                "score": score,
                "result": result_label,
                "color": color,
                "cubrio": cubrio
            })

        # 2. Almacenar en el resumen de hándicaps por tipo
        # Redondear hándicap a cuartos de punto para consistencia
        ah_key = f"AH {ah_hist:+.2f}".replace("+.00", "+0.00").replace("-.00", "-0.00")
        if ah_key not in resumen_handicaps:
            resumen_handicaps[ah_key] = {"ah_val": ah_hist, "jugados": 0, "cubiertos": 0}

        resumen_handicaps[ah_key]["jugados"] += 1
        if cubrio:
            resumen_handicaps[ah_key]["cubiertos"] += 1

    # Ordenar el resumen de hándicaps por valor de hándicap de menor a mayor (más difícil a más fácil)
    resumen_ordenado = sorted(resumen_handicaps.values(), key=lambda x: x['ah_val'])

    # Construir HTML del reporte
    html = f"""
    <div style="font-family: system-ui, -apple-system, sans-serif; color: #1e293b;">

        <!-- Cabecera del Análisis -->
        <div class="d-flex justify-content-between align-items-center p-3 mb-4" style="background: linear-gradient(135deg, #f8fafc, #f1f5f9); border: 1px solid #e2e8f0; border-radius: 8px; box-shadow: inset 0 1px 2px rgba(0,0,0,0.02);">
            <div>
                <span class="text-muted text-uppercase fw-bold" style="font-size: 0.7rem; letter-spacing: 0.8px; display: block; margin-bottom: 2px;">Analizando Favorito</span>
                <span class="fw-bold text-dark" style="font-size: 1.1rem;">{favorito_name} ({condicion_label})</span>
            </div>
            <div class="text-end">
                <span class="text-muted text-uppercase fw-bold" style="font-size: 0.7rem; letter-spacing: 0.8px; display: block; margin-bottom: 2px;">Línea de Hoy</span>
                <span class="badge bg-primary px-2.5 py-1.5" style="font-size: 0.8rem; font-weight: 600;">
                    AH {ah_favorito_hoy:+.2f}
                </span>
            </div>
        </div>
    """

    # Tabla 1: Historial de Hándicaps Similares
    if precedentes_similares:
        html += f"""
        <div class="mb-4">
            <h6 class="fw-bold text-primary mb-2.5" style="font-size: 0.9rem; display: flex; align-items: center; gap: 6px;">
                <i class="fa-solid fa-clock-rotate-left"></i> Partidos Recientes con Hándicap Similar ({ah_favorito_hoy:+.2f})
            </h6>
            <div class="table-responsive" style="border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; background: white;">
                <table class="table table-sm table-hover mb-0 align-middle" style="font-size: 0.8rem; min-width: 320px;">
                    <thead style="background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
                        <tr>
                            <th class="ps-3 py-2 text-muted fw-bold" style="width: 85px;">Fecha</th>
                            <th class="py-2 text-muted fw-bold">Rival</th>
                            <th class="py-2 text-center text-muted fw-bold" style="width: 80px;">Hándicap</th>
                            <th class="py-2 text-center text-muted fw-bold" style="width: 70px;">Resultado</th>
                            <th class="pe-3 py-2 text-center text-muted fw-bold" style="width: 110px;">Hándicap</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        for p in precedentes_similares:
            html += f"""
                        <tr style="border-bottom: 1px solid #f1f5f9;">
                            <td class="ps-3 py-2 text-muted">{p['date']}</td>
                            <td class="py-2 fw-semibold text-dark">{p['rival']}</td>
                            <td class="py-2 text-center fw-bold text-primary">{p['ah']:+.2f}</td>
                            <td class="py-2 text-center fw-bold" style="font-size: 0.85rem;">{p['score']}</td>
                            <td class="pe-3 py-2 text-center">
                                <span class="badge" style="background-color: {p['color']}; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 600; font-size: 0.7rem; display: inline-block;">
                                    {p['result']}
                                </span>
                            </td>
                        </tr>
            """
        html += """
                    </tbody>
                </table>
            </div>
        </div>
        """
    else:
        html += f"""
        <div class="alert alert-light border p-3 mb-4 text-center text-muted" style="font-size: 0.8rem;">
            <i class="fa-solid fa-circle-info me-1"></i> No se encontraron partidos {condicion_label} con hándicaps similares.
        </div>
        """

    # Tabla 2: Rendimiento por Tipo de Hándicap (La columna con veces superado fuera de casa)
    if resumen_ordenado:
        html += f"""
        <div class="mb-4">
            <h6 class="fw-bold text-orange mb-2.5" style="font-size: 0.9rem; display: flex; align-items: center; gap: 6px; color: #ea580c;">
                <i class="fa-solid fa-chart-column"></i> Cobertura por Línea de Hándicap ({condicion_label})
            </h6>
            <div class="table-responsive" style="border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden; background: white;">
                <table class="table table-sm table-hover mb-0 align-middle" style="font-size: 0.8rem; min-width: 320px;">
                    <thead style="background-color: #f8fafc; border-bottom: 1px solid #e2e8f0;">
                        <tr>
                            <th class="ps-4 py-2 text-muted fw-bold">Línea de Hándicap</th>
                            <th class="py-2 text-center text-muted fw-bold" style="width: 100px;">Partidos</th>
                            <th class="py-2 text-center text-muted fw-bold" style="width: 120px;">Veces Superado</th>
                            <th class="pe-4 py-2 text-center text-muted fw-bold" style="width: 100px;">Rendimiento</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        for r in resumen_ordenado:
            pct = (r['cubiertos'] / r['jugados']) * 100 if r['jugados'] > 0 else 0

            # Formatear color del badge según rendimiento
            if pct >= 65:
                color_pct = "#10b981" # verde
            elif pct >= 45:
                color_pct = "#6b7280" # gris
            else:
                color_pct = "#dc2626" # rojo

            # Resaltar la línea de hándicap actual de hoy
            is_today_line = math.isclose(r['ah_val'], ah_favorito_hoy, abs_tol=0.05)
            row_style = 'style="background-color: #f8fafc; font-weight: bold; border-left: 3px solid #3b82f6;"' if is_today_line else 'style="border-bottom: 1px solid #f1f5f9;"'
            badge_today = ' <span class="badge bg-primary" style="font-size:0.6rem; padding: 2px 4px; vertical-align: middle;">HOY</span>' if is_today_line else ''

            html += f"""
                        <tr {row_style}>
                            <td class="ps-4 py-2 text-dark">{r['ah_val']:+.2f}{badge_today}</td>
                            <td class="py-2 text-center text-secondary">{r['jugados']}</td>
                            <td class="py-2 text-center fw-semibold text-dark">{r['cubiertos']} de {r['jugados']}</td>
                            <td class="pe-4 py-2 text-center">
                                <span class="badge" style="background-color: {color_pct}; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 0.7rem;">
                                    {pct:.1f}%
                                </span>
                            </td>
                        </tr>
            """
        html += """
                    </tbody>
                </table>
            </div>
        </div>
        """

    # Veredicto e implicaciones estadísticas
    total_similares = len(precedentes_similares)
    cubiertos_similares = sum(1 for p in precedentes_similares if p['cubrio'])
    pct_similares = (cubiertos_similares / total_similares) * 100 if total_similares > 0 else 0

    color_veredicto = "#6b7280"
    veredicto_titulo = "SIN CONCLUSIÓN"
    color_bg_veredicto = "#f8fafc"
    veredicto_desc = ""

    if total_similares > 0:
        if pct_similares >= 65:
            color_veredicto = "#10b981"
            color_bg_veredicto = "#ecfdf5"
            veredicto_titulo = f"SOPORTE DE FAVORITO ({condicion_label})"
            veredicto_desc = f"{favorito_name} tiene una tasa de cobertura muy alta ({pct_similares:.1f}%) bajo hándicaps similares {condicion_label}. Superó la línea en {cubiertos_similares} de los {total_similares} precedentes."
        elif pct_similares <= 35:
            color_veredicto = "#dc2626"
            color_bg_veredicto = "#fef2f2"
            veredicto_titulo = f"RIESGO EN FAVORITO ({condicion_label})"
            veredicto_desc = f"{favorito_name} tiene problemas para cubrir hándicaps similares {condicion_label}. Falló en cubrir la línea en la mayoría de sus precedentes ({pct_similares:.1f}% de cobertura)."
        else:
            veredicto_titulo = "HISTORIAL EQUILIBRADO"
            veredicto_desc = f"{favorito_name} muestra un historial neutro/equilibrado de cobertura bajo esta línea {condicion_label} ({pct_similares:.1f}% de éxito)."
    else:
        veredicto_desc = f"No hay partidos registrados de {favorito_name} {condicion_label} con hándicap similar a {ah_favorito_hoy:+.2f}."

    html += f"""
        <!-- Veredicto Final -->
        <div class="p-3.5 mb-2" style="background-color: {color_bg_veredicto}; border-left: 4px solid {color_veredicto}; border-radius: 6px; box-shadow: 0 1px 2px rgba(0,0,0,0.02); border: 1px solid #e2e8f0; border-left-width: 4px;">
            <div class="d-flex align-items-center gap-2 mb-2">
                <i class="fa-solid fa-circle-check" style="color: {color_veredicto}; font-size: 1rem;"></i>
                <span class="fw-bold text-uppercase" style="font-size: 0.8rem; letter-spacing: 0.5px; color: {color_veredicto};">{veredicto_titulo}</span>
            </div>
            <p class="mb-0 text-secondary" style="font-size: 0.8rem; line-height: 1.45;">{veredicto_desc}</p>
        </div>

    </div>
    """

    return {
        "alerta": f"HISTORIAL HÁNDICAP FAVORITO ({condicion_label.upper()})",
        "veredicto": veredicto_titulo,
        "explicacion": html,
        "color": color_veredicto
    }
