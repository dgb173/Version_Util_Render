
# --- FUNCIONES DE SCRAPING DIRECTO PARA COLAB / BACKGROUND ---

async def scrape_main_page_matches_async_direct(limit=None, offset=0, handicap_filter=None, goal_line_filter=None, min_time=None):
    """
    Versión DEDICADA para scripts de fondo (Colab). Descarga la web fresca.
    NO USAR EN LA WEB (lento).
    """
    print("🌍 [DIRECT SCRAPE] Descargando página principal (Próximos)...")
    html = await _fetch_nowgoal_html() # path None = home
    if not html:
        print("❌ [DIRECT SCRAPE] Error: No se pudo descargar HTML.")
        return []

    print(f"✅ [DIRECT SCRAPE] HTML descargado ({len(html)} bytes). Parseando...")
    matches = parse_main_page_matches(
        html, 
        limit=limit, 
        offset=offset, 
        handicap_filter=handicap_filter, 
        goal_line_filter=goal_line_filter
    )
    
    if min_time:
        filtered = []
        for m in matches:
            t_str = m.get('start_time')
            if t_str and t_str != 'N/A':
                try:
                    # Parsear ISO string
                    # A veces vienen con UTC offset o 'T', nos aseguramos.
                    t_obj = datetime.datetime.fromisoformat(t_str)
                    
                    # Asegurar timezone
                    if t_obj.tzinfo is None:
                         # Asumimos que parse_main_page_matches devuelve UTC o timezone-naive comparable
                         pass

                    # Comparación simple
                    # El min_time suele ser timezone aware si usamos datetime.now(datetime.timezone.utc)
                    # Si t_obj es offset-naive y min_time es offset-aware, fallará.
                    # Asumimos que el parser devuelve algo consistente.
                    
                    # Hack simple: ignorar tz para comparar si da error, o convertir.
                    # Mejor: confiar en que el parser lo hace bien. 
                    
                    if t_obj.replace(tzinfo=None) >= min_time.replace(tzinfo=None):
                        filtered.append(m)
                except Exception as e:
                    # Si no podemos parsear la fecha, lo incluimos por duda o excluimos?
                    # Excluimos para no ensuciar.
                    pass
        matches = filtered

    print(f"✅ [DIRECT SCRAPE] Encontrados {len(matches)} partidos.")
    return matches

async def scrape_main_page_finished_matches_async_direct(limit=None, offset=0, handicap_filter=None, goal_line_filter=None):
    """
    Versión DEDICADA para scripts de fondo (Colab). Descarga la web fresca para terminados.
    """
    print("🌍 [DIRECT SCRAPE] Descargando página principal (Finalizados)...")
    html = await _fetch_nowgoal_html()
    if not html:
        print("❌ [DIRECT SCRAPE] Error: No se pudo descargar HTML.")
        return []

    print(f"✅ [DIRECT SCRAPE] HTML descargado. Parseando...")
    matches = parse_main_page_finished_matches(
        html,
        limit=limit,
        offset=offset,
        handicap_filter=handicap_filter,
        goal_line_filter=goal_line_filter
    )
    print(f"✅ [DIRECT SCRAPE] Encontrados {len(matches)} partidos terminados.")
    return matches
