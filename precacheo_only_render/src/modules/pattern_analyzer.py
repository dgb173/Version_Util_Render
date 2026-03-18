import pandas as pd
import os
import numpy as np

def get_match_outcome(row, team_of_interest):
    """Determines the outcome of a match for a specific team (Win, Draw, Loss)."""
    if pd.isna(row['home_goals']) or pd.isna(row['away_goals']):
        return "Desconocido"
        
    if row['home_team'] == team_of_interest:
        if row['home_goals'] > row['away_goals']:
            return "Victoria"
        elif row['home_goals'] < row['away_goals']:
            return "Derrota"
        else:
            return "Empate"
    elif row['away_team'] == team_of_interest:
        if row['away_goals'] > row['home_goals']:
            return "Victoria"
        elif row['away_goals'] < row['home_goals']:
            return "Derrota"
        else:
            return "Empate"
    return "No Involucrado"

def predict_by_analogy(home_team: str, away_team: str, future_handicap: float, db_path='data/historical_matches.csv'):
    """
    Predicts a match outcome by finding the most analogous past match.
    """
    print("--- Predicción por Analogía ---")
    print(f"Partido a analizar: {home_team} vs {away_team} (AH futuro: {future_handicap})")
    print()

    if not os.path.exists(db_path):
        print(f"Error: No se encuentra la base de datos en {db_path}")
        return

    df = pd.read_csv(db_path)

    # --- 1. Preparar datos ---
    # Convertir AH a numérico, ignorando errores (texto como '-')
    df['ah_numeric'] = pd.to_numeric(df['ah'], errors='coerce')
    # Rellenar NaNs en goles para evitar errores de cálculo
    df[['home_goals', 'away_goals']] = df[['home_goals', 'away_goals']].fillna(-1).astype(int)


    # --- 2. Buscar el mejor análogo ---
    # Filtrar partidos donde nuestro equipo local estuvo involucrado
    candidate_matches = df[(df['home_team'] == home_team) | (df['away_team'] == home_team)].copy()
    
    if candidate_matches.empty:
        print(f"No se encontraron partidos históricos para '{home_team}'.")
        return

    # Calcular 'similitud' para cada partido candidato
    # La similitud se basa en la diferencia de hándicap y si es un H2H.
    candidate_matches['handicap_diff'] = np.abs(candidate_matches['ah_numeric'] - future_handicap)
    candidate_matches['is_h2h'] = ((candidate_matches['home_team'] == home_team) & (candidate_matches['away_team'] == away_team)) | \
                                  ((candidate_matches['home_team'] == away_team) & (candidate_matches['away_team'] == home_team))
    
    # Priorizar H2H y luego la mínima diferencia de hándicap
    candidate_matches.sort_values(by=['is_h2h', 'handicap_diff'], ascending=[False, True], inplace=True)
    
    best_analogy = candidate_matches.iloc[0]

    # --- 3. Presentar el resultado ---
    print("Se ha encontrado el siguiente 'partido espejo' en el historial:")
    
    past_home_team = best_analogy['home_team']
    past_away_team = best_analogy['away_team']
    past_ah = best_analogy['ah_numeric']
    past_date = best_analogy['date']
    past_hg = best_analogy['home_goals']
    past_ag = best_analogy['away_goals']

    print(f"  - Fecha: {past_date}")
    print(f"  - Partido: {past_home_team} vs {past_away_team}")
    print(f"  - Hándicap: {past_ah} (Diferencia con el actual: {best_analogy['handicap_diff']:.2f})")
    if best_analogy['is_h2h']:
        print("  - Nota: ¡Era un enfrentamiento directo (H2H)!")

    # Determinar el resultado de aquel partido para nuestro equipo de interés
    outcome_of_analogy = get_match_outcome(best_analogy, home_team)
    
    print(f"\nEn aquel partido, el resultado para '{home_team}' fue: {outcome_of_analogy} ({past_hg}-{past_ag})")

    if outcome_of_analogy in ["Victoria", "Empate", "Derrota"]:
        print(f"\n>>> Predicción por Analogía: Basado en este caso de estudio, el resultado podría ser una {outcome_of_analogy.upper()} de '{home_team}'.")
    else:
        print("\nNo se puede generar una predicción clara con el partido encontrado.")


if __name__ == '__main__':
    # --- DATOS DE ENTRADA ---
    # Puedes cambiar estos valores para analizar cualquier partido
    partido_local = "Tokyo Verdy"
    partido_visitante = "Avispa Fukuoka"
    handicap_futuro = 0.0  # Ejemplo: un hándicap de 0.0

    predict_by_analogy(partido_local, partido_visitante, handicap_futuro)