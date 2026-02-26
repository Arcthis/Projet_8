import json
import pandas as pd
import sys
import re

def load_jsonl(file_path):
    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                row = json.loads(line)
                data = row.get("_airbyte_data", row)

                if "hourly" in data and "stations" in data:
                    stations_dict = {s["id"]: s for s in data.get("stations", [])}
                    hourly = data.get("hourly", {})
                    for station_id, mesures in hourly.items():
                        station_info = stations_dict.get(station_id, {})
                        for mesure in mesures:
                            if isinstance(mesure, str):
                                try:
                                    mesure = json.loads(mesure)
                                except json.JSONDecodeError:
                                    continue
                            if isinstance(mesure, dict):
                                mesure["station_id"] = station_id
                                mesure["station_name"] = station_info.get("name")
                                mesure["latitude"] = station_info.get("latitude")
                                mesure["longitude"] = station_info.get("longitude")
                                mesure["elevation"] = station_info.get("elevation")
                                records.append(mesure)
                else:
                    records.append(data)

            except json.JSONDecodeError:
                print(f"Ligne illisible dans {file_path}")
    return pd.DataFrame(records)

def check_nulls(df, threshold=0.7):
    null_ratios = df.isnull().mean()
    print("\nTaux de valeurs nulles par colonne :")
    print(null_ratios.sort_values(ascending=False))

    too_empty = null_ratios[null_ratios > threshold]
    if not too_empty.empty:
        print(f"\nColonnes avec plus de {int(threshold*100)}% de valeurs nulles :")
        print(too_empty)
    else:
        print("Aucune colonne avec trop de valeurs nulles.")

def check_types(df):
    print("\nVérification des types de données :")
    print(df.dtypes)
    print("Types de données listés ci-dessus.")

def check_station_fields(df):
    required_fields = ["station_id", "station_name", "latitude", "longitude"]
    print("\nVérification de la présence de champs obligatoires :")
    missing = False
    for field in required_fields:
        if field not in df.columns:
            print(f" Champ manquant : {field}")
            missing = True
        elif df[field].isnull().all():
            print(f"Champ {field} présent mais complètement vide")
            missing = True
    if not missing:
        print("Tous les champs obligatoires sont présents et contiennent des données.")

def check_lat_lon_ranges(df):
    print("\nVérification des plages géographiques :")
    ok = True
    if "latitude" in df.columns:
        invalid_lat = df[~df["latitude"].between(-90, 90, inclusive="both")]
        if not invalid_lat.empty:
            print(f"Lignes avec latitude hors bornes (-90 à 90) : {len(invalid_lat)}")
            ok = False
    if "longitude" in df.columns:
        invalid_lon = df[~df["longitude"].between(-180, 180, inclusive="both")]
        if not invalid_lon.empty:
            print(f"Lignes avec longitude hors bornes (-180 à 180) : {len(invalid_lon)}")
            ok = False
    if ok:
        print("Toutes les coordonnées sont dans des plages valides.")

def check_station_duplicates(df):
    print("\nVérification des doublons de station :")
    cols = ["station_id", "station_name", "latitude", "longitude"]

    if not set(cols).issubset(df.columns):
        print("Colonnes nécessaires manquantes pour vérifier les doublons.")
        return

    duplicates_mask = df.duplicated(subset=cols, keep=False)
    n_duplicates = duplicates_mask.sum()

    if n_duplicates == 0:
        print("Aucun doublon exact sur les stations.")
    else:
        print(f"{n_duplicates} lignes concernées par des doublons potentiels.")

    print("\nStations uniques (comme après un drop_duplicates) :")
    print(df[cols].drop_duplicates().head(5).to_string(index=False))

def extract_temperature_number(val):
    if pd.isnull(val):
        return None
    if isinstance(val, (int, float)):
        return val
    match = re.search(r"-?\d+(\.\d+)?", str(val))
    if match:
        return float(match.group())
    return None

def is_valid_time(val):
    if pd.isnull(val):
        return True
    if isinstance(val, str):
        return bool(re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d", val.strip()))
    return False

def check_data_quality(df):
    print("\nVérification de la qualité des données :")

    # --- Temperature ---
    if "Temperature" in df.columns:
        non_null_temps = df[~df["Temperature"].isnull()].copy()
        non_null_temps["__temp_num__"] = non_null_temps["Temperature"].apply(extract_temperature_number)
        invalid_temps = non_null_temps[non_null_temps["__temp_num__"].isnull()]

        if invalid_temps.empty:
            print("Toutes les températures non nulles sont valides.")
        else:
            print(f"{len(invalid_temps)} ligne(s) avec température non exploitable :")
            print(invalid_temps.drop(columns="__temp_num__").to_string(index=False))
    else:
        print("Colonne 'Temperature' absente.")

    # --- Time ---
    if "Time" in df.columns:
        invalid_times = df[~df["Time"].apply(is_valid_time) & ~df["Time"].isnull()]
        if invalid_times.empty:
            print("Toutes les valeurs non nulles du champ 'Time' sont au format HH:MM:SS.")
        else:
            print(f"{len(invalid_times)} ligne(s) avec heure non exploitable :")
            print(invalid_times.to_string(index=False))
    else:
        print("Colonne 'Time' absente.")

def main(files):
    for file in files:
        print(f"\nAnalyse du fichier : {file}")
        df = load_jsonl(file)
        if df.empty:
            print("Fichier vide ou non lisible.")
            continue
        check_station_fields(df)
        check_types(df)
        check_nulls(df)
        check_lat_lon_ranges(df)
        check_station_duplicates(df)
        check_data_quality(df)

if __name__ == "__main__":
    # Exemple : python check_data_integrity.py ../json/belgique.jsonl ../json/france.jsonl ../json/info_climat.jsonl
    if len(sys.argv) < 2:
        print("Usage: python check_data_integrity.py <fichier1.jsonl> <fichier2.jsonl> ...")
    else:
        main(sys.argv[1:])
