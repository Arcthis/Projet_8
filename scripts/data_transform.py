import json
import pandas as pd
import os
import re

import datetime
from datetime import datetime, date, timedelta

def load_json_column_jsonl(file_path, source_name): 
    records = []
    current_date = date(2024, 10, 1)
    date_format = "%d-%m-%y"

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line in lines[1:]:
        row = json.loads(line)
        data = row.get("_airbyte_data", {})
        data["source_file"] = source_name

        time_str = data.get("Time")
        if time_str == "00:04:00" and len(records) > 0:
            current_date += timedelta(days=1)

        data["date"] = current_date.strftime(date_format)

        if source_name == "belgique.jsonl":
            data.update({
                "station_name": "WeerstationBS",
                "station_id": "IICHTE19",
                "latitude": 51.092,
                "longitude": 2.999,
                "elevation": 15
            })
        elif source_name == "france.jsonl":
            data.update({
                "station_name": "La Madeleine",
                "station_id": "ILAMAD25",
                "latitude": 50.659,
                "longitude": 3.07,
                "elevation": 23
            })

        records.append(data)

    return pd.DataFrame(records)

def load_structured_station_jsonl(file_path, source_name):
    all_rows = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            raw = json.loads(line)
            airbyte_data = raw.get("_airbyte_data", {})

            stations = {s["id"]: s for s in airbyte_data.get("stations", [])}
            hourly_data = airbyte_data.get("hourly", {})

            for station_id, raw_records in hourly_data.items():
                station_info = stations.get(station_id, {})
                for record in raw_records:
                    if isinstance(record, str):
                        try:
                            record = json.loads(record)
                        except json.JSONDecodeError:
                            continue
                    if not isinstance(record, dict):
                        continue

                    record_flat = record.copy()
                    record_flat["station_id"] = station_id
                    record_flat["station_name"] = station_info.get("name")
                    record_flat["latitude"] = station_info.get("latitude")
                    record_flat["longitude"] = station_info.get("longitude")
                    record_flat["elevation"] = station_info.get("elevation")
                    record_flat["source_file"] = source_name

                    dh_utc = record.get("dh_utc")
                    if dh_utc:
                        try:
                            dt_obj = datetime.strptime(dh_utc, "%Y-%m-%d %H:%M:%S")
                            record_flat["Time"] = dt_obj.strftime("%H:%M:%S")
                            record_flat["date"] = dt_obj.strftime("%d-%m-%y")
                        except ValueError:
                            pass
                    all_rows.append(record_flat)

    return pd.DataFrame(all_rows)

def clean_and_convert_units(df):
    def extract_number(value):
        if isinstance(value, str):
            value = value.replace(",", ".").replace('\xa0', ' ').strip()  # Standardiser
            match = re.search(r"[-+]?\d*\.\d+|\d+", value)
            if match:
                return float(match.group())
        elif isinstance(value, (int, float)):
            return float(value)
        return None

    def fahrenheit_to_celsius(f):
        return round((f - 32) * 5.0 / 9.0, 2)

    def inchesHg_to_hPa(inch):
        return round(inch * 33.066, 2)

    def mph_to_kmh(mph):
        return round(mph * 1.412, 2)

    if "Temperature" in df.columns:
        df["Temperature"] = df["Temperature"].apply(lambda x: fahrenheit_to_celsius(extract_number(x)) if x else None)

    if "Dew Point" in df.columns:
        df["Dew Point"] = df["Dew Point"].apply(lambda x: fahrenheit_to_celsius(extract_number(x)) if x else None)

    if "Pressure" in df.columns:
        df["Pressure"] = df["Pressure"].apply(lambda x: inchesHg_to_hPa(extract_number(x)) if x else None)

    if "Speed" in df.columns:
        df["Speed"] = df["Speed"].apply(lambda x: mph_to_kmh(extract_number(x)) if x else None)

    if "Gust" in df.columns:
        df["Gust"] = df["Gust"].apply(lambda x: mph_to_kmh(extract_number(x)) if x else None)

    if "Humidity" in df.columns:
        df["Humidity"] = df["Humidity"].apply(lambda x: extract_number(x))

    if "Precip. Rate." in df.columns:
        df["Precip. Rate."] = df["Precip. Rate."].apply(lambda x: round(extract_number(x) * 25.4, 3) if x else None)

    if "Precip. Accum." in df.columns:
        df["Precip. Accum."] = df["Precip. Accum."].apply(lambda x: round(extract_number(x) * 25.4, 3) if x else None)

    return df

def merge_similar_columns(df):
    merge_map = {
        "Humidity": "humidite",
        "Temperature": "temperature",
        "Dew Point": "point_de_rosee",
        "Pressure": "pression",
        "Wind": "vent_direction",
        "Speed": "vent_moyen",
        "Gust": "vent_rafales",
        "id_station": "station_id",
    }

    for old_col, new_col in merge_map.items():
        if old_col in df.columns:
            if new_col not in df.columns:
                df[new_col] = None
            df[new_col] = df[new_col].combine_first(df[old_col])
            df.drop(columns=old_col, inplace=True)

    return df

def remove_empty_columns(df):
    return df.dropna(axis=1, how='all')

def remove_high_null_columns(df, threshold=0.6):
    null_ratios = df.isnull().mean()
    cols_to_drop = null_ratios[null_ratios > threshold].index.tolist()
    df = df.drop(columns=cols_to_drop)
    return df, cols_to_drop

def export_station_info(df, output_path="../json/stations_info.json"):
    station_cols = ["station_id", "station_name", "latitude", "longitude"]
    station_df = df[station_cols].drop_duplicates().dropna(subset=["station_id"])
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    station_df.to_json(output_path, orient="records", force_ascii=False, indent=2)

def harmonize_and_export(file1, file2, file3, output_path="../json/merged_weather_data.json"):
    df1 = load_json_column_jsonl(file1, "belgique.jsonl")
    df1 = clean_and_convert_units(df1)
    df2 = load_json_column_jsonl(file2, "france.jsonl")
    df2 = clean_and_convert_units(df2)
    df3 = load_structured_station_jsonl(file3, "info_climat.jsonl")

    final_df = pd.concat([df1, df2, df3], ignore_index=True)
    final_df = merge_similar_columns(final_df)
    final_df = remove_empty_columns(final_df)

    # Supprimer les colonnes avec trop de nulls (> 70%)
    # final_df, dropped_columns = remove_high_null_columns(final_df, threshold=0.7)
    # print(f"Colonnes supprimées car trop de valeurs nulles (>70%) : {dropped_columns}")

    # Exporter les infos des stations
    export_station_info(final_df)

    # Supprimer les infos des stations du fichier principal (pour éviter les redondances)
    final_df = final_df.drop(columns=["station_name", "latitude", "longitude", "dh_utc"], errors='ignore')

    if 'source_file' in final_df.columns:
        final_df = final_df.drop(columns=['source_file'])

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    final_df.to_json("../json/merged_weather_data.json", orient='records', lines=False, force_ascii=False, indent=2)
    final_df.to_json("../json/merged_weather_data_to_check.json", orient='records', lines=True, force_ascii=False)

if __name__ == "__main__":
    harmonize_and_export(
        "../json/belgique.jsonl",
        "../json/france.jsonl",
        "../json/info_climat.jsonl"
    )
