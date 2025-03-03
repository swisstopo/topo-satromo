import pandas as pd
import geopandas as gpd
from fsspec.implementations.http import HTTPFileSystem
from datetime import datetime
import os

def get_vhi_for_date(date):
    """Lädt die VHI-Daten für ein bestimmtes Datum aus dem Geoparquet."""
    STAC_PATH = "https://sys-data.int.bgdi.ch/"
    SURFACE_TYPE = "forest"
    date_str = date.strftime('%Y-%m-%d')
    url = f"{STAC_PATH}ch.swisstopo.swisseo_vhi_v100/{date_str}t235959/ch.swisstopo.swisseo_vhi_v100_{date_str}t235959_{SURFACE_TYPE}-warnregions.parquet"

    try:
        filesystem = HTTPFileSystem()
        gdf = gpd.read_parquet(url, filesystem=filesystem)
        gdf = gdf.rename(columns={'REGION_NR': 'Region_ID', 'vhi_mean': 'VHI'})
        return gdf[['Region_ID', 'VHI']]
    except Exception as e:
        print(f"Keine VHI-Daten für {date_str}: {e}")
        return None

def merge_cdi_vhi(cdi_csv_path, output_csv_path):
    """Erstellt eine neue Datei mit CDI- und VHI-Daten."""
    # Lade CDI-Daten
    cdi_data = pd.read_csv(cdi_csv_path, sep=';', encoding="latin1")
    cdi_data['Datum'] = pd.to_datetime(cdi_data['Datum'])

    # Liste zur Speicherung der Ergebnisse
    merged_rows = []

    for date in cdi_data['Datum'].unique():
        vhi_data = get_vhi_for_date(date)
        if vhi_data is not None:
            date_cdi = cdi_data[cdi_data['Datum'] == date]
            merged_data = date_cdi.merge(vhi_data, on='Region_ID', how='inner')
            merged_rows.append(merged_data)

    # Alle Daten zusammenfügen
    if merged_rows:
        final_data = pd.concat(merged_rows, ignore_index=True)
        final_data.to_csv(output_csv_path, sep=';', encoding='latin1', index=False)
        print(f"Neue Datei gespeichert: {output_csv_path}")
    else:
        print("Keine Daten mit VHI-Werten gefunden.")

# Beispielaufruf
cdi_csv_path = r"C:\temp\temp\CDI_1991-01-01_2022-12-31_RegionaleHochwasserregionen.csv"
output_csv_path = r"C:\temp\temp\CDI_VHI_Merged.csv"
merge_cdi_vhi(cdi_csv_path, output_csv_path)
