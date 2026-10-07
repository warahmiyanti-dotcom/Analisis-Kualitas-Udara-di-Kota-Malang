from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import mysql.connector
import pandas as pd
import numpy as np

from math import radians, sin, cos, sqrt, atan2


# ============================================================
# KONFIGURASI
# ============================================================

DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 3306,
    "user": "admin",
    "password": "admin",
    "database": "airflow_malang"
}

LAT_AQ = -7.9185093
LON_AQ = 112.651344

RADIUS_KM = 10

# Data integrasi dimulai dari 7 Agustus 2026
START_DATE = "2026-08-07"


# ============================================================
# HAVERSINE
# ============================================================

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        sin(dlat / 2) ** 2
        + cos(lat1)
        * cos(lat2)
        * sin(dlon / 2) ** 2
    )

    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


# ============================================================
# FUNGSI UTAMA INTEGRASI
# ============================================================

def integrate_malang():

    print("=" * 70)
    print("INTEGRASI DATA MALANG")
    print("=" * 70)

    # --------------------------------------------------------
    # Koneksi MySQL
    # --------------------------------------------------------

    db = mysql.connector.connect(**DB_CONFIG)

    print("✓ Berhasil terhubung ke MySQL")

    # --------------------------------------------------------
    # Baca OpenAQ
    # --------------------------------------------------------

    query_openaq = f"""
        SELECT
            datetime_local,
            value
        FROM openaq_data
        WHERE parameter = 'pm25'
          AND datetime_utc >= '{START_DATE} 00:00:00'
        ORDER BY datetime_utc
    """

    openaq = pd.read_sql(query_openaq, db)

    print(f"✓ OpenAQ: {len(openaq)} baris")


    # --------------------------------------------------------
    # Baca Open-Meteo
    # --------------------------------------------------------

    query_openmeteo = f"""
        SELECT
            datetime_local,
            temperature,
            humidity,
            precipitation,
            pressure,
            wind_speed,
            wind_direction,
            cloud_cover
        FROM openmeteo_data
        WHERE datetime_utc >= '{START_DATE} 00:00:00'
        ORDER BY datetime_utc
    """

    openmeteo = pd.read_sql(query_openmeteo, db)

    print(f"✓ Open-Meteo: {len(openmeteo)} baris")


    # --------------------------------------------------------
    # Baca FIRMS
    # --------------------------------------------------------

    query_firms = f"""
        SELECT
            latitude,
            longitude,
            datetime_utc,
            datetime_local,
            frp
        FROM firms_data
        WHERE datetime_utc >= '{START_DATE} 00:00:00'
        ORDER BY datetime_utc
    """

    firms = pd.read_sql(query_firms, db)

    print(f"✓ FIRMS: {len(firms)} baris")


    # ========================================================
    # OPENAQ
    # ========================================================

    openaq["datetime_local"] = pd.to_datetime(
        openaq["datetime_local"]
    )

    openaq["tanggal"] = openaq["datetime_local"].dt.date

    openaq_feature = (
        openaq
        .groupby("tanggal", as_index=False)
        ["value"]
        .mean()
        .rename(columns={"value": "PM2_5"})
    )

    print(
        f"✓ OpenAQ harian: "
        f"{len(openaq_feature)} hari"
    )


    # ========================================================
    # OPEN-METEO
    # ========================================================

    openmeteo["datetime_local"] = pd.to_datetime(
        openmeteo["datetime_local"]
    )

    openmeteo["tanggal"] = (
        openmeteo["datetime_local"].dt.date
    )

    openmeteo_feature = (
        openmeteo
        .groupby("tanggal")
        .agg(
            temperature_mean=("temperature", "mean"),
            humidity_mean=("humidity", "mean"),
            precipitation_sum=("precipitation", "sum"),
            pressure_mean=("pressure", "mean"),
            wind_speed_mean=("wind_speed", "mean"),
            wind_direction_mean=("wind_direction", "mean"),
            cloud_cover_mean=("cloud_cover", "mean")
        )
        .reset_index()
    )

    print(
        f"✓ Open-Meteo harian: "
        f"{len(openmeteo_feature)} hari"
    )


    # ========================================================
    # FIRMS
    # ========================================================

    if len(firms) > 0:

        # Hitung jarak setiap hotspot
        firms["jarak_km"] = firms.apply(
            lambda row: haversine(
                LAT_AQ,
                LON_AQ,
                row["latitude"],
                row["longitude"]
            ),
            axis=1
        )

        # Filter radius 10 km
        firms_10km = firms[
            firms["jarak_km"] <= RADIUS_KM
        ].copy()

        print(
            f"✓ Hotspot sebelum filter: "
            f"{len(firms)}"
        )

        print(
            f"✓ Hotspot <= {RADIUS_KM} km: "
            f"{len(firms_10km)}"
        )

        if len(firms_10km) > 0:

            firms_10km["datetime_local"] = pd.to_datetime(
                firms_10km["datetime_local"]
            )

            firms_10km["tanggal"] = (
                firms_10km["datetime_local"].dt.date
            )

            firms_feature = (
                firms_10km
                .groupby("tanggal")
                .agg(
                    hotspot_count=("frp", "count"),
                    total_frp=("frp", "sum"),
                    max_frp=("frp", "max")
                )
                .reset_index()
            )

        else:

            firms_feature = pd.DataFrame(
                columns=[
                    "tanggal",
                    "hotspot_count",
                    "total_frp",
                    "max_frp"
                ]
            )

    else:

        print("✓ Tidak ada data FIRMS")

        firms_feature = pd.DataFrame(
            columns=[
                "tanggal",
                "hotspot_count",
                "total_frp",
                "max_frp"
            ]
        )


    # ========================================================
    # MERGE OPENAQ + OPEN-METEO
    # ========================================================

    integrated = pd.merge(
        openaq_feature,
        openmeteo_feature,
        on="tanggal",
        how="left"
    )


    # ========================================================
    # MERGE FIRMS
    # ========================================================

    integrated = pd.merge(
        integrated,
        firms_feature,
        on="tanggal",
        how="left"
    )


    # ========================================================
    # HOTSPOT KOSONG = 0
    # ========================================================

    hotspot_columns = [
        "hotspot_count",
        "total_frp",
        "max_frp"
    ]

    for col in hotspot_columns:
        integrated[col] = integrated[col].fillna(0)


    # Pastikan tipe data
    integrated["hotspot_count"] = (
        integrated["hotspot_count"].astype(int)
    )


    # ========================================================
    # URUTKAN
    # ========================================================

    integrated = integrated.sort_values(
        "tanggal"
    ).reset_index(drop=True)


    # ========================================================
    # VALIDASI
    # ========================================================

    print("\n" + "=" * 70)
    print("HASIL INTEGRASI")
    print("=" * 70)

    print(
        f"Jumlah baris: {len(integrated)}"
    )

    print(
        f"Periode: "
        f"{integrated['tanggal'].min()} "
        f"sampai "
        f"{integrated['tanggal'].max()}"
    )

    print("\nKolom:")
    print(integrated.columns.tolist())


    # ========================================================
    # HAPUS DATA INTEGRASI LAMA
    # ========================================================

    cursor = db.cursor()

    cursor.execute(
        f"""
        DELETE FROM integrated_data
        WHERE tanggal >= '{START_DATE}'
        """
    )

    db.commit()

    print(
        "✓ Data integrated_data lama "
        "berhasil dibersihkan"
    )


    # ========================================================
    # INSERT DATA TERBARU
    # ========================================================

    insert_query = """
        INSERT INTO integrated_data (
            tanggal,
            PM2_5,
            temperature_mean,
            humidity_mean,
            precipitation_sum,
            pressure_mean,
            wind_speed_mean,
            wind_direction_mean,
            cloud_cover_mean,
            hotspot_count,
            total_frp,
            max_frp
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
    """

    data_insert = []

    for _, row in integrated.iterrows():

        data_insert.append((
            row["tanggal"],
            row["PM2_5"],
            row["temperature_mean"],
            row["humidity_mean"],
            row["precipitation_sum"],
            row["pressure_mean"],
            row["wind_speed_mean"],
            row["wind_direction_mean"],
            row["cloud_cover_mean"],
            row["hotspot_count"],
            row["total_frp"],
            row["max_frp"]
        ))

    cursor.executemany(
        insert_query,
        data_insert
    )

    db.commit()

    print(
        f"✓ {len(data_insert)} baris "
        "berhasil dimasukkan ke integrated_data"
    )


    # ========================================================
    # TUTUP KONEKSI
    # ========================================================

    cursor.close()
    db.close()

    print("\n" + "=" * 70)
    print("INTEGRASI SELESAI")
    print("=" * 70)


# ============================================================
# AIRFLOW DAG
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="integrate_malang",
    default_args=default_args,
    description="Integrasi OpenAQ, Open-Meteo dan NASA FIRMS untuk dashboard Malang",
    start_date=datetime(2026, 8, 7),
    schedule="@daily",
    catchup=False,
    tags=["malang", "integration", "dashboard"]
) as dag:

    integrate_task = PythonOperator(
        task_id="integrate_data",
        python_callable=integrate_malang
    )