from datetime import datetime, timedelta, timezone

import requests
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


# ============================================================
# KONFIGURASI
# ============================================================

DAG_ID = "etl_openmeteo_mysql"
MYSQL_CONN_ID = "mysql_malang"
TABLE_NAME = "weather"

LOCATION_ID = 6144741
LATITUDE = -7.9185093
LONGITUDE = 112.651344

API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Tanggal awal pengambilan data historis
START_DATE = datetime(2026, 8, 1).date()


# ============================================================
# PROSES EXTRACT, TRANSFORM, LOAD
# ============================================================

def etl_openmeteo():

    # --------------------------------------------------------
    # 1. EXTRACT: ambil data sejak 1 Agustus 2026
    # sampai kemarin, menggunakan waktu UTC
    # --------------------------------------------------------

    today_utc = datetime.now(timezone.utc).date()
    end_date = today_utc - timedelta(days=1)
    start_date = START_DATE

    if end_date < start_date:
        raise ValueError(
            f"Tanggal akhir {end_date} lebih awal daripada "
            f"tanggal awal {start_date}."
        )

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation,"
            "pressure_msl,"
            "wind_speed_10m,"
            "wind_direction_10m,"
            "cloud_cover"
        ),
        "timezone": "UTC",
        "wind_speed_unit": "kmh"
    }

    print("Memulai pengambilan data Open-Meteo...")
    print(f"Lokasi: Malang ({LATITUDE}, {LONGITUDE})")
    print(f"Tanggal awal: {start_date}")
    print(f"Tanggal akhir: {end_date}")

    response = requests.get(
        API_URL,
        params=params,
        timeout=120
    )

    response.raise_for_status()
    data = response.json()

    hourly = data.get("hourly", {})
    times = hourly.get("time", [])

    if not times:
        raise ValueError(
            "Open-Meteo tidak mengembalikan data cuaca."
        )

    # --------------------------------------------------------
    # 2. TRANSFORM: sesuaikan data dengan tabel weather
    # --------------------------------------------------------

    required_columns = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "pressure_msl",
        "wind_speed_10m",
        "wind_direction_10m",
        "cloud_cover"
    ]

    for column in required_columns:
        if column not in hourly:
            raise ValueError(
                f"Kolom {column} tidak tersedia dari Open-Meteo."
            )

        if len(hourly[column]) != len(times):
            raise ValueError(
                f"Jumlah data {column} tidak sesuai dengan waktu."
            )

    rows = []

    for i, timestamp in enumerate(times):

        datetime_utc = datetime.strptime(
            timestamp,
            "%Y-%m-%dT%H:%M"
        )

        row = (
            LOCATION_ID,
            LATITUDE,
            LONGITUDE,
            datetime_utc,
            hourly["temperature_2m"][i],
            hourly["relative_humidity_2m"][i],
            hourly["precipitation"][i],
            hourly["pressure_msl"][i],
            hourly["wind_speed_10m"][i],
            hourly["wind_direction_10m"][i],
            hourly["cloud_cover"][i]
        )

        rows.append(row)

    if not rows:
        raise ValueError("Tidak ada baris data yang dapat diproses.")

    # --------------------------------------------------------
    # 3. LOAD: simpan ke MySQL
    # Menggunakan unique key lokasi dan waktu.
    # Data yang sudah ada diperbarui, bukan diduplikasi.
    # --------------------------------------------------------

    mysql_hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    connection = mysql_hook.get_conn()

    sql = f"""
        INSERT INTO {TABLE_NAME} (
            location_id,
            latitude,
            longitude,
            datetime_utc,
            temperature_c,
            humidity_percent,
            precipitation_mm,
            pressure_hpa,
            wind_speed_kmh,
            wind_direction_degree,
            cloud_cover_percent
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            temperature_c = VALUES(temperature_c),
            humidity_percent = VALUES(humidity_percent),
            precipitation_mm = VALUES(precipitation_mm),
            pressure_hpa = VALUES(pressure_hpa),
            wind_speed_kmh = VALUES(wind_speed_kmh),
            wind_direction_degree = VALUES(wind_direction_degree),
            cloud_cover_percent = VALUES(cloud_cover_percent)
    """

    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    # --------------------------------------------------------
    # 4. LAPORAN HASIL ETL
    # --------------------------------------------------------

    print("=" * 60)
    print("ETL OPEN-METEO BERHASIL")
    print("=" * 60)
    print(f"Lokasi ID       : {LOCATION_ID}")
    print(f"Tanggal awal    : {start_date}")
    print(f"Tanggal akhir   : {end_date}")
    print(f"Jumlah baris API: {len(rows)}")
    print(f"Tabel tujuan    : {TABLE_NAME}")
    print("Data berhasil dimuat atau diperbarui di MySQL.")
    print("=" * 60)


# ============================================================
# DEFINISI DAG AIRFLOW
# ============================================================

with DAG(
    dag_id=DAG_ID,
    description=(
        "ETL data cuaca historis Malang dari Open-Meteo "
        "ke MySQL, mulai 1 Agustus 2026"
    ),
    start_date=pendulum.datetime(
        2026, 8, 1, tz="UTC"
    ),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "ETL",
        "Open-Meteo",
        "Weather",
        "Malang"
    ]
) as dag:

    extract_transform_load = PythonOperator(
        task_id="extract_transform_load_weather",
        python_callable=etl_openmeteo
    )

    extract_transform_load

