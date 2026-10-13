
import os
import logging
from datetime import timedelta

import requests
import pandas as pd
import pendulum

from airflow.sdk import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook


# ============================================================
# 1. KONFIGURASI
# ============================================================

DAG_ID = "etl_openaq"

BASE_URL = "https://api.openaq.org/v3"
LOCATION_ID = 6144741

LATITUDE = -7.9185093
LONGITUDE = 112.651344

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

MYSQL_CONN_ID = "mysql_malang"
TABLE_NAME = "air_quality"

API_LIMIT = 1000
LOCAL_TIMEZONE = "Asia/Jakarta"

logger = logging.getLogger(__name__)


# ============================================================
# 2. FUNGSI REQUEST OPENAQ
# ============================================================

def openaq_get(endpoint, params=None):
    if not OPENAQ_API_KEY:
        raise ValueError(
            "OPENAQ_API_KEY belum tersedia di environment Airflow."
        )

    url = f"{BASE_URL}{endpoint}"

    response = requests.get(
        url,
        headers={
            "X-API-Key": OPENAQ_API_KEY,
            "Accept": "application/json",
        },
        params=params or {},
        timeout=60,
    )

    response.raise_for_status()
    return response.json()


# ============================================================
# 3. DEFINISI DAG
# ============================================================

@dag(
    dag_id=DAG_ID,
    description="ETL OpenAQ PM2.5 Malang ke MySQL",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz=LOCAL_TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["openaq", "pm25", "malang", "etl"],
)
def etl_openaq():

    # ========================================================
    # TASK 1: MENCARI SENSOR PM2.5 SECARA OTOMATIS
    # ========================================================

    @task
    def extract_data():

        location_data = openaq_get(
            f"/locations/{LOCATION_ID}/sensors",
            params={"limit": 1000, "page": 1},
        )

        sensors = location_data.get("results", [])

        if not sensors:
            raise ValueError(
                f"Tidak ditemukan sensor untuk location_id {LOCATION_ID}."
            )

        # Cari sensor yang parameternya PM2.5
        pm25_sensors = []

        for sensor in sensors:
            parameter = sensor.get("parameter") or {}

            parameter_name = str(
                parameter.get("name", "")
            ).strip().lower()

            sensor_name = str(
                sensor.get("name", "")
            ).strip().lower()

            if parameter_name in ("pm25", "pm2.5", "pm2_5") or \
               sensor_name in ("pm25", "pm2.5", "pm2_5"):
                pm25_sensors.append(sensor)

        if not pm25_sensors:
            available = [
                {
                    "id": sensor.get("id"),
                    "name": sensor.get("name"),
                    "parameter": (sensor.get("parameter") or {}).get("name"),
                }
                for sensor in sensors
            ]

            raise ValueError(
                "Sensor PM2.5 tidak ditemukan pada lokasi tersebut. "
                f"Sensor yang tersedia: {available}"
            )

        logger.info(
            "Sensor PM2.5 ditemukan: %s",
            [
                {
                    "id": sensor.get("id"),
                    "name": sensor.get("name"),
                }
                for sensor in pm25_sensors
            ],
        )

        records = []

        # Ambil pengukuran dari setiap sensor PM2.5
        for sensor in pm25_sensors:
            sensor_id = sensor.get("id")

            if not sensor_id:
                continue

            sensor_name = sensor.get("name") or "pm25"
            parameter = sensor.get("parameter") or {}
            parameter_name = parameter.get("name") or "pm25"
            unit = parameter.get("units")

            page = 1

            while True:
                result = openaq_get(
                    f"/sensors/{sensor_id}/measurements",
                    params={
                        "limit": API_LIMIT,
                        "page": page,
                    },
                )

                measurements = result.get("results", [])

                if not measurements:
                    break

                for item in measurements:
                    datetime_obj = item.get("datetime") or {}
                    coordinates = item.get("coordinates") or {}

                    datetime_utc = datetime_obj.get("utc")

                    if not datetime_utc:
                        continue

                    records.append({
                        "location_id": LOCATION_ID,
                        "latitude": (
                            coordinates.get("latitude")
                            if coordinates.get("latitude") is not None
                            else LATITUDE
                        ),
                        "longitude": (
                            coordinates.get("longitude")
                            if coordinates.get("longitude") is not None
                            else LONGITUDE
                        ),
                        "sensor_id": sensor_id,
                        "sensor_name": sensor_name,
                        "parameter": parameter_name,
                        "unit": item.get("unit") or unit,
                        "value": item.get("value"),
                        "datetime_utc": datetime_utc,
                    })

                if len(measurements) < API_LIMIT:
                    break

                page += 1

        if not records:
            logger.warning("API tidak mengembalikan data pengukuran PM2.5.")
            return []

        logger.info("Jumlah data hasil ekstraksi: %s", len(records))
        return records

    # ========================================================
    # TASK 2: TRANSFORMASI DATA
    # ========================================================

    @task
    def transform_data(records):

        if not records:
            return []

        df = pd.DataFrame(records)

        # Hanya 10 kolom yang tersedia pada tabel MySQL.
        # id AUTO_INCREMENT dan display_name tidak digunakan.
        columns = [
            "location_id",
            "latitude",
            "longitude",
            "sensor_id",
            "sensor_name",
            "parameter",
            "unit",
            "value",
            "datetime_utc",
            "datetime_local",
        ]

        # Buat datetime UTC
        df["datetime_utc"] = pd.to_datetime(
            df["datetime_utc"],
            errors="coerce",
            utc=True,
        )

        df = df.dropna(
            subset=[
                "location_id",
                "sensor_id",
                "datetime_utc",
            ]
        )

        # Hitung waktu lokal Malang (UTC+7)
        df["datetime_local"] = (
            df["datetime_utc"]
            .dt.tz_convert(LOCAL_TIMEZONE)
        )

        # MySQL DATETIME menyimpan waktu tanpa info zona waktu
        df["datetime_utc"] = (
            df["datetime_utc"]
            .dt.tz_localize(None)
        )

        df["datetime_local"] = (
            df["datetime_local"]
            .dt.tz_localize(None)
        )

        # Konversi tipe numerik
        for col in [
            "location_id",
            "sensor_id",
            "latitude",
            "longitude",
            "value",
        ]:
            df[col] = pd.to_numeric(
                df[col],
                errors="coerce",
            )

        df = df.dropna(
            subset=[
                "location_id",
                "sensor_id",
                "datetime_utc",
            ]
        )

        df = df[columns]

        # Hilangkan duplikat dalam batch berdasarkan sensor dan waktu
        df = df.drop_duplicates(
            subset=[
                "location_id",
                "sensor_id",
                "datetime_utc",
            ]
        )

        # Ganti NaN/NaT dengan None untuk MySQL
        df = df.astype(object).where(pd.notna(df), None)

        rows = list(
            df.itertuples(index=False, name=None)
        )

        # Pastikan setiap baris memiliki tepat 10 nilai
        for index, row in enumerate(rows):
            if len(row) != 10:
                raise ValueError(
                    f"Baris {index} berisi {len(row)} nilai, "
                    "seharusnya 10."
                )

        logger.info(
            "Jumlah data setelah transformasi: %s",
            len(rows),
        )

        return rows

    # ========================================================
    # TASK 3: LOAD DATA KE MYSQL
    # ========================================================

    @task
    def load_data(rows):

        if not rows:
            logger.info("Tidak ada data untuk disimpan.")
            return

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = """
            INSERT INTO air_quality (
                location_id,
                latitude,
                longitude,
                sensor_id,
                sensor_name,
                parameter,
                unit,
                value,
                datetime_utc,
                datetime_local
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                sensor_name = VALUES(sensor_name),
                parameter = VALUES(parameter),
                unit = VALUES(unit),
                value = VALUES(value),
                datetime_local = VALUES(datetime_local)
        """

        try:
            cursor.executemany(sql, rows)
            conn.commit()

            logger.info(
                "Berhasil memproses %s baris ke tabel %s.",
                len(rows),
                TABLE_NAME,
            )

        except Exception:
            conn.rollback()
            logger.exception("Gagal menyimpan data ke MySQL.")
            raise

        finally:
            cursor.close()
            conn.close()

    # ========================================================
    # ALUR ETL
    # ========================================================

    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)


etl_openaq()