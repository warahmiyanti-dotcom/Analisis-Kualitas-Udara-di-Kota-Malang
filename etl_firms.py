
import os
import time
from io import StringIO
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import pendulum

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook


# ============================================================
# 1. KONFIGURASI
# ============================================================

MAP_KEY = os.getenv("NASA_FIRMS_MAP_KEY")

SOURCE = "VIIRS_NOAA21_NRT"

# Bounding box Jawa Timur dan sekitarnya:
# west, south, east, north
AREA = "110.90,-8.80,116.40,-5.00"

# Lokasi referensi Malang
LOCATION_ID = 6144741
LATITUDE = -7.9185093
LONGITUDE = 112.651344

MYSQL_CONN_ID = "mysql_malang"
TABLE_NAME = "fire_hotspots"

# Periode data yang ingin diambil
START_DATE = "2026-08-01"
END_DATE = "2026-10-09"

# API Area NASA FIRMS: maksimal 5 hari per permintaan
DAY_RANGE = 5

BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"


# ============================================================
# 2. MEMBUAT TABEL MYSQL
# ============================================================

def create_table(mysql_hook):
    sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            location_id BIGINT NOT NULL,
            reference_latitude DOUBLE NOT NULL,
            reference_longitude DOUBLE NOT NULL,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            brightness DOUBLE NULL,
            scan DOUBLE NULL,
            track DOUBLE NULL,
            acq_date DATE NOT NULL,
            acq_time VARCHAR(4) NOT NULL,
            datetime_utc DATETIME NOT NULL,
            datetime_local DATETIME NOT NULL,
            satellite VARCHAR(20) NULL,
            instrument VARCHAR(30) NULL,
            confidence VARCHAR(30) NULL,
            version VARCHAR(30) NULL,
            bright_ti4 DOUBLE NULL,
            bright_ti5 DOUBLE NULL,
            frp DOUBLE NULL,
            daynight VARCHAR(5) NULL,
            source VARCHAR(40) NOT NULL,
            UNIQUE KEY unique_hotspot (
                latitude,
                longitude,
                acq_date,
                acq_time,
                satellite,
                instrument
            ),
            INDEX idx_acq_date (acq_date),
            INDEX idx_datetime_utc (datetime_utc)
        )
    """
    mysql_hook.run(sql)


# ============================================================
# 3. EXTRACT: MENGAMBIL DATA NASA FIRMS
# ============================================================

def download_firms():
    if not MAP_KEY:
        raise ValueError(
            "NASA_FIRMS_MAP_KEY belum tersedia di environment Airflow."
        )

    start = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end = datetime.strptime(END_DATE, "%Y-%m-%d").date()

    if start > end:
        raise ValueError("START_DATE tidak boleh melewati END_DATE.")

    all_data = []
    current_date = start

    session = requests.Session()

    try:
        while current_date <= end:
            remaining_days = (end - current_date).days + 1
            day_range = min(DAY_RANGE, remaining_days)

            request_end = current_date + timedelta(days=day_range - 1)

            # Format API:
            # /area/csv/{MAP_KEY}/{SOURCE}/{AREA}/{DAY_RANGE}/{DATE}
            url = (
                f"{BASE_URL}/{MAP_KEY}/{SOURCE}/"
                f"{AREA}/{day_range}/{current_date.isoformat()}"
            )

            print(
                f"Mengambil data dari {current_date} "
                f"sampai {request_end}"
            )

            response = session.get(url, timeout=120)
            response.raise_for_status()

            if response.text.strip():
                try:
                    df_part = pd.read_csv(StringIO(response.text))

                    if not df_part.empty and "latitude" in df_part.columns:
                        all_data.append(df_part)
                    else:
                        print("Tidak ada baris hotspot pada rentang ini.")

                except pd.errors.EmptyDataError:
                    print("Respons CSV kosong pada rentang ini.")

            current_date += timedelta(days=day_range)
            time.sleep(0.2)

    finally:
        session.close()

    if not all_data:
        print("Tidak ada data NASA FIRMS yang berhasil diambil.")
        return pd.DataFrame()

    df = pd.concat(all_data, ignore_index=True)

    # Memastikan tanggal tersedia dan valid
    if "acq_date" not in df.columns:
        raise ValueError("Kolom acq_date tidak ditemukan pada respons NASA FIRMS.")

    df["acq_date"] = pd.to_datetime(
        df["acq_date"], errors="coerce"
    ).dt.date

    df = df[df["acq_date"].notna()].copy()
    df = df[df["acq_date"].between(start, end)].copy()

    # Menghapus duplikat akibat permintaan API yang berdekatan
    subset = [
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
        "satellite",
        "instrument",
    ]

    missing_columns = [col for col in subset if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Kolom identifikasi hotspot tidak tersedia: {missing_columns}"
        )

    df = df.drop_duplicates(subset=subset).copy()
    df["source"] = SOURCE

    print("\n=== HASIL EXTRACT NASA FIRMS ===")
    print("Total data:", len(df))

    if not df.empty:
        print("Tanggal awal:", df["acq_date"].min())
        print("Tanggal akhir:", df["acq_date"].max())

    return df


# ============================================================
# 4. TRANSFORM: MEMBERSIHKAN DATA DAN MEMBUAT WAKTU
# ============================================================

def transform_data(df):
    if df.empty:
        return []

    rows = []

    for _, item in df.iterrows():
        try:
            latitude = float(item["latitude"])
            longitude = float(item["longitude"])

            acq_date = item["acq_date"].strftime("%Y-%m-%d")

            # Waktu akuisisi dalam format HHMM
            raw_time = str(int(float(item["acq_time"]))).zfill(4)

            if len(raw_time) != 4:
                raise ValueError("Format acq_time tidak valid.")

            hour = int(raw_time[:2])
            minute = int(raw_time[2:])

            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("Jam atau menit di luar rentang yang valid.")

            # Waktu akuisisi NASA FIRMS dianggap UTC
            dt_utc = datetime.strptime(
                f"{acq_date} {hour:02d}:{minute:02d}:00",
                "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=timezone.utc)

            # Waktu lokal Malang: UTC+7
            dt_local = dt_utc.astimezone(
                timezone(timedelta(hours=7))
            )

            def get_number(column):
                value = item.get(column)

                if value is None or pd.isna(value):
                    return None

                return float(value)

            def get_text(column):
                value = item.get(column)

                if value is None or pd.isna(value):
                    return None

                return str(value)

            rows.append((
                LOCATION_ID,
                LATITUDE,
                LONGITUDE,
                latitude,
                longitude,
                get_number("brightness"),
                get_number("scan"),
                get_number("track"),
                acq_date,
                raw_time,
                dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
                dt_local.strftime("%Y-%m-%d %H:%M:%S"),
                get_text("satellite"),
                get_text("instrument"),
                get_text("confidence"),
                get_text("version"),
                get_number("bright_ti4"),
                get_number("bright_ti5"),
                get_number("frp"),
                get_text("daynight"),
                SOURCE
            ))

        except (ValueError, TypeError, KeyError) as exc:
            print(f"Baris dilewati karena format tidak valid: {exc}")

    return rows


# ============================================================
# 5. DAG ETL NASA FIRMS
# ============================================================

@dag(
    dag_id="etl_firms_mysql",
    description="ETL historis hotspot NASA FIRMS wilayah Jawa Timur",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ETL", "NASA FIRMS", "Hotspot", "Jawa Timur"]
)
def etl_firms_mysql():

    @task
    def extract_transform_load():

        # 1. EXTRACT
        df = download_firms()

        print(f"Jumlah data dari NASA FIRMS: {len(df)}")

        if df.empty:
            print("Tidak ada data hotspot pada periode yang diminta.")
            return

        # 2. TRANSFORM
        rows = transform_data(df)

        print(f"Jumlah data setelah transformasi: {len(rows)}")

        if not rows:
            print("Tidak ada baris valid untuk disimpan.")
            return

        # 3. LOAD
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

        create_table(mysql_hook)

        sql = f"""
            INSERT INTO {TABLE_NAME} (
                location_id,
                reference_latitude,
                reference_longitude,
                latitude,
                longitude,
                brightness,
                scan,
                track,
                acq_date,
                acq_time,
                datetime_utc,
                datetime_local,
                satellite,
                instrument,
                confidence,
                version,
                bright_ti4,
                bright_ti5,
                frp,
                daynight,
                source
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                brightness = VALUES(brightness),
                scan = VALUES(scan),
                track = VALUES(track),
                confidence = VALUES(confidence),
                version = VALUES(version),
                bright_ti4 = VALUES(bright_ti4),
                bright_ti5 = VALUES(bright_ti5),
                frp = VALUES(frp),
                daynight = VALUES(daynight),
                source = VALUES(source)
        """

        connection = mysql_hook.get_conn()
        cursor = connection.cursor()

        try:
            cursor.executemany(sql, rows)
            connection.commit()

            print(
                f"Berhasil memproses {len(rows)} baris "
                f"ke tabel {TABLE_NAME}."
            )

        except Exception:
            connection.rollback()
            raise

        finally:
            cursor.close()
            connection.close()

    extract_transform_load()


etl_firms_mysql()