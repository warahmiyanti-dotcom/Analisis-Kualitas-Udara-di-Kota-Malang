# ============================================================
# ETL NASA FIRMS - MALANG
# MYSQL VERSION
# Periode: 1 Agustus 2026 - data terakhir
# ============================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, date, timedelta
from io import StringIO

import time
import requests
import pandas as pd


# ============================================================
# CONFIGURATION
# ============================================================

# MASUKKAN MAP_KEY FIRMS KAMU SENDIRI
MAP_KEY = "6ee9e8147511b6ae583d0dd8d4ac14ea"

# Connection Airflow
MYSQL_CONN_ID = "mysql_airflow"

# Nama tabel MySQL
TABLE_NAME = "firms_data"

# Identitas lokasi
LOCATION_ID = "MALANG"

# Zona waktu lokal
TIMEZONE_LOCAL = "Asia/Jakarta"

# Bounding box Malang
# west, south, east, north
AREA = "112.45,-8.15,112.85,-7.70"

# ============================================================
# PERIODE PENELITIAN
# ============================================================

START_DATE = date(2026, 8, 1)

# Maksimal request FIRMS Area API
MAX_DAYS_PER_REQUEST = 5

# Batch insert MySQL
BATCH_SIZE = 1000

# Timeout request
REQUEST_TIMEOUT = 120

# Percobaan request
HTTP_RETRIES = 3

# ============================================================
# SOURCE FIRMS
# ============================================================

# NOAA-21 dipilih karena mencakup periode penelitian
SOURCES = {
    "NOAA21": [
        "VIIRS_NOAA21_NRT"
    ]
}


# ============================================================
# URL FIRMS
# ============================================================

FIRMS_AREA_URL = (
    "https://firms.modaps.eosdis.nasa.gov/"
    "api/area/csv"
)

FIRMS_AVAILABILITY_URL = (
    "https://firms.modaps.eosdis.nasa.gov/"
    "api/data_availability/csv"
)


# ============================================================
# HTTP SESSION
# ============================================================

def create_session():

    session = requests.Session()

    session.headers.update({
        "User-Agent": (
            "Airflow-NASA-FIRMS-ETL/1.0"
        )
    })

    return session


# ============================================================
# DATABASE TABLE
# ============================================================

def create_table_if_not_exists():

    hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:

        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (

                id BIGINT AUTO_INCREMENT PRIMARY KEY,

                location_id VARCHAR(100),

                latitude DOUBLE,

                longitude DOUBLE,

                acq_date DATE,

                acq_time VARCHAR(10),

                datetime_utc DATETIME,

                datetime_local DATETIME,

                satellite VARCHAR(50),

                instrument VARCHAR(50),

                confidence VARCHAR(50),

                frp DOUBLE,

                daynight VARCHAR(10),

                created_at TIMESTAMP
                    DEFAULT CURRENT_TIMESTAMP,

                UNIQUE KEY unique_firms_data (
                    datetime_utc,
                    latitude,
                    longitude,
                    satellite
                )

            )
            """
        )

        conn.commit()

        print(
            f"Tabel {TABLE_NAME} siap digunakan."
        )

    except Exception as e:

        conn.rollback()

        print(
            f"Gagal membuat tabel: {e}"
        )

        raise

    finally:

        cursor.close()
        conn.close()


# ============================================================
# HAPUS DATA SEBELUM START_DATE
# ============================================================

def delete_old_data():

    hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:

        cursor.execute(
            f"""
            DELETE FROM {TABLE_NAME}
            WHERE acq_date < %s
            """,
            (START_DATE,)
        )

        deleted_rows = cursor.rowcount

        conn.commit()

        print(
            f"Data sebelum {START_DATE} "
            f"yang dihapus: {deleted_rows} baris."
        )

    except Exception as e:

        conn.rollback()

        print(
            f"Gagal menghapus data lama: {e}"
        )

        raise

    finally:

        cursor.close()
        conn.close()


# ============================================================
# GET LAST DATETIME
# ============================================================

def get_last_datetime():

    hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:

        cursor.execute(
            f"""
            SELECT MAX(datetime_utc)
            FROM {TABLE_NAME}
            WHERE acq_date >= %s
            """,
            (START_DATE,)
        )

        result = cursor.fetchone()

        if result and result[0]:

            print(
                f"Data terakhir: {result[0]}"
            )

            return result[0]

        print(
            "Belum ada data FIRMS setelah START_DATE."
        )

        return None

    except Exception as e:

        print(
            f"Gagal membaca datetime terakhir: {e}"
        )

        return None

    finally:

        cursor.close()
        conn.close()


# ============================================================
# GET FIRMS DATA AVAILABILITY
# ============================================================

def get_data_availability():

    session = create_session()

    url = (
        f"{FIRMS_AVAILABILITY_URL}/"
        f"{MAP_KEY}/ALL"
    )

    print(
        "Mengambil informasi availability FIRMS..."
    )

    for attempt in range(
        1,
        HTTP_RETRIES + 1
    ):

        try:

            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            print(
                f"Availability HTTP status: "
                f"{response.status_code}"
            )

            if response.status_code != 200:

                print(
                    "Availability gagal:"
                    f" {response.text[:300]}"
                )

                if attempt < HTTP_RETRIES:

                    time.sleep(5)

                    continue

                return pd.DataFrame()

            text = response.text.strip()

            if not text:

                print(
                    "Response availability kosong."
                )

                return pd.DataFrame()

            df = pd.read_csv(
                StringIO(text)
            )

            print(
                "Availability berhasil."
            )

            return df

        except Exception as e:

            print(
                f"Error availability "
                f"percobaan {attempt}: {e}"
            )

            if attempt < HTTP_RETRIES:

                time.sleep(5)

    return pd.DataFrame()


# ============================================================
# CEK SOURCE YANG TERSEDIA
# ============================================================

def get_source_for_period(
    start_date,
    end_date,
    availability_df
):

    candidates = []

    if availability_df.empty:

        print(
            "Availability tidak tersedia."
        )

        # fallback ke NOAA-21
        return [
            (
                "NOAA21",
                "VIIRS_NOAA21_NRT"
            )
        ]

    if "data_id" not in availability_df.columns:

        print(
            "Kolom data_id tidak ditemukan."
        )

        return candidates

    availability_df = (
        availability_df.copy()
    )

    availability_df["min_date"] = (
        pd.to_datetime(
            availability_df["min_date"],
            errors="coerce"
        ).dt.date
    )

    availability_df["max_date"] = (
        pd.to_datetime(
            availability_df["max_date"],
            errors="coerce"
        ).dt.date
    )

    for satellite_name, source_list in SOURCES.items():

        for source in source_list:

            match = availability_df[
                availability_df["data_id"]
                == source
            ]

            if match.empty:

                continue

            for _, row in match.iterrows():

                min_date = row["min_date"]
                max_date = row["max_date"]

                if pd.isna(min_date):
                    continue

                if pd.isna(max_date):
                    continue

                if (
                    start_date >= min_date
                    and end_date <= max_date
                ):

                    candidates.append(
                        (
                            satellite_name,
                            source
                        )
                    )

                    break

    return candidates


# ============================================================
# DOWNLOAD FIRMS DATA
# ============================================================

def download_firms_data(
    source,
    start_date,
    end_date
):

    day_range = (
        end_date - start_date
    ).days + 1

    if day_range < 1:

        return pd.DataFrame()

    if day_range > MAX_DAYS_PER_REQUEST:

        raise ValueError(
            "Request FIRMS maksimal 5 hari."
        )

    url = (
        f"{FIRMS_AREA_URL}/"
        f"{MAP_KEY}/"
        f"{source}/"
        f"{AREA}/"
        f"{day_range}/"
        f"{start_date}"
    )

    session = create_session()

    for attempt in range(
        1,
        HTTP_RETRIES + 1
    ):

        try:

            print(
                "\nRequest FIRMS:"
            )

            print(
                f"Source : {source}"
            )

            print(
                f"Date   : "
                f"{start_date} - {end_date}"
            )

            response = session.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            print(
                f"HTTP status: "
                f"{response.status_code}"
            )

            if response.status_code != 200:

                print(
                    "FIRMS response error:"
                    f" {response.text[:500]}"
                )

                if attempt < HTTP_RETRIES:

                    time.sleep(5)

                    continue

                return pd.DataFrame()

            text = response.text.strip()

            if not text:

                print(
                    "FIRMS mengembalikan "
                    "response kosong."
                )

                return pd.DataFrame()

            first_line = (
                text.splitlines()[0]
                .strip()
                .lower()
            )

            if (
                "error" in first_line
                or "invalid" in first_line
                or "exceeded" in first_line
            ):

                print(
                    "FIRMS mengembalikan pesan:"
                )

                print(
                    text[:500]
                )

                return pd.DataFrame()

            df = pd.read_csv(
                StringIO(text)
            )

            if df.empty:

                print(
                    "Tidak ada hotspot "
                    "pada periode ini."
                )

                return pd.DataFrame()

            print(
                f"Data diterima: "
                f"{len(df)} baris."
            )

            return df

        except pd.errors.EmptyDataError:

            print(
                "CSV FIRMS kosong."
            )

            return pd.DataFrame()

        except Exception as e:

            print(
                f"Download error "
                f"percobaan {attempt}: {e}"
            )

            if attempt < HTTP_RETRIES:

                time.sleep(5)

    return pd.DataFrame()


# ============================================================
# PROCESS FIRMS DATA
# ============================================================

def process_firms_data(df):

    if df.empty:

        return pd.DataFrame()

    print(
        "\nKolom asli FIRMS:"
    )

    print(
        df.columns.tolist()
    )

    required_columns = [
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
        "satellite",
        "instrument",
        "confidence",
        "frp",
        "daynight"
    ]

    missing = [
        col
        for col in required_columns
        if col not in df.columns
    ]

    if missing:

        print(
            "Kolom FIRMS tidak tersedia:"
        )

        print(missing)

        return pd.DataFrame()

    # ========================================================
    # LOCATION ID
    # ========================================================

    df["location_id"] = LOCATION_ID

    # ========================================================
    # LATITUDE / LONGITUDE
    # ========================================================

    df["latitude"] = pd.to_numeric(
        df["latitude"],
        errors="coerce"
    )

    df["longitude"] = pd.to_numeric(
        df["longitude"],
        errors="coerce"
    )

    # ========================================================
    # FILTER MALANG
    # ========================================================

    df = df[
        (df["longitude"] >= 112.45)
        &
        (df["longitude"] <= 112.85)
        &
        (df["latitude"] >= -8.15)
        &
        (df["latitude"] <= -7.70)
    ].copy()

    print(
        f"Data setelah filter Malang: "
        f"{len(df)}"
    )

    if df.empty:

        return pd.DataFrame()

    # ========================================================
    # ACQ DATE
    # ========================================================

    df["acq_date"] = pd.to_datetime(
        df["acq_date"],
        errors="coerce"
    ).dt.date

    # ========================================================
    # FILTER PERIODE
    # ========================================================

    df = df[
        df["acq_date"] >= START_DATE
    ].copy()

    print(
        f"Data setelah filter tanggal "
        f">= {START_DATE}: {len(df)}"
    )

    if df.empty:

        return pd.DataFrame()

    # ========================================================
    # ACQ TIME
    # ========================================================

    df["acq_time"] = (
        df["acq_time"]
        .astype(str)
        .str.replace(
            ".0",
            "",
            regex=False
        )
        .str.replace(
            "nan",
            "",
            regex=False
        )
        .str.zfill(4)
    )

    # ========================================================
    # DATETIME UTC
    # ========================================================

    df["datetime_utc"] = pd.to_datetime(
        df["acq_date"].astype(str)
        + " "
        + df["acq_time"].str[:2]
        + ":"
        + df["acq_time"].str[2:4],
        errors="coerce"
    )

    # ========================================================
    # DATETIME LOCAL
    # ========================================================

    df["datetime_local"] = (
        df["datetime_utc"]
        .dt.tz_localize("UTC")
        .dt.tz_convert(TIMEZONE_LOCAL)
        .dt.tz_localize(None)
    )

    # ========================================================
    # FRP
    # ========================================================

    df["frp"] = pd.to_numeric(
        df["frp"],
        errors="coerce"
    )

    # ========================================================
    # HAPUS DATA INVALID
    # ========================================================

    df = df.dropna(
        subset=[
            "latitude",
            "longitude",
            "datetime_utc"
        ]
    )

    # ========================================================
    # DUPLICATE
    # ========================================================

    df = df.drop_duplicates(
        subset=[
            "datetime_utc",
            "latitude",
            "longitude",
            "satellite"
        ]
    )

    # ========================================================
    # FINAL COLUMNS
    # ========================================================

    final_columns = [
        "location_id",
        "latitude",
        "longitude",
        "acq_date",
        "acq_time",
        "datetime_utc",
        "datetime_local",
        "satellite",
        "instrument",
        "confidence",
        "frp",
        "daynight"
    ]

    df = df[
        final_columns
    ].copy()

    print(
        "\nKolom final FIRMS:"
    )

    print(
        df.columns.tolist()
    )

    print(
        f"Jumlah data final: "
        f"{len(df)}"
    )

    return df


# ============================================================
# SAVE TO MYSQL
# ============================================================

def save_to_mysql(df):

    if df.empty:

        print(
            "Tidak ada data untuk disimpan."
        )

        return

    hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    conn = hook.get_conn()
    cursor = conn.cursor()

    insert_query = f"""
        INSERT IGNORE INTO {TABLE_NAME} (

            location_id,
            latitude,
            longitude,
            acq_date,
            acq_time,
            datetime_utc,
            datetime_local,
            satellite,
            instrument,
            confidence,
            frp,
            daynight

        )
        VALUES (

            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s

        )
    """

    try:

        records = []

        for _, row in df.iterrows():

            records.append(
                (
                    row["location_id"],
                    row["latitude"],
                    row["longitude"],
                    row["acq_date"],
                    row["acq_time"],
                    row["datetime_utc"],
                    row["datetime_local"],
                    row["satellite"],
                    row["instrument"],
                    row["confidence"],
                    row["frp"],
                    row["daynight"]
                )
            )

        total = len(records)

        print(
            f"Total record yang akan "
            f"dimasukkan: {total}"
        )

        for start in range(
            0,
            total,
            BATCH_SIZE
        ):

            end = min(
                start + BATCH_SIZE,
                total
            )

            batch = records[
                start:end
            ]

            cursor.executemany(
                insert_query,
                batch
            )

            conn.commit()

            print(
                f"Batch berhasil: "
                f"{start + 1}-{end} "
                f"/ {total}"
            )

        print(
            "Data berhasil disimpan "
            "ke MySQL."
        )

    except Exception as e:

        conn.rollback()

        print(
            f"Database error: {e}"
        )

        raise

    finally:

        cursor.close()
        conn.close()


# ============================================================
# ETL MAIN
# ============================================================

def etl_firms():

    print(
        "\n"
        + "=" * 70
    )

    print(
        "ETL NASA FIRMS - MALANG"
    )

    print(
        "=" * 70
    )

    # ========================================================
    # 1. DATABASE
    # ========================================================

    create_table_if_not_exists()

    # ========================================================
    # 2. HAPUS DATA SEBELUM PERIODE PENELITIAN
    # ========================================================

    delete_old_data()

    # ========================================================
    # 3. LAST DATA
    # ========================================================

    last_datetime = get_last_datetime()

    today = date.today()

    # ========================================================
    # 4. TENTUKAN MODE
    # ========================================================

    if last_datetime is None:

        start_date = START_DATE

        print(
            "\nMODE: INITIAL LOAD"
        )

    else:

        # Ambil kembali 1 hari sebelumnya
        # untuk menghindari data yang terlewat
        start_date = (
            last_datetime.date()
            - timedelta(days=1)
        )

        if start_date < START_DATE:

            start_date = START_DATE

        print(
            "\nMODE: INCREMENTAL"
        )

    print(
        f"Mulai : {start_date}"
    )

    print(
        f"Sampai: {today}"
    )

    # ========================================================
    # 5. CEK PERIODE
    # ========================================================

    if start_date > today:

        print(
            "Tidak ada tanggal baru."
        )

        return

    # ========================================================
    # 6. AVAILABILITY
    # ========================================================

    availability_df = (
        get_data_availability()
    )

    # ========================================================
    # 7. DOWNLOAD
    # ========================================================

    all_data = []

    current_start = start_date

    while current_start <= today:

        current_end = min(
            current_start
            + timedelta(
                days=MAX_DAYS_PER_REQUEST - 1
            ),
            today
        )

        print(
            "\n"
            + "-" * 70
        )

        print(
            f"Periode: "
            f"{current_start} - "
            f"{current_end}"
        )

        # ====================================================
        # CARI SOURCE
        # ====================================================

        candidates = get_source_for_period(
            current_start,
            current_end,
            availability_df
        )

        if not candidates:

            print(
                "Tidak ditemukan source "
                "yang mencakup periode ini."
            )

            current_start = (
                current_end
                + timedelta(days=1)
            )

            continue

        # ====================================================
        # COBA SOURCE
        # ====================================================

        period_downloaded = False

        for (
            satellite_name,
            source
        ) in candidates:

            print(
                "\nMencoba:"
            )

            print(
                f"Satellite = "
                f"{satellite_name}"
            )

            print(
                f"Source = "
                f"{source}"
            )

            df = download_firms_data(
                source=source,
                start_date=current_start,
                end_date=current_end
            )

            if df.empty:

                print(
                    f"Source {source} "
                    "tidak menghasilkan data."
                )

                continue

            all_data.append(df)

            print(
                f"Source {source} berhasil."
            )

            period_downloaded = True

            break

        if not period_downloaded:

            print(
                "Tidak ada source yang "
                f"berhasil untuk "
                f"{current_start} - "
                f"{current_end}."
            )

        # ====================================================
        # NEXT PERIOD
        # ====================================================

        current_start = (
            current_end
            + timedelta(days=1)
        )

    # ========================================================
    # 8. JIKA TIDAK ADA DATA
    # ========================================================

    if not all_data:

        print(
            "\nTidak ada data FIRMS "
            "yang berhasil diperoleh."
        )

        return

    # ========================================================
    # 9. GABUNGKAN
    # ========================================================

    print(
        "\nMenggabungkan data FIRMS..."
    )

    combined_df = pd.concat(
        all_data,
        ignore_index=True
    )

    print(
        f"Total data mentah: "
        f"{len(combined_df)}"
    )

    # ========================================================
    # 10. PROCESS
    # ========================================================

    processed_df = process_firms_data(
        combined_df
    )

    # ========================================================
    # 11. SAVE
    # ========================================================

    if not processed_df.empty:

        save_to_mysql(
            processed_df
        )

    else:

        print(
            "Tidak ada data valid "
            "setelah processing."
        )

    # ========================================================
    # 12. SELESAI
    # ========================================================

    print(
        "\n"
        + "=" * 70
    )

    print(
        "ETL FIRMS SELESAI"
    )

    print(
        "=" * 70
    )


# ============================================================
# AIRFLOW DAG
# ============================================================

default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(
        minutes=5
    ),
}


with DAG(

    dag_id="etl_firms",

    default_args=default_args,

    description=(
        "ETL NASA FIRMS "
        "untuk wilayah Malang"
    ),

    # Jadwal DAG dimulai Agustus
    start_date=datetime(
        2026,
        8,
        1
    ),

    # Jalankan setiap hari
    schedule="0 1 * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "ETL",
        "NASA",
        "FIRMS",
        "Malang",
        "MySQL"
    ]

) as dag:

    etl_firms_task = PythonOperator(

        task_id="etl_firms",

        python_callable=etl_firms

    )