from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timezone
import requests


# ============================================================
# KONFIGURASI OPEN-METEO
# ============================================================

LOCATION_ID = "malang"

LATITUDE = -7.9185093
LONGITUDE = 112.651344

# Historical Weather API
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Data awal
START_DATE = "2026-08-01"

# MySQL
MYSQL_CONN_ID = "mysql_airflow"

# Nama tabel
TABLE_NAME = "openmeteo_data"


# ============================================================
# 1. MEMBUAT TABEL JIKA BELUM ADA
# ============================================================

def create_table_if_not_exists(cursor):

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (

            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            location_id VARCHAR(50) NOT NULL,

            datetime_utc DATETIME NOT NULL,

            datetime_local DATETIME,

            latitude DOUBLE,

            longitude DOUBLE,

            temperature DOUBLE,

            humidity DOUBLE,

            precipitation DOUBLE,

            wind_speed DOUBLE,

            wind_direction DOUBLE,

            pressure DOUBLE,

            cloud_cover DOUBLE,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE KEY unique_openmeteo_data (
                location_id,
                datetime_utc
            )
        )
    """)

    print(
        f"Tabel {TABLE_NAME} siap digunakan."
    )


# ============================================================
# 2. MENGAMBIL DATA DARI OPEN-METEO
# ============================================================

def get_openmeteo_data(
    start_date,
    end_date
):

    params = {

        "latitude": LATITUDE,

        "longitude": LONGITUDE,

        "start_date": start_date,

        "end_date": end_date,

        # Data hourly
        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation,"
            "wind_speed_10m,"
            "wind_direction_10m,"
            "surface_pressure,"
            "cloud_cover"
        ),

        # Gunakan UTC agar konsisten
        # dengan OpenAQ
        "timezone": "UTC",

        # Satuan
        "temperature_unit": "celsius",

        "wind_speed_unit": "kmh",

        "precipitation_unit": "mm",

        "timeformat": "iso8601"
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=120
    )

    print(
        "URL:",
        response.url
    )

    print(
        "Status Open-Meteo:",
        response.status_code
    )

    response.raise_for_status()

    return response.json()


# ============================================================
# 3. MENGAMBIL DATA TERAKHIR DARI DATABASE
# ============================================================

def get_last_datetime(cursor):

    cursor.execute(
        f"""
        SELECT MAX(datetime_utc)
        FROM {TABLE_NAME}
        WHERE location_id = %s
        """,
        (LOCATION_ID,)
    )

    result = cursor.fetchone()

    return result[0]


# ============================================================
# 4. ETL OPEN-METEO
# ============================================================

def etl_openmeteo():

    print("=" * 70)
    print("ETL OPEN-METEO - MALANG")
    print("=" * 70)

    # --------------------------------------------------------
    # KONEKSI MYSQL
    # --------------------------------------------------------

    mysql_hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    conn = mysql_hook.get_conn()

    cursor = conn.cursor()

    try:

        # ----------------------------------------------------
        # BUAT TABEL JIKA BELUM ADA
        # ----------------------------------------------------

        print(
            "Memeriksa tabel database..."
        )

        create_table_if_not_exists(
            cursor
        )

        conn.commit()

        # ----------------------------------------------------
        # CEK DATA TERAKHIR
        # ----------------------------------------------------

        last_datetime = get_last_datetime(
            cursor
        )

        # ====================================================
        # INITIAL LOAD
        # ====================================================

        if last_datetime is None:

            start_date = START_DATE

            print(
                "MODE: INITIAL LOAD"
            )

            print(
                f"Data dimulai dari: "
                f"{start_date}"
            )

        # ====================================================
        # INCREMENTAL LOAD
        # ====================================================

        else:

            start_date = (
                last_datetime.strftime(
                    "%Y-%m-%d"
                )
            )

            print(
                "MODE: INCREMENTAL LOAD"
            )

            print(
                "Data terakhir di database: "
                f"{last_datetime}"
            )

        # ----------------------------------------------------
        # TANGGAL AKHIR
        # ----------------------------------------------------

        end_date = (
            datetime.now(
                timezone.utc
            )
            .strftime(
                "%Y-%m-%d"
            )
        )

        print(
            f"Data diambil sampai: "
            f"{end_date}"
        )

        # ----------------------------------------------------
        # EXTRACT
        # ----------------------------------------------------

        data = get_openmeteo_data(
            start_date,
            end_date
        )

        # ----------------------------------------------------
        # AMBIL BAGIAN HOURLY
        # ----------------------------------------------------

        hourly = data.get(
            "hourly",
            {}
        )

        times = hourly.get(
            "time",
            []
        )

        temperatures = hourly.get(
            "temperature_2m",
            []
        )

        humidities = hourly.get(
            "relative_humidity_2m",
            []
        )

        precipitations = hourly.get(
            "precipitation",
            []
        )

        wind_speeds = hourly.get(
            "wind_speed_10m",
            []
        )

        wind_directions = hourly.get(
            "wind_direction_10m",
            []
        )

        pressures = hourly.get(
            "surface_pressure",
            []
        )

        cloud_covers = hourly.get(
            "cloud_cover",
            []
        )

        print(
            "Jumlah data hourly dari API: "
            f"{len(times)}"
        )

        # ====================================================
        # TRANSFORM + LOAD
        # ====================================================

        total_data = 0

        for i in range(
            len(times)
        ):

            # ------------------------------------------------
            # DATETIME
            # ------------------------------------------------

            datetime_utc = times[i]

            # API menggunakan UTC
            datetime_local = datetime_utc

            # ------------------------------------------------
            # NILAI WEATHER
            # ------------------------------------------------

            temperature = (
                temperatures[i]
                if i < len(temperatures)
                else None
            )

            humidity = (
                humidities[i]
                if i < len(humidities)
                else None
            )

            precipitation = (
                precipitations[i]
                if i < len(precipitations)
                else None
            )

            wind_speed = (
                wind_speeds[i]
                if i < len(wind_speeds)
                else None
            )

            wind_direction = (
                wind_directions[i]
                if i < len(wind_directions)
                else None
            )

            pressure = (
                pressures[i]
                if i < len(pressures)
                else None
            )

            cloud_cover = (
                cloud_covers[i]
                if i < len(cloud_covers)
                else None
            )

            # =================================================
            # LOAD KE MYSQL
            # =================================================

            cursor.execute(
                f"""
                INSERT INTO {TABLE_NAME} (

                    location_id,
                    datetime_utc,
                    datetime_local,
                    latitude,
                    longitude,
                    temperature,
                    humidity,
                    precipitation,
                    wind_speed,
                    wind_direction,
                    pressure,
                    cloud_cover

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

                ON DUPLICATE KEY UPDATE

                    datetime_local =
                        VALUES(datetime_local),

                    latitude =
                        VALUES(latitude),

                    longitude =
                        VALUES(longitude),

                    temperature =
                        VALUES(temperature),

                    humidity =
                        VALUES(humidity),

                    precipitation =
                        VALUES(precipitation),

                    wind_speed =
                        VALUES(wind_speed),

                    wind_direction =
                        VALUES(wind_direction),

                    pressure =
                        VALUES(pressure),

                    cloud_cover =
                        VALUES(cloud_cover)
                """,

                (

                    LOCATION_ID,

                    datetime_utc,

                    datetime_local,

                    LATITUDE,

                    LONGITUDE,

                    temperature,

                    humidity,

                    precipitation,

                    wind_speed,

                    wind_direction,

                    pressure,

                    cloud_cover

                )
            )

            total_data += 1

        # ----------------------------------------------------
        # COMMIT
        # ----------------------------------------------------

        conn.commit()

        print("=" * 70)

        print(
            "ETL OPEN-METEO SELESAI"
        )

        print("=" * 70)

        print(
            f"Total data diproses: "
            f"{total_data}"
        )

    except Exception:

        conn.rollback()

        print(
            "Terjadi error. "
            "Transaksi database dibatalkan."
        )

        raise

    finally:

        cursor.close()

        conn.close()


# ============================================================
# 5. AIRFLOW DAG
# ============================================================

with DAG(

    dag_id="etl_openmeteo",

    start_date=datetime(
        2026,
        1,
        1
    ),

    # Jalan otomatis setiap 1 jam
    schedule="0 * * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "ETL",
        "Open-Meteo",
        "Malang",
        "MySQL"
    ]

) as dag:

    run_etl = PythonOperator(

        task_id=(
            "extract_transform_load_openmeteo"
        ),

        python_callable=etl_openmeteo

    )