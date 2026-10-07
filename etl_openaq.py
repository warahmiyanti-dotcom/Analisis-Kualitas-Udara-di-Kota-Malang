# ================================================================
# ETL OPENAQ - MYSQL
# ================================================================
# Sumber   : OpenAQ API v3
# Lokasi   : Malang
# Parameter: PM2.5
# Database : MySQL
# Tabel    : openaq_data
# ================================================================

from datetime import datetime, timedelta, timezone

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


# ================================================================
# KONFIGURASI
# ================================================================

API_BASE_URL = "https://api.openaq.org/v3"

# Masukkan API key OpenAQ kamu sendiri
API_KEY = "ea9abdfeb0d74ec00f7f61c35204ce88f571a9af964fdbd361036923f7c84de1"

HEADERS = {
    "X-API-Key": API_KEY
}

MYSQL_CONN_ID = "mysql_airflow"

TABLE_NAME = "openaq_data"

LOCATION_ID = 6144741

LATITUDE = -7.9185093
LONGITUDE = 112.651344

# Data awal
START_DATE = "2026-08-01T00:00:00Z"

TIMEOUT = 60

LIMIT = 1000


# ================================================================
# KONVERSI DATETIME OPENAQ -> MYSQL
# ================================================================

def to_mysql_datetime(value):
    """
    Mengubah datetime OpenAQ ke format MySQL DATETIME.

    Contoh:
    2026-01-01T00:00:00Z
    ->
    2026-01-01 00:00:00
    """

    if value is None:
        return None

    # Jika sudah datetime Python
    if isinstance(value, datetime):

        if value.tzinfo is not None:

            value = value.astimezone(
                timezone.utc
            ).replace(
                tzinfo=None
            )

        return value.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    # Jika string
    if isinstance(value, str):

        value = value.strip()

        if not value:
            return None

        # Format ISO dengan Z
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        try:

            dt = datetime.fromisoformat(
                value
            )

            if dt.tzinfo is not None:

                dt = dt.astimezone(
                    timezone.utc
                ).replace(
                    tzinfo=None
                )

            return dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        except ValueError:

            # Coba format MySQL langsung
            try:

                dt = datetime.strptime(
                    value,
                    "%Y-%m-%d %H:%M:%S"
                )

                return dt.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

            except ValueError:

                print(
                    f"WARNING: format datetime "
                    f"tidak dikenali: {value}"
                )

                return None

    return None


# ================================================================
# KONVERSI DATETIME MYSQL -> OPENAQ API
# ================================================================

def datetime_to_openaq(value):
    """
    Mengubah datetime MySQL menjadi format
    yang dapat digunakan OpenAQ API.
    """

    if value is None:
        return START_DATE

    if isinstance(value, datetime):

        # Anggap datetime MySQL sebagai UTC
        value = value.replace(
            tzinfo=timezone.utc
        )

        return value.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    if isinstance(value, str):

        try:

            dt = datetime.strptime(
                value,
                "%Y-%m-%d %H:%M:%S"
            )

            dt = dt.replace(
                tzinfo=timezone.utc
            )

            return dt.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

        except ValueError:

            return START_DATE

    return START_DATE


# ================================================================
# CREATE TABLE
# ================================================================

def create_table(cursor):

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (

            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            location_id VARCHAR(50) NOT NULL,

            latitude DOUBLE,

            longitude DOUBLE,

            sensor_id INT NOT NULL,

            sensor_name VARCHAR(255),

            parameter VARCHAR(100),

            display_name VARCHAR(255),

            unit VARCHAR(50),

            value DOUBLE,

            datetime_utc DATETIME NOT NULL,

            datetime_local DATETIME,

            datetime_to_utc DATETIME,

            datetime_to_local DATETIME,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE KEY unique_openaq_data (
                location_id,
                sensor_id,
                datetime_utc
            )

        )
        """
    )


# ================================================================
# GET SENSOR
# ================================================================

def get_sensors():

    url = (
        f"{API_BASE_URL}/locations/"
        f"{LOCATION_ID}/sensors"
    )

    print("=" * 70)
    print("MENGAMBIL SENSOR OPENAQ")
    print("=" * 70)

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=TIMEOUT
    )

    response.raise_for_status()

    data = response.json()

    results = data.get(
        "results",
        []
    )

    print(
        f"Jumlah sensor ditemukan: "
        f"{len(results)}"
    )

    sensors = []

    for sensor in results:

        sensor_id = sensor.get(
            "id"
        )

        parameter = sensor.get(
            "parameter"
        ) or {}

        if not isinstance(
            parameter,
            dict
        ):
            continue

        parameter_name = parameter.get(
            "name"
        )

        if not parameter_name:
            continue

        # ========================================================
        # HANYA PM2.5
        # ========================================================

        normalized_parameter = (
            str(parameter_name)
            .lower()
            .replace(".", "")
            .replace("_", "")
            .replace("-", "")
        )

        if normalized_parameter not in [
            "pm25"
        ]:
            continue

        sensors.append(
            {
                "sensor_id": sensor_id,

                "sensor_name": sensor.get(
                    "name"
                ),

                "parameter": parameter_name,

                "display_name": parameter.get(
                    "displayName"
                ),

                "unit": parameter.get(
                    "units"
                )
            }
        )

    print(
        f"Sensor PM2.5 ditemukan: "
        f"{len(sensors)}"
    )

    for sensor in sensors:

        print(
            f"  Sensor ID: "
            f"{sensor['sensor_id']} | "
            f"{sensor['sensor_name']}"
        )

    return sensors


# ================================================================
# GET LAST DATETIME DARI MYSQL
# ================================================================

def get_last_datetime(cursor):

    cursor.execute(
        f"""
        SELECT MAX(datetime_utc)
        FROM {TABLE_NAME}
        """
    )

    result = cursor.fetchone()

    if not result:
        return None

    return result[0]


# ================================================================
# GET MEASUREMENTS OPENAQ
# ================================================================

def get_measurements(
    sensor_id,
    date_from=None
):

    all_results = []

    page = 1

    while True:

        params = {
            "limit": LIMIT,
            "page": page
        }

        if date_from:

            params[
                "datetime_from"
            ] = date_from

        url = (
            f"{API_BASE_URL}/sensors/"
            f"{sensor_id}/measurements"
        )

        print(
            f"Mengambil sensor "
            f"{sensor_id}, "
            f"page {page}"
        )

        response = requests.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=TIMEOUT
        )

        response.raise_for_status()

        data = response.json()

        results = data.get(
            "results",
            []
        )

        # --------------------------------------------------------
        # Tidak ada hasil
        # --------------------------------------------------------

        if not results:

            print(
                f"Tidak ada data pada "
                f"page {page}."
            )

            break

        all_results.extend(
            results
        )

        print(
            f"Data page {page}: "
            f"{len(results)}"
        )

        print(
            f"Total sementara: "
            f"{len(all_results)}"
        )

        # --------------------------------------------------------
        # META
        # --------------------------------------------------------

        meta = data.get(
            "meta"
        ) or {}

        # --------------------------------------------------------
        # PERBAIKAN:
        # found bisa integer atau string
        # --------------------------------------------------------

        found = meta.get(
            "found"
        )

        try:

            if found is not None:
                found = int(found)

        except (
            ValueError,
            TypeError
        ):

            found = None

        # --------------------------------------------------------
        # Jika jumlah data sudah mencapai found
        # --------------------------------------------------------

        if found is not None:

            if len(all_results) >= found:

                print(
                    f"Semua data sudah diambil: "
                    f"{len(all_results)}"
                )

                break

        # --------------------------------------------------------
        # Jika hasil kurang dari LIMIT,
        # berarti kemungkinan halaman terakhir
        # --------------------------------------------------------

        if len(results) < LIMIT:

            print(
                "Jumlah hasil kurang dari limit. "
                "Pagination selesai."
            )

            break

        # --------------------------------------------------------
        # Lanjut halaman berikutnya
        # --------------------------------------------------------

        page += 1

    return all_results


# ================================================================
# ETL UTAMA
# ================================================================

def etl_openaq():

    print("=" * 70)
    print("START ETL OPENAQ")
    print("=" * 70)

    # ------------------------------------------------------------
    # MYSQL CONNECTION
    # ------------------------------------------------------------

    mysql_hook = MySqlHook(
        mysql_conn_id=MYSQL_CONN_ID
    )

    conn = mysql_hook.get_conn()

    cursor = conn.cursor()

    try:

        # --------------------------------------------------------
        # CREATE TABLE
        # --------------------------------------------------------

        create_table(
            cursor
        )

        conn.commit()

        print(
            f"Tabel {TABLE_NAME} siap."
        )

        # --------------------------------------------------------
        # CEK DATA TERAKHIR
        # --------------------------------------------------------

        last_datetime = get_last_datetime(
            cursor
        )

        if last_datetime:

            print(
                f"Data terakhir di MySQL: "
                f"{last_datetime}"
            )

            # ----------------------------------------------------
            # Ambil sedikit sebelum data terakhir
            # untuk mencegah data terlewat
            # ----------------------------------------------------

            if isinstance(
                last_datetime,
                datetime
            ):

                last_datetime_adjusted = (
                    last_datetime
                    - timedelta(
                        minutes=1
                    )
                )

                date_from = (
                    datetime_to_openaq(
                        last_datetime_adjusted
                    )
                )

            else:

                date_from = START_DATE

        else:

            print(
                "Belum ada data OpenAQ di MySQL."
            )

            print(
                f"Mulai mengambil data dari "
                f"{START_DATE}"
            )

            date_from = START_DATE

        print(
            f"datetime_from API: "
            f"{date_from}"
        )

        # --------------------------------------------------------
        # GET SENSOR
        # --------------------------------------------------------

        sensors = get_sensors()

        if not sensors:

            print(
                "Tidak ada sensor PM2.5."
            )

            return

        total_data = 0

        # --------------------------------------------------------
        # LOOP SENSOR
        # --------------------------------------------------------

        for sensor in sensors:

            sensor_id = sensor[
                "sensor_id"
            ]

            sensor_name = sensor[
                "sensor_name"
            ]

            parameter_name = sensor[
                "parameter"
            ]

            parameter_display = sensor[
                "display_name"
            ]

            unit = sensor[
                "unit"
            ]

            print()
            print("=" * 70)

            print(
                f"Sensor ID   : "
                f"{sensor_id}"
            )

            print(
                f"Sensor Name : "
                f"{sensor_name}"
            )

            print(
                f"Parameter   : "
                f"{parameter_name}"
            )

            print(
                f"Unit        : "
                f"{unit}"
            )

            print("=" * 70)

            # ----------------------------------------------------
            # GET MEASUREMENTS
            # ----------------------------------------------------

            measurements = get_measurements(
                sensor_id=sensor_id,
                date_from=date_from
            )

            print(
                f"Jumlah measurement: "
                f"{len(measurements)}"
            )

            # ----------------------------------------------------
            # LOOP DATA
            # ----------------------------------------------------

            for item in measurements:

                # ------------------------------------------------
                # VALUE
                # ------------------------------------------------

                value = item.get(
                    "value"
                )

                if value is None:
                    continue

                try:

                    value = float(
                        value
                    )

                except (
                    ValueError,
                    TypeError
                ):

                    continue

                # ------------------------------------------------
                # PERIOD
                # ------------------------------------------------

                period = item.get(
                    "period"
                ) or {}

                if not isinstance(
                    period,
                    dict
                ):
                    continue

                # ------------------------------------------------
                # DATETIME FROM
                # ------------------------------------------------

                datetime_from_data = (
                    period.get(
                        "datetimeFrom"
                    ) or {}
                )

                if not isinstance(
                    datetime_from_data,
                    dict
                ):
                    continue

                datetime_utc = (
                    datetime_from_data.get(
                        "utc"
                    )
                )

                datetime_local = (
                    datetime_from_data.get(
                        "local"
                    )
                )

                if datetime_utc is None:

                    continue

                # ------------------------------------------------
                # DATETIME TO
                # ------------------------------------------------

                datetime_to_data = (
                    period.get(
                        "datetimeTo"
                    ) or {}
                )

                if not isinstance(
                    datetime_to_data,
                    dict
                ):

                    datetime_to_data = {}

                datetime_to_utc = (
                    datetime_to_data.get(
                        "utc"
                    )
                )

                datetime_to_local = (
                    datetime_to_data.get(
                        "local"
                    )
                )

                # ------------------------------------------------
                # KOORDINAT
                # ------------------------------------------------

                coordinates = (
                    item.get(
                        "coordinates"
                    ) or {}
                )

                if not isinstance(
                    coordinates,
                    dict
                ):

                    coordinates = {}

                latitude = coordinates.get(
                    "latitude"
                )

                longitude = coordinates.get(
                    "longitude"
                )

                # Jika API tidak memberikan koordinat,
                # gunakan koordinat lokasi Malang.

                if latitude is None:

                    latitude = LATITUDE

                if longitude is None:

                    longitude = LONGITUDE

                # ------------------------------------------------
                # KONVERSI DATETIME KE MYSQL
                # ------------------------------------------------

                datetime_utc = (
                    to_mysql_datetime(
                        datetime_utc
                    )
                )

                datetime_local = (
                    to_mysql_datetime(
                        datetime_local
                    )
                )

                datetime_to_utc = (
                    to_mysql_datetime(
                        datetime_to_utc
                    )
                )

                datetime_to_local = (
                    to_mysql_datetime(
                        datetime_to_local
                    )
                )

                # datetime_utc wajib ada
                if datetime_utc is None:

                    continue

                # ------------------------------------------------
                # INSERT / UPDATE MYSQL
                # ------------------------------------------------

                cursor.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (

                        location_id,
                        latitude,
                        longitude,
                        sensor_id,
                        sensor_name,
                        parameter,
                        display_name,
                        unit,
                        value,
                        datetime_utc,
                        datetime_local,
                        datetime_to_utc,
                        datetime_to_local

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
                        %s,
                        %s

                    )

                    ON DUPLICATE KEY UPDATE

                        latitude =
                            VALUES(latitude),

                        longitude =
                            VALUES(longitude),

                        sensor_name =
                            VALUES(sensor_name),

                        parameter =
                            VALUES(parameter),

                        display_name =
                            VALUES(display_name),

                        unit =
                            VALUES(unit),

                        value =
                            VALUES(value),

                        datetime_local =
                            VALUES(datetime_local),

                        datetime_to_utc =
                            VALUES(datetime_to_utc),

                        datetime_to_local =
                            VALUES(datetime_to_local)

                    """,

                    (
                        str(
                            LOCATION_ID
                        ),

                        latitude,

                        longitude,

                        sensor_id,

                        sensor_name,

                        parameter_name,

                        parameter_display,

                        unit,

                        value,

                        datetime_utc,

                        datetime_local,

                        datetime_to_utc,

                        datetime_to_local
                    )
                )

                total_data += 1

        # --------------------------------------------------------
        # COMMIT
        # --------------------------------------------------------

        conn.commit()

        print()
        print("=" * 70)
        print("ETL OPENAQ SELESAI")
        print("=" * 70)

        print(
            f"Total data diproses: "
            f"{total_data}"
        )

    # ------------------------------------------------------------
    # ERROR
    # ------------------------------------------------------------

    except Exception as e:

        conn.rollback()

        print()
        print("=" * 70)
        print("ETL OPENAQ ERROR")
        print("=" * 70)

        print(
            f"Error: {e}"
        )

        print(
            "Transaksi database "
            "dibatalkan."
        )

        raise

    # ------------------------------------------------------------
    # CLOSE
    # ------------------------------------------------------------

    finally:

        cursor.close()

        conn.close()

        print(
            "Koneksi MySQL ditutup."
        )


# ================================================================
# AIRFLOW DAG
# ================================================================

default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "retries": 0

}


with DAG(

    dag_id="etl_openaq_mysql",

    default_args=default_args,

    description=(
        "ETL data PM2.5 OpenAQ "
        "ke MySQL"
    ),

    start_date=datetime(
        2026,
        1,
        1
    ),

    schedule="0 * * * *",

    catchup=False,

    max_active_runs=1,

    tags=[
        "ETL",
        "OpenAQ",
        "Malang",
        "MySQL"
    ]

) as dag:

    extract_transform_load_openaq = (
        PythonOperator(

            task_id=(
                "extract_transform_load_openaq"
            ),

            python_callable=etl_openaq

        )
    )
