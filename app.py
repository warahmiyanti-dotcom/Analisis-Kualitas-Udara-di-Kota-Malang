import os
from datetime import date

import mysql.connector
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ============================================================
# 1. KONFIGURASI HALAMAN
# ============================================================

st.set_page_config(
    page_title="Malang Air Quality Dashboard",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# 2. DESAIN UI/UX
# ============================================================

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    .stApp {
        background-color: #F5F7FB;
    }

    [data-testid="stSidebar"] {
        background-color: #102A43;
    }

    [data-testid="stSidebar"] * {
        color: #F0F4F8;
    }

    [data-testid="stSidebar"] [data-baseweb="select"] * {
        color: #172B4D;
    }

    [data-testid="stSidebar"] input {
        color: #172B4D;
    }

    .block-container {
        padding-top: 1.8rem;
        padding-bottom: 2rem;
        max-width: 1500px;
    }

    .hero {
        background: linear-gradient(120deg, #102A43 0%, #176B68 100%);
        padding: 30px 34px;
        border-radius: 20px;
        color: white;
        margin-bottom: 22px;
        box-shadow: 0 8px 24px rgba(16, 42, 67, 0.12);
    }

    .hero h1 {
        color: white;
        font-size: 31px;
        font-weight: 700;
        margin: 0 0 8px 0;
    }

    .hero p {
        color: #D9F3EE;
        font-size: 14px;
        margin: 0;
        line-height: 1.7;
    }

    .section-title {
        color: #102A43;
        font-size: 21px;
        font-weight: 700;
        margin: 12px 0 4px 0;
    }

    .section-subtitle {
        color: #627D98;
        font-size: 13px;
        margin-bottom: 16px;
    }

    .metric-card {
        background: white;
        border: 1px solid #E6ECF2;
        border-radius: 16px;
        padding: 20px;
        min-height: 125px;
        box-shadow: 0 3px 12px rgba(16, 42, 67, 0.035);
    }

    .metric-label {
        color: #627D98;
        font-size: 13px;
        font-weight: 600;
        margin-bottom: 9px;
    }

    .metric-value {
        color: #102A43;
        font-size: 29px;
        font-weight: 700;
        line-height: 1.2;
    }

    .metric-note {
        color: #829AB1;
        font-size: 11px;
        margin-top: 9px;
    }

    .panel {
        background: white;
        border: 1px solid #E6ECF2;
        border-radius: 16px;
        padding: 18px 20px;
        box-shadow: 0 3px 12px rgba(16, 42, 67, 0.035);
    }

    div[data-testid="stTabs"] button {
        font-weight: 600;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #E6ECF2;
        border-radius: 12px;
    }

    div.stButton > button,
    div.stDownloadButton > button {
        border-radius: 10px;
        font-weight: 600;
        min-height: 42px;
    }

    hr {
        border-color: #E6ECF2;
    }

    footer {
        visibility: hidden;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# 3. KONFIGURASI DATABASE
# ============================================================

MALANG_LAT = -7.9185093
MALANG_LON = 112.651344
RADIUS_KM = 25


def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3307")),
        user=os.getenv("MYSQL_USER", "malang"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "airflow_malang"),
        connection_timeout=10,
    )


def read_sql(query):
    connection = get_db_connection()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        return pd.DataFrame(rows)
    finally:
        connection.close()


# ============================================================
# 4. MEMUAT DATA MYSQL
# ============================================================

@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    air = read_sql("""
        SELECT *
        FROM air_quality
        ORDER BY datetime_local
    """)

    weather = read_sql("""
        SELECT *
        FROM weather
        ORDER BY datetime_utc
    """)

    hotspots = read_sql("""
        SELECT *
        FROM fire_hotspots
        ORDER BY datetime_local
    """)

    locations = read_sql("""
        SELECT location_id, location_name, latitude, longitude
        FROM locations
    """)

    for df, column in [
        (air, "datetime_local"),
        (weather, "datetime_utc"),
        (hotspots, "datetime_local"),
    ]:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column], errors="coerce"
            )

    for df, columns in [
        (air, ["latitude", "longitude", "value"]),
        (weather, [
            "latitude", "longitude", "temperature_c",
            "humidity_percent", "precipitation_mm",
            "pressure_hpa", "wind_speed_kmh",
            "wind_direction_degree", "cloud_cover_percent",
        ]),
        (hotspots, [
            "latitude", "longitude", "reference_latitude",
            "reference_longitude", "frp",
        ]),
        (locations, ["latitude", "longitude"]),
    ]:
        for column in columns:
            if column in df.columns:
                df[column] = pd.to_numeric(
                    df[column], errors="coerce"
                )

    return air, weather, hotspots, locations


# ============================================================
# 5. FUNGSI JARAK GEOGRAFIS
# ============================================================

def calculate_distance_km(lat, lon, ref_lat, ref_lon):
    """Menghitung jarak permukaan bumi dengan rumus Haversine."""
    earth_radius_km = 6371.0

    lat1 = np.radians(np.asarray(lat, dtype=float))
    lon1 = np.radians(np.asarray(lon, dtype=float))
    lat2 = np.radians(float(ref_lat))
    lon2 = np.radians(float(ref_lon))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1) * np.cos(lat2)
        * np.sin(dlon / 2) ** 2
    )

    a = np.clip(a, 0, 1)
    return 2 * earth_radius_km * np.arcsin(np.sqrt(a))


def filter_hotspots_radius(hotspots_data):
    result = hotspots_data.copy()

    if result.empty:
        result["jarak_km"] = pd.Series(dtype=float)
        return result

    required = ["latitude", "longitude"]
    if not all(column in result.columns for column in required):
        result["jarak_km"] = np.nan
        return result.iloc[0:0].copy()

    result = result.dropna(subset=required).copy()

    result["jarak_km"] = calculate_distance_km(
        result["latitude"].to_numpy(),
        result["longitude"].to_numpy(),
        MALANG_LAT,
        MALANG_LON,
    )

    return result[result["jarak_km"] <= RADIUS_KM].copy()


# ============================================================
# 6. FUNGSI TAMPILAN
# ============================================================

def section_title(title, subtitle=None):
    st.markdown(
        f'<div class="section-title">{title}</div>',
        unsafe_allow_html=True,
    )
    if subtitle:
        st.markdown(
            f'<div class="section-subtitle">{subtitle}</div>',
            unsafe_allow_html=True,
        )


def metric_card(label, value, note):
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_chart(fig, height=390):
    fig.update_layout(
        height=height,
        paper_bgcolor="white",
        plot_bgcolor="white",
        font=dict(
            family="DM Sans, sans-serif",
            color="#334E68",
            size=12,
        ),
        margin=dict(l=10, r=15, t=55, b=15),
        title=dict(
            font=dict(size=16, color="#102A43"),
            x=0.02,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        hovermode="closest",
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor="#EDF2F7",
        zeroline=False,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="#EDF2F7",
        zeroline=False,
    )
    return fig


def csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8-sig")


# ============================================================
# 7. HEADER DAN SIDEBAR
# ============================================================

st.markdown(
    """
    <div class="hero">
        <h1>🌿 Malang Air Quality Dashboard</h1>
        <p>
            Pusat pemantauan kualitas udara, kondisi cuaca, dan titik panas.
            Jelajahi data, amati tren, dan pantau kondisi di sekitar Malang.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("## 🌿 Malang Monitor")
    st.caption("AIR QUALITY · WEATHER · HOTSPOTS")
    st.divider()

    st.markdown("### ⚙️ Pengaturan")

    if st.button("↻ Muat ulang data", use_container_width=True):
        load_data.clear()
        st.rerun()

    st.caption("Cache data: maksimal 5 menit.")

    page = st.radio(
        "Navigasi",
        [
            "Ringkasan",
            "Kualitas Udara",
            "Cuaca",
            "Titik Panas",
            "Data & Unduhan",
        ],
        index=0,
    )

    st.divider()
    st.markdown("### 📍 Lokasi Acuan")
    st.write(f"Latitude: `{MALANG_LAT}`")
    st.write(f"Longitude: `{MALANG_LON}`")
    st.write(f"Radius titik panas: **{RADIUS_KM} km**")

    st.divider()
    st.caption("Sumber data: MySQL · OpenAQ · Open-Meteo · NASA FIRMS")


# ============================================================
# 8. AMBIL DATA
# ============================================================

try:
    with st.spinner("Mengambil data dari MySQL..."):
        air, weather, hotspots_all, locations = load_data()

except Exception as error:
    st.error(
        "Dashboard belum dapat terhubung ke MySQL. "
        "Periksa apakah container MySQL berjalan dan konfigurasi "
        "environment variable sudah benar."
    )
    st.code(str(error))
    st.stop()


# Filter NASA FIRMS berdasarkan jarak sesungguhnya
hotspots = filter_hotspots_radius(hotspots_all)


# Parameter dan filter kualitas udara
if not air.empty and "parameter" in air.columns:
    parameters = sorted(
        air["parameter"].dropna().astype(str).unique().tolist()
    )
else:
    parameters = []

with st.sidebar:
    st.markdown("### 🔎 Filter Data")

    selected_parameter = None
    if parameters:
        preferred = next(
            (
                parameter for parameter in parameters
                if parameter.lower().replace(".", "") in
                ["pm25", "pm2 5"]
            ),
            parameters[0],
        )
        selected_parameter = st.selectbox(
            "Parameter kualitas udara",
            parameters,
            index=parameters.index(preferred),
        )

    date_min = None
    date_max = None
    selected_dates = None

    if (
        not air.empty
        and "datetime_local" in air.columns
        and air["datetime_local"].notna().any()
    ):
        valid_dates = air["datetime_local"].dropna()
        date_min = valid_dates.min().date()
        date_max = valid_dates.max().date()

        selected_dates = st.date_input(
            "Rentang tanggal udara",
            value=(date_min, date_max),
            min_value=date_min,
            max_value=date_max,
        )


# Terapkan filter kualitas udara
filtered_air = air.copy()

if selected_parameter is not None:
    filtered_air = filtered_air[
        filtered_air["parameter"].astype(str) == selected_parameter
    ].copy()

if (
    selected_dates
    and isinstance(selected_dates, (tuple, list))
    and len(selected_dates) == 2
    and "datetime_local" in filtered_air.columns
):
    start_date, end_date = selected_dates
    filtered_air = filtered_air[
        filtered_air["datetime_local"].dt.date.between(
            start_date, end_date
        )
    ].copy()


# ============================================================
# 9. RINGKASAN GLOBAL
# ============================================================

latest_air = (
    air["datetime_local"].max()
    if not air.empty and "datetime_local" in air.columns
    else pd.NaT
)
latest_weather = (
    weather["datetime_utc"].max()
    if not weather.empty and "datetime_utc" in weather.columns
    else pd.NaT
)
latest_hotspot = (
    hotspots["datetime_local"].max()
    if not hotspots.empty and "datetime_local" in hotspots.columns
    else pd.NaT
)


def format_timestamp(value):
    if value is None:
        return "Belum tersedia"

    try:
        if pd.isna(value):
            return "Belum tersedia"

        return pd.Timestamp(value).strftime("%d %b %Y, %H:%M")

    except (ValueError, TypeError, OverflowError):
        return "Waktu tidak valid"


if page == "Ringkasan":
    section_title(
        "Ringkasan Pemantauan",
        "Gambaran umum data yang tersimpan di database.",
    )

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        metric_card(
            "DATA KUALITAS UDARA",
            f"{len(air):,}",
            "Total baris di MySQL",
        )
    with c2:
        metric_card(
            "DATA CUACA",
            f"{len(weather):,}",
            "Total baris di MySQL",
        )
    with c3:
        metric_card(
            "TITIK PANAS RADIUS 25 KM",
            f"{len(hotspots):,}",
            f"Dari {len(hotspots_all):,} baris FIRMS",
        )
    with c4:
        metric_card(
            "PARAMETER UDARA",
            f"{len(parameters):,}",
            "Parameter yang tersedia",
        )

    st.write("")

    left, right = st.columns([1.55, 1])

    with left:
        section_title("📈 Tren Kualitas Udara")

        if (
            not filtered_air.empty
            and {"datetime_local", "value"}.issubset(filtered_air.columns)
        ):
            chart = filtered_air.dropna(
                subset=["datetime_local", "value"]
            ).sort_values("datetime_local")

            fig = px.line(
                chart,
                x="datetime_local",
                y="value",
                title=f"Tren {selected_parameter or 'kualitas udara'}",
                labels={
                    "datetime_local": "Waktu lokal",
                    "value": "Nilai pengukuran",
                },
                markers=False,
            )
            fig.update_traces(
                line=dict(color="#167D7F", width=2.5)
            )
            st.plotly_chart(
                style_chart(fig, 370),
                use_container_width=True,
            )
        else:
            st.info("Belum ada data kualitas udara yang sesuai filter.")

    with right:
        section_title(
            "🕒 Pembaruan Data",
            "Waktu data terbaru yang tersimpan di database.",
        )

        with st.container(border=True):
            st.markdown("**🌫️ Kualitas Udara**")
            st.write(format_timestamp(latest_air))

            st.divider()

            st.markdown("**🌦️ Cuaca (UTC)**")
            st.write(format_timestamp(latest_weather))

            st.divider()

            st.markdown("**🔥 Titik Panas (waktu lokal)**")
            st.write(format_timestamp(latest_hotspot))

    st.write("")
    section_title(
        "🔥 Titik Panas Sekitar Malang",
        "Peta interaktif dengan radius geografis maksimal 25 km.",
    )

    if not hotspots.empty:
        map_data = hotspots.dropna(
            subset=["latitude", "longitude"]
        ).copy()

        hover_columns = [
            column for column in [
                "datetime_local", "jarak_km", "satellite",
                "confidence", "frp",
            ] if column in map_data.columns
        ]

        fig_map = px.scatter_map(
            map_data,
            lat="latitude",
            lon="longitude",
            color="frp" if "frp" in map_data.columns else None,
            size="frp" if "frp" in map_data.columns else None,
            hover_name="datetime_local"
            if "datetime_local" in map_data.columns else None,
            hover_data=hover_columns,
            color_continuous_scale="OrRd",
            size_max=13,
            zoom=9,
            center={"lat": MALANG_LAT, "lon": MALANG_LON},
            map_style="open-street-map",
            title="Distribusi titik panas dalam radius 25 km",
        )
        fig_map.update_layout(height=500, margin=dict(l=0, r=0, t=50, b=0))
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Belum ditemukan titik panas dalam radius 25 km.")

    st.caption(
        "Catatan: jumlah titik panas adalah jumlah baris deteksi, "
        "bukan jumlah kebakaran unik. Satu kejadian dapat menghasilkan "
        "beberapa deteksi satelit."
    )


# ============================================================
# 10. HALAMAN KUALITAS UDARA
# ============================================================

elif page == "Kualitas Udara":
    section_title(
        "Kualitas Udara",
        "Analisis nilai pengukuran berdasarkan parameter dan waktu.",
    )

    if filtered_air.empty:
        st.info("Tidak ada data untuk parameter atau rentang tanggal ini.")
    else:
        chart_air = filtered_air.dropna(
            subset=["datetime_local", "value"]
        ).sort_values("datetime_local").copy()

        if not chart_air.empty:
            c1, c2, c3 = st.columns(3)

            values = pd.to_numeric(
                chart_air["value"], errors="coerce"
            ).dropna()

            with c1:
                metric_card(
                    "RATA-RATA",
                    f"{values.mean():.2f}" if not values.empty else "—",
                    "Nilai pengukuran",
                )
            with c2:
                metric_card(
                    "MEDIAN",
                    f"{values.median():.2f}" if not values.empty else "—",
                    "Nilai tengah",
                )
            with c3:
                metric_card(
                    "NILAI MAKSIMUM",
                    f"{values.max():.2f}" if not values.empty else "—",
                    "Nilai tertinggi",
                )

            fig = px.line(
                chart_air,
                x="datetime_local",
                y="value",
                title=f"Perubahan {selected_parameter}",
                labels={
                    "datetime_local": "Waktu lokal",
                    "value": "Nilai pengukuran",
                },
            )
            fig.update_traces(
                line=dict(color="#167D7F", width=2.5)
            )
            st.plotly_chart(
                style_chart(fig, 430),
                use_container_width=True,
            )

            st.markdown("#### Data pengukuran")
            st.dataframe(
                chart_air,
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "⬇️ Unduh data kualitas udara CSV",
                data=csv_bytes(chart_air),
                file_name="kualitas_udara_malang.csv",
                mime="text/csv",
            )


# ============================================================
# 11. HALAMAN CUACA
# ============================================================

elif page == "Cuaca":
    section_title(
        "Kondisi Cuaca",
        "Jelajahi perubahan variabel cuaca berdasarkan waktu UTC.",
    )

    weather_options = {
        "Suhu": ("temperature_c", "°C"),
        "Kelembapan": ("humidity_percent", "%"),
        "Curah hujan": ("precipitation_mm", "mm"),
        "Tekanan udara": ("pressure_hpa", "hPa"),
        "Kecepatan angin": ("wind_speed_kmh", "km/jam"),
        "Arah angin": ("wind_direction_degree", "derajat"),
        "Tutupan awan": ("cloud_cover_percent", "%"),
    }

    available_weather = {
        label: values
        for label, values in weather_options.items()
        if values[0] in weather.columns
    }

    if weather.empty or not available_weather:
        st.info("Data cuaca belum tersedia.")
    else:
        selected_weather = st.selectbox(
            "Variabel cuaca",
            list(available_weather.keys()),
        )

        weather_column, unit = available_weather[selected_weather]
        chart_weather = weather.dropna(
            subset=["datetime_utc", weather_column]
        ).copy()

        chart_weather[weather_column] = pd.to_numeric(
            chart_weather[weather_column], errors="coerce"
        )
        chart_weather = chart_weather.dropna(
            subset=[weather_column]
        ).sort_values("datetime_utc")

        if not chart_weather.empty:
            values = chart_weather[weather_column]

            c1, c2, c3 = st.columns(3)
            with c1:
                metric_card(
                    "RATA-RATA",
                    f"{values.mean():.2f} {unit}",
                    selected_weather,
                )
            with c2:
                metric_card(
                    "MINIMUM",
                    f"{values.min():.2f} {unit}",
                    selected_weather,
                )
            with c3:
                metric_card(
                    "MAKSIMUM",
                    f"{values.max():.2f} {unit}",
                    selected_weather,
                )

            fig = px.line(
                chart_weather,
                x="datetime_utc",
                y=weather_column,
                title=f"Tren {selected_weather}",
                labels={
                    "datetime_utc": "Waktu UTC",
                    weather_column: f"{selected_weather} ({unit})",
                },
            )
            fig.update_traces(
                line=dict(color="#3B82A0", width=2.5)
            )
            st.plotly_chart(
                style_chart(fig, 430),
                use_container_width=True,
            )

            st.markdown("#### Data cuaca")
            st.dataframe(
                chart_weather,
                use_container_width=True,
                hide_index=True,
            )

            st.download_button(
                "⬇️ Unduh data cuaca CSV",
                data=csv_bytes(chart_weather),
                file_name="cuaca_malang.csv",
                mime="text/csv",
            )


# ============================================================
# 12. HALAMAN TITIK PANAS
# ============================================================

elif page == "Titik Panas":
    section_title(
        "Pemantauan Titik Panas NASA FIRMS",
        "Hanya deteksi dengan jarak maksimum 25 km dari koordinat acuan Malang.",
    )

    c1, c2, c3 = st.columns(3)

    with c1:
        metric_card(
            "DETEKSI DALAM RADIUS",
            f"{len(hotspots):,}",
            "Maksimal 25 km",
        )
    with c2:
        metric_card(
            "SEMUA DETEKSI MYSQL",
            f"{len(hotspots_all):,}",
            "Tanpa filter radius",
        )
    with c3:
        metric_card(
            "RADIUS",
            "25 km",
            "Dari titik acuan Malang",
        )

    if not hotspots.empty:
        map_data = hotspots.dropna(
            subset=["latitude", "longitude"]
        ).copy()

        st.markdown("#### 🗺️ Peta interaktif")

        color_column = (
            "frp" if "frp" in map_data.columns else None
        )

        fig = px.scatter_map(
            map_data,
            lat="latitude",
            lon="longitude",
            color=color_column,
            size=color_column,
            hover_data=[
                column for column in [
                    "datetime_local", "jarak_km",
                    "satellite", "confidence", "frp",
                ] if column in map_data.columns
            ],
            color_continuous_scale="OrRd",
            size_max=14,
            zoom=9,
            center={"lat": MALANG_LAT, "lon": MALANG_LON},
            map_style="open-street-map",
            title="Titik panas dalam radius 25 km",
        )
        fig.update_layout(height=560, margin=dict(l=0, r=0, t=50, b=0))
        st.plotly_chart(fig, use_container_width=True)

        if "datetime_local" in map_data.columns:
            daily = map_data.dropna(
                subset=["datetime_local"]
            ).copy()
            daily["tanggal"] = daily["datetime_local"].dt.date

            daily_counts = (
                daily.groupby("tanggal")
                .size()
                .reset_index(name="jumlah_deteksi")
                .sort_values("tanggal")
            )

            if not daily_counts.empty:
                fig_daily = px.bar(
                    daily_counts,
                    x="tanggal",
                    y="jumlah_deteksi",
                    title="Jumlah deteksi titik panas per hari",
                    labels={
                        "tanggal": "Tanggal",
                        "jumlah_deteksi": "Jumlah deteksi",
                    },
                )
                fig_daily.update_traces(marker_color="#D97745")
                st.plotly_chart(
                    style_chart(fig_daily, 340),
                    use_container_width=True,
                )

        st.markdown("#### 📋 Rincian deteksi")

        columns = [
            column for column in [
                "datetime_local",
                "latitude",
                "longitude",
                "jarak_km",
                "satellite",
                "confidence",
                "frp",
                "daynight",
            ] if column in hotspots.columns
        ]

        sorted_hotspots = hotspots.sort_values(
            "jarak_km", ascending=True
        )

        st.dataframe(
            sorted_hotspots[columns],
            use_container_width=True,
            hide_index=True,
        )

        st.download_button(
            "⬇️ Unduh titik panas radius 25 km",
            data=csv_bytes(sorted_hotspots),
            file_name="titik_panas_radius_25km.csv",
            mime="text/csv",
        )

        st.caption(
            "Jarak dihitung dari koordinat acuan Malang menggunakan "
            "rumus Haversine. Radius ini bukan batas administratif. "
            "Jumlah baris menunjukkan deteksi, bukan kejadian kebakaran unik."
        )
    else:
        st.info(
            "Tidak ada titik panas dalam radius 25 km pada data yang tersedia."
        )


# ============================================================
# 13. HALAMAN DATA DAN UNDUHAN
# ============================================================

elif page == "Data & Unduhan":
    section_title(
        "Data & Unduhan",
        "Lihat tabel sumber dan unduh data untuk analisis lanjutan.",
    )

    tab_air, tab_weather, tab_hotspots, tab_locations = st.tabs(
        [
            "Kualitas Udara",
            "Cuaca",
            "Titik Panas 25 km",
            "Lokasi",
        ]
    )

    with tab_air:
        st.write(f"Total baris: **{len(air):,}**")
        st.dataframe(
            filtered_air,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Unduh kualitas udara CSV",
            data=csv_bytes(filtered_air),
            file_name="kualitas_udara_malang.csv",
            mime="text/csv",
            key="download_air",
        )

    with tab_weather:
        st.write(f"Total baris: **{len(weather):,}**")
        st.dataframe(
            weather,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Unduh cuaca CSV",
            data=csv_bytes(weather),
            file_name="cuaca_malang.csv",
            mime="text/csv",
            key="download_weather",
        )

    with tab_hotspots:
        st.write(
            f"Deteksi dalam radius 25 km: **{len(hotspots):,}** "
            f"dari **{len(hotspots_all):,}** deteksi total."
        )
        st.dataframe(
            hotspots.sort_values("jarak_km")
            if not hotspots.empty else hotspots,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Unduh titik panas CSV",
            data=csv_bytes(hotspots),
            file_name="titik_panas_radius_25km.csv",
            mime="text/csv",
            key="download_hotspots",
        )

    with tab_locations:
        st.write(f"Total lokasi: **{len(locations):,}**")
        st.dataframe(
            locations,
            use_container_width=True,
            hide_index=True,
        )
        st.download_button(
            "Unduh lokasi CSV",
            data=csv_bytes(locations),
            file_name="lokasi_malang.csv",
            mime="text/csv",
            key="download_locations",
        )


# ============================================================
# 14. FOOTER
# ============================================================

st.divider()
st.markdown(
    """
    <div style="text-align:center; color:#829AB1; font-size:12px;">
        <b>Malang Air Quality Dashboard</b><br>
        Pemantauan kualitas udara · Cuaca · Titik panas<br>
        Data diperbarui mengikuti jadwal ETL dan ketersediaan API sumber.
    </div>
    """,
    unsafe_allow_html=True,
)
