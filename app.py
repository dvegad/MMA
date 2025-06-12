# app.py

import os
from dotenv import load_dotenv

import pandas as pd
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine
from shapely.affinity import rotate

# =============================================================================
# 1. CONFIGURACIÓN Y CARGA DE DATOS
# =============================================================================

# ── 1.1 Cargar variables de entorno (archivo .env con credenciales de BD)
load_dotenv(dotenv_path=".env")  # Asegúrate de que .env esté en la misma carpeta que app.py

DB_USER = os.getenv("PGUSER")
DB_PASS = os.getenv("PGPASSWORD")
DB_HOST = os.getenv("PGHOST")
DB_PORT = os.getenv("PGPORT")
DB_NAME = os.getenv("PGDATABASE")

# Crear el motor de SQLAlchemy
ENGINE = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── 1.2 Función para cargar todos los datos de emisiones (2005–2023), con caching
@st.cache_data(ttl=3600)
def load_emissions_data() -> pd.DataFrame:
    """
    Carga desde la base de datos todos los registros de air_ps.emissions_hist
    entre 2005 y 2023, aplicando la normalización de 'region_norm'.

    Retorna:
        df (pd.DataFrame) con columnas:
            - period (int)
            - region_norm (str)
            - id_vu (text)
            - nombre_establecimiento (text)
            - rubro_vu (text)
            - contaminantes (text)
            - cantidad_toneladas (numeric)
    """
    query = """
    SELECT
        CAST(eh.period AS INTEGER) AS period,
        CASE
            WHEN TRIM(eh.region) ILIKE 'Metropolitana%' THEN 'Metropolitana de Santiago'
            WHEN TRIM(eh.region) ILIKE 'Araucanía' THEN 'La Araucanía'
            WHEN TRIM(eh.region) ILIKE 'Ñuble' THEN 'Ñuble'
            WHEN TRIM(eh.region) ILIKE 'Coquimbo' THEN 'Coquimbo'
            WHEN TRIM(eh.region) ILIKE 'Tarapacá' THEN 'Tarapacá'
            WHEN TRIM(eh.region) ILIKE 'Antofagasta' THEN 'Antofagasta'
            WHEN TRIM(eh.region) ILIKE 'La Araucanía' THEN 'La Araucanía'
            WHEN TRIM(eh.region) ILIKE 'Los Lagos' THEN 'Los Lagos'
            WHEN TRIM(eh.region) ILIKE 'Los Ríos' THEN 'Los Ríos'
            WHEN TRIM(eh.region) ILIKE 'Atacama' THEN 'Atacama'
            WHEN TRIM(eh.region) ILIKE 'Biobío' THEN 'Biobío'
            WHEN TRIM(eh.region) ILIKE 'Maule' THEN 'Maule'
            WHEN TRIM(eh.region) ILIKE 'Arica y Parinacota' THEN 'Arica y Parinacota'
            WHEN TRIM(eh.region) ILIKE 'Valparaíso' THEN 'Valparaíso'
            WHEN TRIM(eh.region) ILIKE 'Magallanes y de la Antártica Chilena' THEN 'Magallanes y de la Antártica Chilena'
            WHEN TRIM(eh.region) ILIKE 'Magallanes' THEN 'Magallanes y de la Antártica Chilena'
            WHEN TRIM(eh.region) ILIKE 'O''Higgins' THEN 'Libertador General Bernardo O''Higgins'
            WHEN TRIM(eh.region) ILIKE 'Libertador Gral. Bernardo O''Higgins' THEN 'Libertador General Bernardo O''Higgins'
            WHEN TRIM(eh.region) ILIKE 'Aysén del Gral. Carlos Ibáñez del Campo' THEN 'Aysén del General Carlos Ibáñez del Campo'
            WHEN TRIM(eh.region) ILIKE 'Aysén del General Carlos Ibáñez del Campo' THEN 'Aysén del General Carlos Ibáñez del Campo'
            ELSE TRIM(eh.region)
        END AS region_norm,
        eh.id_vu,
        eh.nombre_establecimiento,
        eh.rubro_vu,
        eh.contaminantes,
        eh.cantidad_toneladas AS emisiones
    FROM air_ps.emissions_hist AS eh
    WHERE eh.period BETWEEN 2005 AND 2023
      AND eh.cantidad_toneladas IS NOT NULL
      AND eh.region IS NOT NULL
      AND eh.rubro_vu IS NOT NULL
    ;
    """
    df = pd.read_sql(query, con=ENGINE)
    # Asegurar tipos
    df["period"] = df["period"].astype(int)
    df["emisiones"] = pd.to_numeric(df["emisiones"], errors="coerce")
    return df


# ── 1.3 Cargar el GeoJSON simplificado de regiones y normalizar nombres
@st.cache_data(ttl=86400)
def load_regions_geojson(path_geojson: str) -> gpd.GeoDataFrame:
    """
    Carga el GeoJSON simplificado de regiones, normaliza la columna 'Region'
    y añade 'region_norm' para coincidir con df_aireconsolidado.

    Parámetros:
        path_geojson (str): Ruta al archivo Regional.geojson

    Retorna:
        gdf_regiones (GeoDataFrame) con columnas:
            - REGION   (original)
            - region_norm (normalizado)
            - geometry
    """
    gdf = gpd.read_file(path_geojson)

    # Diccionario de normalización
    mapa_norm = {
        'Región Metropolitana de Santiago': 'Metropolitana de Santiago',
        'Región de La Araucanía': 'La Araucanía',
        'Región de Ñuble': 'Ñuble',
        'Región de Coquimbo': 'Coquimbo',
        'Región de Tarapacá': 'Tarapacá',
        'Región de Antofagasta': 'Antofagasta',
        'Región de Los Lagos': 'Los Lagos',
        'Región de Los Ríos': 'Los Ríos',
        'Región de Atacama': 'Atacama',
        'Región del Bío-Bío': 'Biobío',
        'Región del Maule': 'Maule',
        'Región de Arica y Parinacota': 'Arica y Parinacota',
        'Región de Valparaíso': 'Valparaíso',
        'Región de Magallanes y Antártica Chilena': 'Magallanes y de la Antártica Chilena',
        'Región de Aysén del Gral.Ibañez del Campo': 'Aysén del General Carlos Ibáñez del Campo',
        "Región del Libertador Bernardo O'Higgins": "Libertador General Bernardo O'Higgins",
        'Zona sin demarcar': 'Sin demarcar'
    }
    gdf["region_norm"] = gdf["Region"].map(mapa_norm)
    return gdf


# =============================================================================
# 2. UTILIDADES COMUNES
# =============================================================================

def filtrar_emisiones(
    df: pd.DataFrame,
    rubro: str | None = None,
    contaminante: str | None = None,
    region: str | None = None,
    anio_min: int | None = None,
    anio_max: int | None = None
) -> pd.DataFrame:
    """
    Devuelve un sub-DataFrame de emisiones filtrado según los parámetros dados.
    Cualquier parámetro que sea None o 'Todos' no se aplica.

    Parámetros:
        df            : pd.DataFrame original con datos de emisiones
        rubro         : filtro de rubro_vu
        contaminante  : filtro de contaminantes
        region        : filtro de region_norm
        anio_min,anio_max : rango de años para filtrar period

    Retorna:
        pd.DataFrame filtrado
    """
    df2 = df
    if rubro and rubro != "Todos":
        df2 = df2[df2["rubro_vu"] == rubro]
    if contaminante and contaminante != "Todos":
        df2 = df2[df2["contaminantes"] == contaminante]
    if region and region != "Todos":
        df2 = df2[df2["region_norm"] == region]
    if anio_min is not None and anio_max is not None:
        df2 = df2[(df2["period"] >= anio_min) & (df2["period"] <= anio_max)]
    return df2


# =============================================================================
# 3. INTERFAZ STREAMLIT
# =============================================================================

st.set_page_config(
    page_title="Indicadores Globales de Emisiones",
    layout="wide"
)

st.title("📊 Indicadores Globales de Emisiones de Contaminantes (2005–2023)")
st.write(
    "Este dashboard muestra distintos gráficos interactivos sobre emisiones "
    "de contaminantes en distintas regiones de Chile, usando datos de la base RETC."
)

# ── 3.1 Carga de datos (una sola vez, con caching)
with st.spinner("Cargando datos de emisiones desde la base..."):
    df_emisiones = load_emissions_data()

# ── 3.2 Definir filtros en la barra lateral
st.sidebar.header("Filtros Generales")

# 3.2.1 Rango de años
anio_min, anio_max = st.sidebar.slider(
    label="Rango de Años",
    min_value=int(df_emisiones["period"].min()),
    max_value=int(df_emisiones["period"].max()),
    value=(2014, 2023),
    step=1
)

# 3.2.2 Rubro
rubros = ["Todos"] + sorted(df_emisiones["rubro_vu"].dropna().unique().tolist())
rubro_sel = st.sidebar.selectbox("Rubro", options=rubros)

# 3.2.3 Contaminante
contaminantes = ["Todos"] + sorted(df_emisiones["contaminantes"].dropna().unique().tolist())
contaminante_sel = st.sidebar.selectbox("Contaminante", options=contaminantes)

# 3.2.4 Región
regiones = ["Todos"] + sorted(df_emisiones["region_norm"].dropna().unique().tolist())
region_sel = st.sidebar.selectbox("Región", options=regiones)

# Filtrar DataFrame base según todo
df_filtrado = filtrar_emisiones(
    df_emisiones,
    rubro=rubro_sel,
    contaminante=contaminante_sel,
    region=region_sel,
    anio_min=anio_min,
    anio_max=anio_max
)

# ── 3.3 Panel principal con tabs para cada sección
tabs = st.tabs(
    [
        "1️⃣ Emisiones por Región",
        "2️⃣ Emisiones por Rubro",
        "3️⃣ Heatmap Región vs Año",
        "4️⃣ Mapa Coroplético"
    ]
)

# =============================================================================
# 4. GRÁFICO 1: Emisiones por Región (barras horizontales con Top 10 est.)
# =============================================================================
with tabs[0]:
    st.subheader("Emisiones acumuladas por Región")
    if df_filtrado.empty:
        st.warning("No hay datos para los filtros seleccionados.")
    else:
        # 4.1 Calcular totales por región
        emis_region = (
            df_filtrado.groupby("region_norm")["emisiones"]
            .sum()
            .sort_values(ascending=True)
            .reset_index()
        )
        emis_region["region_norm"] = pd.Categorical(
            emis_region["region_norm"],
            categories=emis_region["region_norm"],
            ordered=True
        )

        # 4.2 Construir Top10 establecimientos por región
        top10_list = []
        for region in emis_region["region_norm"]:
            df_r = df_filtrado[df_filtrado["region_norm"] == region]
            top10 = (
                df_r.groupby(["id_vu", "nombre_establecimiento"])["emisiones"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
            lista_html = []
            for (id_vu, nombre), val in top10.items():
                lista_html.append(f"{id_vu} – {nombre} ({val/1e6:.2f} M)")
            top10_list.append("<br>".join(lista_html))

        emis_region["Top10_Establecimientos"] = top10_list

        # 4.3 Graficar con Plotly Express
        fig1 = px.bar(
            emis_region,
            x="emisiones",
            y="region_norm",
            orientation="h",
            labels={"emisiones": "Toneladas", "region_norm": "Región"},
            title=f"Emisiones acumuladas por Región ({anio_min}–{anio_max})"
        )
        fig1.update_traces(
            hovertemplate=(
                "<b>Región:</b> %{y}<br>"
                "<b>Emisiones:</b> %{x:,.0f} Ton<br><br>"
                "<b>Top 10 Establecimientos:</b><br>%{customdata}<extra></extra>"
            ),
            customdata=emis_region[["Top10_Establecimientos"]].values
        )
        fig1.update_layout(
            template="simple_white",
            xaxis_title="Emisiones (Toneladas)",
            yaxis_title="Región",
            hoverlabel_align="left"
        )
        st.plotly_chart(fig1, use_container_width=True)

# =============================================================================
# 5. GRÁFICO 2: Emisiones por Rubro (barras horizontales con Top 10 est.)
# =============================================================================
with tabs[1]:
    st.subheader("Emisiones acumuladas por Rubro VU")
    if df_filtrado.empty:
        st.warning("No hay datos para los filtros seleccionados.")
    else:
        emis_rubro = (
            df_filtrado.groupby("rubro_vu")["emisiones"]
            .sum()
            .sort_values(ascending=True)
            .reset_index()
        )
        emis_rubro["rubro_vu"] = pd.Categorical(
            emis_rubro["rubro_vu"],
            categories=emis_rubro["rubro_vu"],
            ordered=True
        )

        # Top 10 establecimientos por rubro
        top10_rubro = []
        for rubro in emis_rubro["rubro_vu"]:
            df_r = df_filtrado[df_filtrado["rubro_vu"] == rubro]
            top10 = (
                df_r.groupby(["id_vu", "nombre_establecimiento"])["emisiones"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
            lista_html = []
            for (id_vu, nombre), val in top10.items():
                lista_html.append(f"{id_vu} – {nombre} ({val/1e6:.2f} M)")
            top10_rubro.append("<br>".join(lista_html))

        emis_rubro["Top10_Establecimientos"] = top10_rubro

        fig2 = px.bar(
            emis_rubro,
            x="emisiones",
            y="rubro_vu",
            orientation="h",
            labels={"emisiones": "Toneladas", "rubro_vu": "Rubro VU"},
            title=f"Emisiones acumuladas por Rubro VU ({anio_min}–{anio_max})"
        )
        fig2.update_traces(
            hovertemplate=(
                "<b>Rubro:</b> %{y}<br>"
                "<b>Emisiones:</b> %{x:,.0f} Ton<br><br>"
                "<b>Top 10 Establecimientos:</b><br>%{customdata}<extra></extra>"
            ),
            customdata=emis_rubro[["Top10_Establecimientos"]].values
        )
        fig2.update_layout(
            template="simple_white",
            xaxis_title="Emisiones (Toneladas)",
            yaxis_title="Rubro VU",
            hoverlabel_align="left",
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig2, use_container_width=True)

# =============================================================================
# 6. GRÁFICO 3: Heatmap de Emisiones (Región vs Año), con texto dentro de cada celda
# =============================================================================
with tabs[2]:
    st.subheader("Heatmap de Emisiones por Región vs Año")
    if df_filtrado.empty:
        st.warning("No hay datos para los filtros seleccionados.")
    else:
        pivot = (
            df_filtrado.groupby(["region_norm", "period"])["emisiones"]
            .sum()
            .unstack(fill_value=0)
        )
        regions = pivot.index.tolist()
        years = pivot.columns.tolist()
        z_vals = pivot.values  # matriz de emisiones en toneladas

        # Construir matriz de textos para cada celda: M/k/unidades
        text_vals = []
        for fila in z_vals:
            fila_text = []
            for val in fila:
                if val >= 1_000_000:
                    fila_text.append(f"{val/1e6:.1f}M")
                elif val >= 1_000:
                    fila_text.append(f"{int(val/1e3)}k")
                else:
                    fila_text.append(str(int(val)))
            text_vals.append(fila_text)

        # Construir hovertext con Emisiones formateadas y top 10 establecimientos
        hovertext = []
        for i, region in enumerate(regions):
            fila_hover = []
            for j, year in enumerate(years):
                emis = z_vals[i][j]
                # Formato condicional para hover
                if emis >= 1_000_000:
                    emis_str = f"{emis/1e6:.1f}M Ton"
                elif emis >= 1_000:
                    emis_str = f"{emis/1e3:.0f}k Ton"
                else:
                    emis_str = f"{emis:.0f} Ton"

                df_r_a = df_filtrado[
                    (df_filtrado["region_norm"] == region) &
                    (df_filtrado["period"] == year)
                ]
                if df_r_a.empty:
                    top10_str = "Sin datos"
                else:
                    top10 = (
                        df_r_a.groupby(["id_vu", "nombre_establecimiento"])["emisiones"]
                        .sum()
                        .sort_values(ascending=False)
                        .head(10)
                    )
                    lista = []
                    for (id_vu, nombre), val_est in top10.items():
                        lista.append(f"{id_vu} – {nombre} ({val_est/1e6:.2f} M)")
                    top10_str = "<br>".join(lista)

                html = (
                    f"<b>Región:</b> {region}<br>"
                    f"<b>Año:</b> {year}<br>"
                    f"<b>Emisiones:</b> {emis_str}<br><br>"
                    f"<b>Top 10 Establecimientos:</b><br>{top10_str}"
                )
                fila_hover.append(html)
            hovertext.append(fila_hover)

        fig3 = go.Figure(
            go.Heatmap(
                z=z_vals,
                x=[str(y) for y in years],
                y=regions,
                colorscale="YlOrRd",
                colorbar=dict(title="Toneladas emitidas"),
                text=text_vals,
                texttemplate="%{text}",
                textfont={"size": 12, "color": "black"},
                hoverinfo="text",
                hovertext=hovertext
            )
        )
        fig3.update_layout(
            title=f"Heatmap de Emisiones (Región vs Año) ({anio_min}–{anio_max})",
            xaxis_title="Año",
            yaxis_title="Región",
            template="simple_white"
        )
        st.plotly_chart(fig3, use_container_width=True)

# =============================================================================
# 7. GRÁFICO 4: Mapa Coroplético de Emisiones por Región (rotado)
# =============================================================================
with tabs[3]:
    st.subheader("Mapa Coroplético de Emisiones por Región")
    if df_filtrado.empty:
        st.warning("No hay datos para los filtros seleccionados.")
    else:
        # 7.1) Cargar el GeoJSON simplificado de regiones (cached)
        ruta_geojson = "Regional.geojson"  # Ajusta la ruta si es necesario
        gdf_regiones = load_regions_geojson(ruta_geojson)

        # 7.2) Rotar geometrías 90° antihorario para que el norte quede a la izquierda
        #      Se calcula un centro aproximado
        minx, miny, maxx, maxy = gdf_regiones.total_bounds
        centrox = (minx + maxx) / 2
        centroy = (miny + maxy) / 2
        gdf_regiones["geometry"] = gdf_regiones["geometry"].apply(
            lambda geom: rotate(geom, 90, origin=(centrox, centroy))
        )

        # 7.3) Agrupar emisiones totales por región_norm
        emis_region_total = (
            df_filtrado.groupby("region_norm")["emisiones"]
            .sum()
            .reset_index()
        )
        emis_region_total["REGION_UP"] = emis_region_total["region_norm"].str.upper()
        gdf_regiones["REGION_UP"] = gdf_regiones["region_norm"].str.upper()

        # 7.4) Merge para unir geometría con emisiones
        geo_merged = gdf_regiones.merge(
            emis_region_total, on="REGION_UP", how="left"
        ).fillna({"emisiones": 0})

        # 7.5) Convertir a GeoJSON en memoria
        geojson_simp = geo_merged.to_json()

        # 7.6) Construir choropleth_mapbox
        fig4 = px.choropleth_mapbox(
            geo_merged,
            geojson=geojson_simp,
            locations="REGION_UP",
            featureidkey="properties.REGION_UP",
            color="emisiones",
            hover_name="Region",
            hover_data={"emisiones": ":,.0f"},
            title=f"Emisiones acumuladas por Región ({anio_min}–{anio_max}) — Rotado",
            labels={"emisiones": "Toneladas emitidas"},
            mapbox_style="carto-positron",
            center={"lon": centrox, "lat": centroy},
            zoom=3.5,
        )
        fig4.update_layout(
            margin={"l": 0, "r": 0, "t": 50, "b": 0},
            template="simple_white"
        )
        st.plotly_chart(fig4, use_container_width=True)
