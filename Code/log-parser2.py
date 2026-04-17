import asyncio
import re
from pathlib import Path
from datetime import datetime
 
import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
 
# ============================================================
# Configuración
# ============================================================
 
LIVE_LOG_FILE = "cu-lan-ho.log"
AGG_PERIOD_SEC = 1
PARQUET_PERIOD_SEC = 30
DASH_PORT = 8059
 
# ============================================================
# Regex
# ============================================================
 
# Línea SDAP DL con volumen
SDAP_DL_REGEX = re.compile(

    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}).*?"

    r"\[SDAP\s*\].*?"

    r"ue=(?P<ue>\d+).*?"

    r"DL.*?"

    r"pdu_len=(?P<pdu_len>\d+)",

    re.IGNORECASE,

)
 
 
# Línea de metadatos tipo:
# [CU-UEMNG] ... ue=2 ... plmn=21405 ... pci=802 ... rnti=0x4605
UE_METADATA_REGEX = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}).*?"
    r"\[CU-UEMNG\].*?"
    r"ue=(?P<ue>\d+).*?"
    r"plmn=(?P<plmn>\d+).*?"
    r"pci=(?P<pci>\d+).*?"
    r"rnti=(?P<rnti>0x[0-9a-fA-F]+|\d+)",
    re.IGNORECASE,
)
 
# Regex más flexibles, por si aparecen metadatos parciales
UE_ID_REGEX = re.compile(r"\bue=(?P<ue>\d+)\b", re.IGNORECASE)
PLMN_REGEX = re.compile(
    r"\bplmn=(?P<plmn>\d{5,6})\b|"
    r"\bmcc=(?P<mcc>\d{3}).*?\bmnc=(?P<mnc>\d{2,3})",
    re.IGNORECASE,
)
PCI_REGEX = re.compile(r"\bpci=(?P<pci>\d+)\b", re.IGNORECASE)
RNTI_REGEX = re.compile(r"\b(?:c-rnti|rnti)=(?P<rnti>0x[0-9a-fA-F]+|\d+)\b", re.IGNORECASE)
 
# ============================================================
# Estado compartido
# ============================================================
 
queue = asyncio.Queue()
 
raw_df = pl.DataFrame(
    schema={
        "ts": pl.Datetime,
        "ue": pl.Int64,
        "pdu_len": pl.Int64,
    }
)
 
agg_df = pl.DataFrame(
    schema={
        "bucket_ts": pl.Datetime,
        "ue": pl.Int64,
        "bytes_sum": pl.Int64,
        "plmn": pl.Utf8,
        "rnti": pl.Utf8,
        "pci": pl.Int64,
    }
)
 
latest_plot_df = pl.DataFrame(
    schema={
        "bucket_ts": pl.Datetime,
        "ue": pl.Int64,
        "bytes_sum": pl.Int64,
        "plmn": pl.Utf8,
        "rnti": pl.Utf8,
        "pci": pl.Int64,
    }
)
 
# Memoria persistente de metadatos por UE
# Ejemplo:
# {
#   2: {"plmn": "21405", "pci": 802, "rnti": "0x4605"}
# }
ue_metadata = {}
 
# Para no reprocesar el mismo bucket
last_processed_bucket = None
 
# ============================================================
# Parseo
# ============================================================
 
def parse_sdap_dl_line(line: str):
    """
    Extrae eventos SDAP DL con pdu_len.
    """
    match = SDAP_DL_REGEX.search(line)
    if not match:
        return None
 
    return {
        "ts": datetime.fromisoformat(match.group("ts")),
        "ue": int(match.group("ue")),
        "pdu_len": int(match.group("pdu_len")),
    }
 
 
def parse_ue_metadata_line(line: str):
    """
    Extrae metadatos PLMN / PCI / RNTI de una UE cuando aparezcan
    en líneas específicas del log.
    Devuelve:
        {"ue": int, "plmn": ..., "pci": ..., "rnti": ...}
    o None.
    """
 
    # Caso fuerte: línea completa [CU-UEMNG] con todos los campos
    m = UE_METADATA_REGEX.search(line)
    if m:
        return {
            "ue": int(m.group("ue")),
            "plmn": m.group("plmn"),
            "pci": int(m.group("pci")),
            "rnti": m.group("rnti"),
        }
 
    # Caso flexible: si existe ue=... y aparecen algunos campos
    m_ue = UE_ID_REGEX.search(line)
    if not m_ue:
        return None
 
    ue = int(m_ue.group("ue"))
 
    plmn = None
    pci = None
    rnti = None
 
    m_plmn = PLMN_REGEX.search(line)
    if m_plmn:
        if m_plmn.group("plmn"):
            plmn = m_plmn.group("plmn")
        else:
            plmn = f"{m_plmn.group('mcc')}{m_plmn.group('mnc')}"
 
    m_pci = PCI_REGEX.search(line)
    if m_pci:
        pci = int(m_pci.group("pci"))
 
    m_rnti = RNTI_REGEX.search(line)
    if m_rnti:
        rnti = m_rnti.group("rnti")
 
    # Si no hay ningún metadato real, no devolvemos nada
    if plmn is None and pci is None and rnti is None:
        return None
 
    return {
        "ue": ue,
        "plmn": plmn,
        "pci": pci,
        "rnti": rnti,
    }
 
# ============================================================
# Producer
# ============================================================
 
async def tail_log_producer(path: str, data_queue: asyncio.Queue):
    """
    Lee nuevas líneas del log en tiempo real.
    - Si encuentra metadatos, actualiza ue_metadata.
    - Si encuentra tráfico SDAP, mete el evento en la cola.
    """
    global ue_metadata
 
    file = Path(path)
    print("Waiting for:", path)
 
    while not file.exists():
        await asyncio.sleep(0.5)
 
    with open(path, "r", encoding="utf-8") as f:
        print("Opened:", path)
 
        # Solo nuevas líneas
        f.seek(0, 2)
 
        while True:
            line = f.readline()
 
            if not line:
                await asyncio.sleep(0.2)
                continue
 
            # 1) Parsear metadatos UE
            meta = parse_ue_metadata_line(line)
            if meta is not None:
                ue = meta["ue"]
 
                if ue not in ue_metadata:
                    ue_metadata[ue] = {"plmn": None, "pci": None, "rnti": None}
 
                if meta.get("plmn") is not None:
                    ue_metadata[ue]["plmn"] = meta["plmn"]
                if meta.get("pci") is not None:
                    ue_metadata[ue]["pci"] = meta["pci"]
                if meta.get("rnti") is not None:
                    ue_metadata[ue]["rnti"] = meta["rnti"]
 
                print(f"Metadata updated for UE {ue}: {ue_metadata[ue]}")
 
            # 2) Parsear tráfico SDAP
            parsed = parse_sdap_dl_line(line)
            if parsed is not None:
                await data_queue.put(parsed)
                print("Queued:", parsed)
 
# ============================================================
# Consumer
# ============================================================
 
async def consumer(data_queue: asyncio.Queue):
    """
    Consume eventos individuales y los añade a raw_df.
    """
    global raw_df
 
    while True:
        item = await data_queue.get()
 
        new_row = pl.DataFrame([item])
        raw_df = pl.concat([raw_df, new_row], how="vertical")
 
        print("Consumed:", item)
        data_queue.task_done()
 
# ============================================================
# Agregador
# ============================================================
 
async def aggregator():
    """
    Cada 1 segundo:
    - toma raw_df,
    - trunca timestamp al segundo,
    - suma pdu_len por segundo y por UE,
    - enriquece con los últimos metadatos conocidos,
    - añade solo buckets nuevos a agg_df.
    """
    global raw_df, agg_df, latest_plot_df, last_processed_bucket, ue_metadata
 
    while True:
        await asyncio.sleep(AGG_PERIOD_SEC)
 
        if raw_df.height == 0:
            continue
 
        tmp = raw_df.with_columns(
            pl.col("ts").dt.truncate("1s").alias("bucket_ts")
        )
 
        grouped = (
            tmp.group_by(["bucket_ts", "ue"])
               .agg(pl.col("pdu_len").sum().alias("bytes_sum"))
               .sort(["bucket_ts", "ue"])
        )
 
        if grouped.height == 0:
            continue
 
        if last_processed_bucket is None:
            new_rows = grouped
        else:
            new_rows = grouped.filter(pl.col("bucket_ts") > last_processed_bucket)
 
        if new_rows.height == 0:
            continue
 
        # Enriquecer con metadatos actuales
        rows = []
        for row in new_rows.iter_rows(named=True):
            ue = row["ue"]
            meta = ue_metadata.get(ue, {})
            rows.append({
                "bucket_ts": row["bucket_ts"],
                "ue": row["ue"],
                "bytes_sum": row["bytes_sum"],
                "plmn": meta.get("plmn"),
                "rnti": meta.get("rnti"),
                "pci": meta.get("pci"),
            })
 
        enriched_new_rows = pl.DataFrame(rows, schema=agg_df.schema)
 
        agg_df = pl.concat([agg_df, enriched_new_rows], how="vertical")
        latest_plot_df = agg_df.clone()
 
        last_processed_bucket = new_rows["bucket_ts"].max()
        print("Aggregated up to:", last_processed_bucket)
 
# ============================================================
# Guardado periódico a Parquet
# ============================================================
 
async def parquet_saver():
    global agg_df
 
    while True:
        await asyncio.sleep(PARQUET_PERIOD_SEC)
 
        if agg_df.height == 0:
            continue
 
        out_name = f"sdap_agg_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        agg_df.write_parquet(out_name)
        print("Saved parquet:", out_name)
 
# ============================================================
# Dash app
# ============================================================
 
app = Dash(__name__)
 
app.layout = html.Div([
    html.H3("Volumen agregado DL-SDAP por UE"),
    dcc.Graph(id="live-graph"),
    html.Div(id="live-metadata", style={"marginTop": "24px"}),
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
])
 
@app.callback(
    Output("live-graph", "figure"),
    Output("live-metadata", "children"),
    Input("interval", "n_intervals"),
)
def update_graph(n):
    fig = go.Figure()
 
    if latest_plot_df.height == 0:
        fig.update_layout(
            title="Esperando mensajes SDAP DL...",
            xaxis_title="Tiempo",
            yaxis_title="Bytes por segundo",
        )
        return fig, html.Div("Sin datos todavía.")
 
    df = latest_plot_df.sort(["bucket_ts", "ue"])
 
    # ========================================================
    # Relleno global con ceros para evitar diagonales
    # ========================================================
 
    start_ts = df["bucket_ts"].min()
    end_ts = df["bucket_ts"].max()
 
    full_time = pl.DataFrame({
        "bucket_ts": pl.datetime_range(
            start=start_ts,
            end=end_ts,
            interval="1s",
            eager=True
        )
    })
 
    ue_df = pl.DataFrame({
        "ue": sorted(df["ue"].unique().to_list())
    })
 
    # todos los segundos x todas las UEs
    full_grid = full_time.join(ue_df, how="cross")
 
    df_full = (
        full_grid.join(df, on=["bucket_ts", "ue"], how="left")
        .with_columns([
            pl.col("bytes_sum").fill_null(0),
        ])
        .sort(["ue", "bucket_ts"])
    )
 
    # Rellenar metadatos por UE usando la memoria global
    rows = []
    for row in df_full.iter_rows(named=True):
        ue = row["ue"]
        meta = ue_metadata.get(ue, {})
        rows.append({
            "bucket_ts": row["bucket_ts"],
            "ue": ue,
            "bytes_sum": row["bytes_sum"],
            "plmn": row["plmn"] if row["plmn"] is not None else meta.get("plmn"),
            "rnti": row["rnti"] if row["rnti"] is not None else meta.get("rnti"),
            "pci": row["pci"] if row["pci"] is not None else meta.get("pci"),
        })
 
    df_full = pl.DataFrame(rows, schema=latest_plot_df.schema)
 
    # ========================================================
    # Dibujar una serie por UE con metadatos
    # ========================================================
 
    ues = sorted(df_full["ue"].unique().to_list())
 
    for ue in ues:
        df_ue = df_full.filter(pl.col("ue") == ue).sort("bucket_ts")
 
        meta = ue_metadata.get(ue, {})
        plmn = meta.get("plmn") or "-"
        pci = meta.get("pci")
        rnti = meta.get("rnti") or "-"
 
        pci_str = str(pci) if pci is not None else "-"
 
        label = f"UE {ue} | PLMN:{plmn} | PCI:{pci_str} | RNTI:{rnti}"
 
        customdata = [[plmn, pci_str, rnti]] * df_ue.height
 
        fig.add_trace(
            go.Scatter(
                x=df_ue["bucket_ts"].to_list(),
                y=df_ue["bytes_sum"].to_list(),
                mode="lines+markers",
                name=label,
                customdata=customdata,
                hovertemplate=(
                    "Tiempo: %{x}<br>"
                    "Bytes/s: %{y}<br>"
                    "PLMN: %{customdata[0]}<br>"
                    "PCI: %{customdata[1]}<br>"
                    "RNTI: %{customdata[2]}<extra></extra>"
                ),
            )
        )
 
    fig.update_layout(
        title="Evolución temporal del volumen agregado por UE",
        xaxis_title="Tiempo",
        yaxis_title="Bytes/s",
        legend_title="UE",
    )
 
    # ========================================================
    # Tabla de metadatos por UE
    # ========================================================
 
    latest_info_rows = []
    for ue in sorted(ue_metadata.keys()):
        meta = ue_metadata.get(ue, {})
        latest_info_rows.append({
            "ue": ue,
            "plmn": meta.get("plmn"),
            "rnti": meta.get("rnti"),
            "pci": meta.get("pci"),
        })
 
    if latest_info_rows:
        latest_info = pl.DataFrame(latest_info_rows).sort("ue")
 
        metadata_table = html.Table([
            html.Thead(html.Tr([
                html.Th("UE"),
                html.Th("PLMN"),
                html.Th("RNTI"),
                html.Th("PCI"),
            ])),
            html.Tbody([
                html.Tr([
                    html.Td(str(int(row["ue"]))),
                    html.Td(row["plmn"] or "-"),
                    html.Td(row["rnti"] or "-"),
                    html.Td(str(row["pci"]) if row["pci"] is not None else "-"),
                ])
                for row in latest_info.iter_rows(named=True)
            ])
        ], style={"width": "100%", "borderCollapse": "collapse"})
    else:
        metadata_table = html.Div("Sin metadatos de UE todavía.")
 
    return fig, metadata_table
 
# ============================================================
# Main
# ============================================================
 
async def main():
    print("Main")
 
    asyncio.create_task(tail_log_producer(LIVE_LOG_FILE, queue))
    asyncio.create_task(consumer(queue))
    asyncio.create_task(aggregator())
    asyncio.create_task(parquet_saver())
 
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=DASH_PORT,
        debug=False,
    )
 
if __name__ == "__main__":
    asyncio.run(main())