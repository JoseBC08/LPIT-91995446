import asyncio
import re
from pathlib import Path
from datetime import datetime
import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
 
# Fichero que va generando log-sim.py
LIVE_LOG_FILE = "cu-lan-ho.log"
 
# Agregamos cada 1 segundo
AGG_PERIOD_SEC = 1
 
# Se guarda en Parquet cada 30 segundos
PARQUET_PERIOD_SEC = 30

# Puerto Dash
DASH_PORT = 8059
 
# Línea objetivo, por ejemplo:
# 2026-01-19T08:57:06.466216 [SDAP ] [D] ue=1 psi=4 QFI=1 DRB1 DL: TX PDU. QFI=1 pdu_len=1278

SDAP_DL_REGEX = re.compile(

    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6})"

    r".*?\[SDAP\s*\]"

    r".*?ue=(?P<ue>\d+)"

    r".*?DL:\s*TX PDU"

    r".*?pdu_len=(?P<pdu_len>\d+)"

)
 
# ============================================================

# Cola compartida

# ============================================================
 
queue = asyncio.Queue()
 
# DataFrame de eventos brutos extraídos del log

raw_df = pl.DataFrame(

    schema={

        "ts": pl.Datetime,

        "ue": pl.Int64,

        "pdu_len": pl.Int64,

    }

)
 
# DataFrame de datos agregados por segundo y por UE

agg_df = pl.DataFrame(

    schema={

        "bucket_ts": pl.Datetime,

        "ue": pl.Int64,

        "bytes_sum": pl.Int64,

    }

)
 
# Snapshot que usa Dash

latest_plot_df = pl.DataFrame(

    schema={

        "bucket_ts": pl.Datetime,

        "ue": pl.Int64,

        "bytes_sum": pl.Int64,

    }

)
 
# Para no volver a agregar segundos ya procesados

last_processed_bucket = None
 
def parse_sdap_dl_line(line: str):

    """

    Si la línea corresponde a un mensaje SDAP DL con pdu_len,

    devuelve un diccionario con ts, ue y pdu_len.

    Si no, devuelve None.

    """

    match = SDAP_DL_REGEX.search(line)

    if not match:

        return None
 
    ts = datetime.fromisoformat(match.group("ts"))

    ue = int(match.group("ue"))

    pdu_len = int(match.group("pdu_len"))
 
    return {

        "ts": ts,

        "ue": ue,

        "pdu_len": pdu_len,

    }
 
# ============================================================

# Producer: quien lee o genera datos y los mete en la cola

# ============================================================
 
async def tail_log_producer(path: str, data_queue: asyncio.Queue):

    """

    Lee nuevas líneas del log en tiempo real.

    Solo mete en la cola las líneas SDAP DL relevantes.

    """

    file = Path(path)
 
    print("Waiting for:", path)
 
    while not file.exists():

        await asyncio.sleep(0.5)
 
    with open(path, "r", encoding="utf-8") as f:

        print("Opened:", path)
 
        # Solo procesa nuevas líneas, igual que en do-dash.py

        f.seek(0, 2)
 
        while True:

            line = f.readline()
 
            if not line:

                await asyncio.sleep(0.2)

                continue
 
            parsed = parse_sdap_dl_line(line)

            if parsed is not None:

                await data_queue.put(parsed)

                print("Queued:", parsed)
 
# ============================================================

# Consumer: quien consume datos de la cola y los procesa

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

    - trunca el timestamp al segundo,

    - suma pdu_len por segundo y por UE,

    - añade solo los buckets nuevos a agg_df.

    """

    global raw_df, agg_df, latest_plot_df, last_processed_bucket
 
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
 
        # Filtrar solo buckets no procesados todavía

        if last_processed_bucket is None:

            new_rows = grouped

        else:

            new_rows = grouped.filter(pl.col("bucket_ts") > last_processed_bucket)
 
        if new_rows.height == 0:

            continue
 
        agg_df = pl.concat([agg_df, new_rows], how="vertical")

        latest_plot_df = agg_df
 
        last_processed_bucket = new_rows["bucket_ts"].max()
 
        print("Aggregated up to:", last_processed_bucket)
 
# ============================================================

# Guardado periódico a Parquet

# ============================================================
 
async def parquet_saver():

    """

    Cada 30 segundos guarda agg_df en un fichero parquet.

    """

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

    dcc.Interval(id="interval", interval=1000, n_intervals=0),

])
 
@app.callback(

    Output("live-graph", "figure"),

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

        return fig
 
    ues = sorted(latest_plot_df["ue"].unique().to_list())
 
    for ue in ues:

        df_ue = latest_plot_df.filter(pl.col("ue") == ue).sort("bucket_ts")
 
        fig.add_trace(

            go.Scatter(

                x=df_ue["bucket_ts"].to_list(),

                y=df_ue["bytes_sum"].to_list(),

                mode="lines+markers",

                name=f"UE {ue}",

            )

        )
 
    fig.update_layout(

        title="Evolución temporal del volumen agregado por UE",

        xaxis_title="Tiempo",

        yaxis_title="Bytes/s",

        legend_title="UE",

    )
 
    return fig
 
# ============================================================

# Main

# ============================================================
 
async def main():

    print("Main")
 
    asyncio.create_task(tail_log_producer(LIVE_LOG_FILE, queue))

    asyncio.create_task(consumer(queue))

    asyncio.create_task(aggregator())

    asyncio.create_task(parquet_saver())
 
    # Dash se ejecuta en otro hilo para no bloquear asyncio

    await asyncio.to_thread(

        app.run,

        host="127.0.0.1",

        port=DASH_PORT,

        debug=False,

    )
 
if __name__ == "__main__":

    asyncio.run(main())
 