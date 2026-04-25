#==================================================================================
# El código ya no solo busca líneas SDAP, sino también cuatro tipos de información
#==================================================================================
import asyncio
import re
import time
from datetime import datetime
from pathlib import Path
 
import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
 
 
# =========================
# Configuración
# =========================
 
LOG_FILE = "cu-lan-ho.log"
PARQUET_FILE = "data.parquet"
 
AGG_PERIOD = 1
PARQUET_PERIOD = 30
DASH_PORT = 8059
 
 
# =========================
# Expresiones regulares
# =========================
 
sdap_re = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+).*"
    r"\[SDAP\s+\].*ue=(?P<ue>\d+).*DL: TX PDU.*pdu_len=(?P<pdu_len>\d+)"
)
# Se añaden tres nuevas expresiones regulares
# Expresión regular para extraer información de UE creada en CU-CP (plano de control)
cp_info_re = re.compile(
    r"\[CU-UEMNG\].*ue=(?P<cp_ue>\d+).*plmn=(?P<plmn>\d+).*"
    r"pci=(?P<pci>\d+).*rnti=(?P<rnti>0x[0-9a-fA-F]+)"
)
# Expresión regular para mapear UE CP -> cu_cp_ue
cp_to_e1_re = re.compile(
    r"\[CU-CP-E1\].*ue=(?P<cp_ue>\d+).*cu_cp_ue=(?P<cu_cp_ue>\d+)"
)
# Expresión regular para mapear UE UP -> cu_cp_ue
up_to_e1_re = re.compile(
    r"\[CU-UP-E1\].*ue=(?P<up_ue>\d+).*cu_cp_ue=(?P<cu_cp_ue>\d+)"
)
 
 
# =========================
# Variables compartidas
# =========================
 
queue = asyncio.Queue()
 
raw_rows = []
agg_rows = []
 
buffer = {}
known_ues = set()

# Estructuras nuevas de la fase 2
cp_ue_info = {} # Mapea UE del plano de control (CP) a su información (PLMN, PCI, RNTI)
cu_cp_ue_to_cp_ue = {} # Mapea UE del plano de control (CP) a UE del plano de usuario (UP)
up_ue_info = {} # Mapea UE del plano de usuario (UP) a su información (PLMN, PCI, RNTI) obtenida a través de la relación con el plano de control
 
 
# =========================
# Producer: lee el log en vivo
# =========================
 
async def tail_log_producer(path: str, data_queue: asyncio.Queue):
    file = Path(path)
 
    print("Waiting for:", path)
 
    while not file.exists():
        await asyncio.sleep(0.5)
 
    print("Opened:", path)
 
    with open(file, "r", encoding="utf-8", errors="ignore") as f:
        f.seek(0, 2)
 
        while True:
            line = f.readline()
 
            if not line:
                await asyncio.sleep(0.05)
                continue
 
            #UE creado en CU-CP: aquí aparecen PLMN, PCI y RNTI
            m = cp_info_re.search(line)
            if m: 
                await data_queue.put({
                    "type": "cp_info",
                    "cp_ue": int(m.group("cp_ue")),
                    "plmn": m.group("plmn"),
                    "pci": m.group("pci"),
                    "rnti": m.group("rnti"),
                })
                continue
 
            #Relación UE CP -> cu_cp_ue
            m = cp_to_e1_re.search(line)
            if m:
                await data_queue.put({
                    "type": "cp_map",
                    "cp_ue": int(m.group("cp_ue")),
                    "cu_cp_ue": int(m.group("cu_cp_ue")),
                })
                continue
 
            #Relación UE UP -> cu_cp_ue
            m = up_to_e1_re.search(line)
            if m:
                await data_queue.put({
                    "type": "up_map",
                    "up_ue": int(m.group("up_ue")),
                    "cu_cp_ue": int(m.group("cu_cp_ue")),
                })
                continue
 
            #Tráfico SDAP DL
            m = sdap_re.search(line)
            if m:
                await data_queue.put({
                    "type": "sdap",
                    "ts": datetime.fromisoformat(m.group("ts")),
                    "ue": int(m.group("ue")),
                    "pdu_len": int(m.group("pdu_len")),
                })
 
 
# =========================
# Consumer: procesa eventos
# =========================
 
async def consumer(data_queue: asyncio.Queue):
    while True:
        event = await data_queue.get()
 
        event_type = event["type"] # obtenemos el tipo de evento
        # Puede ser "cp_info", "cp_map", "up_map" o "sdap"
        # Tras ello, se procesa cada tipo de evento de forma diferente

        if event_type == "cp_info":
            cp_ue_info[event["cp_ue"]] = {
                "plmn": event["plmn"],
                "pci": event["pci"],
                "rnti": event["rnti"],
            }
 

        elif event_type == "cp_map":
            cu_cp_ue_to_cp_ue[event["cu_cp_ue"]] = event["cp_ue"]
 
        elif event_type == "up_map":
            up_ue = event["up_ue"]
            cu_cp_ue = event["cu_cp_ue"]
 
            cp_ue = cu_cp_ue_to_cp_ue.get(cu_cp_ue)
 
            if cp_ue in cp_ue_info and up_ue not in up_ue_info:
                up_ue_info[up_ue] = cp_ue_info[cp_ue]
 
        # Finalmente, si el evento es de tráfico SDAP, se procesa como antes, pero añadiendo el campo "type" al evento para que el consumidor sepa que es un evento SDAP y lo procese correctamente
        elif event_type == "sdap":
            ue = event["ue"]
            pdu_len = event["pdu_len"]
 
            raw_rows.append({
                "ts": event["ts"],
                "ue": ue,
                "pdu_len": pdu_len,
            })
 
            known_ues.add(ue)
            buffer[ue] = buffer.get(ue, 0) + pdu_len
 
        data_queue.task_done()
 
 
# =========================
# Aggregator: agrega cada segundo
# =========================
 
async def aggregator():
    last_parquet = time.time()
 
    while True:
        await asyncio.sleep(AGG_PERIOD)
 
        now = datetime.now()
 
        for ue in sorted(known_ues):
            total_bytes = buffer.get(ue, 0)
            info = up_ue_info.get(ue, {})
 
            # Guardamos, además de los bytes DL, el PLMN, PCI y RNTI si los tenemos
            agg_rows.append({
                "ts": now,
                "ue": ue,
                "bytes_dl": total_bytes,
                "plmn": info.get("plmn", "-"),
                "pci": info.get("pci", "-"),
                "rnti": info.get("rnti", "-"),
            })
 
        buffer.clear()
 
        #Cada 30 segundos guardar en un archivo Parquet la evolución de volúmenes agregados en cada UE
        if time.time() - last_parquet >= PARQUET_PERIOD and agg_rows:
            pl.DataFrame(agg_rows).write_parquet(PARQUET_FILE)
            last_parquet = time.time()
 
 
# =========================
# Dash
# =========================
 
app = Dash(__name__)
 
app.layout = html.Div([
    html.H2(
        "Monitorización 5G - Tráfico SDAP Downlink",
        style={"textAlign": "center"}
    ),
 
    dcc.Graph(id="live-graph"),
 
    dcc.Interval(
        id="interval",
        interval=1000,
        n_intervals=0,
    ),
])
 
 
@app.callback(
    Output("live-graph", "figure"),
    Input("interval", "n_intervals"),
)
def update_graph(_):
    fig = go.Figure()
 
    if not agg_rows:
        fig.update_layout(
            title="Esperando datos SDAP DL...",
            xaxis_title="Tiempo",
            yaxis_title="Bytes DL/s",
        )
        return fig
 
    df = pl.DataFrame(agg_rows)
 
    for ue in df["ue"].unique().sort():
        ue_int = int(ue)
        df_ue = df.filter(pl.col("ue") == ue_int)
 
        info = up_ue_info.get(ue_int, {})
 
        name = (
            f"UE {ue_int} | "
            f"PLMN={info.get('plmn', '-')} | "
            f"PCI={info.get('pci', '-')} | "
            f"RNTI={info.get('rnti', '-')}"
        )
 
        fig.add_trace(go.Scatter(
            x=df_ue["ts"].to_list(),
            y=df_ue["bytes_dl"].to_list(),
            mode="lines+markers",
            name=name,
        ))
 
    fig.update_layout(
        title={
            "text": "Evolución del tráfico DL agregado por UE",
            "x": 0.5,
            "xanchor": "center",
        },
        xaxis_title="Tiempo",
        yaxis_title="Bytes DL/s",
        legend_title="UE / PLMN / PCI / RNTI",
    )
 
    return fig
 
 
# =========================
# Main
# =========================
 
async def main():
    print("Starting log-parser...")
 
    asyncio.create_task(tail_log_producer(LOG_FILE, queue))
    asyncio.create_task(consumer(queue))
    asyncio.create_task(aggregator())
 
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=DASH_PORT,
        debug=False,
    )
 
 
if __name__ == "__main__":
    asyncio.run(main())