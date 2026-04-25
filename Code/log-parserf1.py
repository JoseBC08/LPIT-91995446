import asyncio  # Ejecutar varias tareas de forma concurrente
import re       # Sirve para usar expresiones regulares
import time 
from datetime import datetime
from pathlib import Path

import polars as pl # Se usa para crear dataframes
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go


LOG_FILE = "cu-lan-ho.log" # Archivo que se va a leer en tiempo rea
PARQUET_FILE = "data.parquet" # Archivo donde se guardarán los datos agregados

AGG_PERIOD = 1
PARQUET_PERIOD = 30
DASH_PORT = 8059

# Definición de las expresiones regulares
# La expresión regular busca líneas del log que tengan la estructura que buscamos
sdap_re = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+).*"
    r"\[SDAP\s+\].*ue=(?P<ue>\d+).*DL: TX PDU.*pdu_len=(?P<pdu_len>\d+)"
)
# Se busca el bloque SDAP

# Variables globales de almacenamiento
queue = asyncio.Queue() # Cola de conexión entre produtor y consumidor
# - El productor se encargará de leer el log y meter eventos en cola
# - El consumidor sacará los eventos de la cola para procesarlos


raw_rows = [] # Guarda los eventos INDIVIDUALES. Cada evento es una PDU detectada en el log
agg_rows = [] # Guarda los datos agregados cada segundo. Cada elemento representa cuántos bytes ha recibido un UE durante 1s

buffer = {}
known_ues = set()

# ============================================================
# Productor: quien lee datos en tiempo real y los mete en la cola
# ============================================================
async def tail_log_producer(path: str, data_queue: asyncio.Queue):
    file = Path(path) #Convertir la ruta del archivo en un objeto Path

    print("Waiting for:", path)

    while not file.exists():
        await asyncio.sleep(0.5)

    print("Opened:", path)

    with open(file, "r", encoding="utf-8", errors="ignore") as f:
        f.seek(0, 2) # El cursor se coloca al final para estar actualizado con las líneas que van llegando

        while True:
            line = f.readline()

            if not line:
                await asyncio.sleep(0.05) 
                # Con await no bloquea todo el programa, solo pausa esta tarea
                # y permite que otras tareas sigan funcionando
                continue

            m = sdap_re.search(line)
            # Buscamos en la linea la expresión regular (sobre el SDAP) de forma que si la encuentra,
            # la guardará en m.

            if m:
                # Procedemos a guardar en la cola el diccionario con los datos de la línea.
                # Estamos añadiendo un evento a la cola
                await data_queue.put({
                    "ts": datetime.fromisoformat(m.group("ts")),
                    "ue": int(m.group("ue")),
                    "pdu_len": int(m.group("pdu_len")),
                })


# ============================================================
#Consumer: quien consume datos de la cola y los procesa
# ============================================================
async def consumer(data_queue: asyncio.Queue):
    while True:
        event = await data_queue.get()
        # Espera hasta que haya un evento disponible en la cola
        # Cuando hay un evento, lo saca y lo guarda en la variable EVENT

        ue = event["ue"]
        pdu_len = event["pdu_len"]

        # Guarda el evento en el diccionario raw_rows
        # Esto permite tener un control de los eventos, como un historial
        raw_rows.append({
            "ts": event["ts"],
            "ue": ue,
            "pdu_len": pdu_len,
        })

        # Ahora guardamos los usuarios conocidos y los bytes recibidos
        known_ues.add(ue)
        buffer[ue] = buffer.get(ue, 0) + pdu_len # Los bytes se acumulan

        data_queue.task_done() # Evento procesado

# ============================================================
# Agregadoción temporal: Cada segundo mira el buffer, guarda los bytes acumulados y reinicia el buffer
# ============================================================
async def aggregator():
    last_parquet = time.time() # Guardamos el instante actual para controlar cuándo guardar el parquet

    while True:
        await asyncio.sleep(AGG_PERIOD) # Esperamos un segundo para hacer la agregación cada segundo

        now = datetime.now()

        # Recorremos todos los UEs
        for ue in sorted(known_ues):
            # Para cada UE, obtenemos el total de bytes acumulados en el buffer
            total_bytes = buffer.get(ue, 0)

            agg_rows.append({
                "ts": now,
                "ue": ue,
                "bytes_dl": total_bytes,
            })

        buffer.clear() # Limpiamos el buffer para, en el siguiente segundo, empezar a acumular de nuevo los bytes que vayan llegando
        
        # Cada 30 segundos guardar en un archivo Parquet la evolución de volúmenes agregados en cada UE
        if time.time() - last_parquet >= PARQUET_PERIOD and agg_rows:
        # Si han pasado 30 segundos desde la última vez que guardamos el parquet, y tenemos datos agregados, entonces guardamos el parquet
            pl.DataFrame(agg_rows).write_parquet(PARQUET_FILE)
            last_parquet = time.time()

# ============================================================
# Creación de la aplicación Dash
# ============================================================
app = Dash(__name__)

app.layout = html.Div([ # Todo lo que esté dentro de html.Div se mostrará en la página web
    html.H2(
        "Volumen DL agregado por UE - SDAP",
        style={"textAlign": "center"}
    ),

    dcc.Graph(id="live-graph"), # LIVE-GRAPH es el identificador del gráfico

    # IMPORTANTE: Creamos un temporizador
    # La idea es que el temporizador se dispare cada segundo, y cada vez que se dispare, se ejecute la función UPDATE_GRAPH para actualizar el gráfico con los datos más recientes
    dcc.Interval(
        id="timer", # Identificador del temporizador
        interval=1000, # Intervalo de tiempo en milisegundos (1000 ms = 1 segundo)
        n_intervals=0, # Contador de intervalos, se incrementa cada vez que se dispara el temporizador
    ),
])


@app.callback(
    Output("live-graph", "figure"),
    Input("timer", "n_intervals")
) # Cada vez que el temporizador se dispare, se llamará a la función UPDATE_GRAPH para actualizar el gráfico con los datos más recientes
def update_graph(_):
    fig = go.Figure()

    if not agg_rows: # Cuando no hay datos agregados, útil para el inicio de la app
        fig.update_layout(
            title="Esperando datos SDAP DL...",
            xaxis_title="Tiempo",
            yaxis_title="Bytes DL/s",
        )
        return fig

    # A partir de Polars, generamos un dataframe a partir de los datos agregados
    # Necesario para hacer el gráfico con Plotly
    df = pl.DataFrame(agg_rows)

    # Recorremos cada UE que hay en el dataframe
    for ue in df["ue"].unique().sort():
        ue_int = int(ue)
        df_ue = df.filter(pl.col("ue") == ue_int)
        # Aquí le hemos pasado un filtro al dataframe para quedarnos solo con el UE en cuestión
        # Añadimos ahora a la gráfica
        fig.add_trace(go.Scatter(
            x=df_ue["ts"].to_list(),
            y=df_ue["bytes_dl"].to_list(),
            mode="lines+markers", # Líneas y puntos, podríamos haber usado otro tipo de gráfico
            name=f"UE {ue_int}",
        ))

    fig.update_layout(
        title={
            "text": "Volumen DL agregado cada 1 segundo por UE",
            "x": 0.5,
            "xanchor": "center",
        },
        xaxis_title="Tiempo",
        yaxis_title="Bytes DL/s",
    )

    return fig


#              ---- FUNCIÓN MAIN ----


async def main():
    print("Starting log-parser Fase 1...")

    # Las tres tareas concurrentes.
    # Se alternan rápidamente, de forma que el programa puede leer el log, procesar los eventos y actualizar el gráfico al mismo tiempo sin bloquearse
    asyncio.create_task(tail_log_producer(LOG_FILE, queue))
    asyncio.create_task(consumer(queue))
    asyncio.create_task(aggregator())

    # Ejecución de la aplicación Dash
    await asyncio.to_thread(
        app.run,
        host="127.0.0.1",
        port=DASH_PORT,
        debug=False,
    )


if __name__ == "__main__":
    asyncio.run(main())