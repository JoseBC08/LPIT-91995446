import asyncio
import re
import time
import os
import threading
from datetime import datetime
import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

# ============================================================
# Variables Globales y Configuración
# ============================================================
log_file = "cu-lan-ho-live.log" 
queue = asyncio.Queue()

# Listas que actuarán como nuestros DataFrames en memoria (Fases 3 y 4).
# ¿Por qué listas? Porque añadir datos uno a uno a una lista de Python es
# muchísimo más rápido que añadirlos a un DataFrame de Polars directamente.
raw_data_list = []  
agg_data_list = [] 

# --- FASE 2: La "Memoria" de los Usuarios ---
# ¿Por qué usamos un diccionario? Porque el volumen de datos (bytes) llega 
# continuamente, pero el PLMN, PCI y RNTI solo aparecen UNA VEZ al conectarse.
# Guardamos esos datos fijos aquí para que Dash los consulte luego.
# Estructura que tendrá: {"UE_2": {"plmn": "21405", "pci": "802", "rnti": "0x4605"}}
ue_metadata = {}

# --- FASE 2: Buscadores (Expresiones Regulares o Regex) ---
# Filtro para el volumen de datos (atrapa el número de UE y los bytes)
regex_sdap = re.compile(r'\[SDAP\s*\].*?ue=(\d+).*?DL:.*?pdu_len=(\d+)')

# NUEVO FILTRO MAESTRO: Como vimos que tu log junta toda la info de identidad en una 
# sola línea (la que empieza por [CU-UEMNG]), creamos una única regla que busca
# "ue=", "plmn=", "pci=" y "rnti=" en la misma frase y captura sus valores en grupos.
regex_metadata = re.compile(r'\[CU-UEMNG\].*?ue=(\d+).*?plmn=(\d+).*?pci=(\d+).*?rnti=(0x[0-9a-fA-F]+)', re.IGNORECASE)

# ============================================================
# FASE 1 y 2: Productor Asíncrono
# ============================================================
async def log_producer(file_path, q):
    global ue_metadata # Llamamos a nuestra memoria global
    print(f"[Productor] Buscando archivo...")
    
    # Si el archivo aún no existe, esperamos 1 segundo y volvemos a mirar
    while not os.path.exists(file_path):
        await asyncio.sleep(1)
        
    # Abrimos el archivo en modo lectura
    with open(file_path, 'r', encoding='utf-8') as f:
        while True:
            line = f.readline()
            
            # Si no hay línea nueva, hacemos una pausa asíncrona de 0.1s para no saturar la CPU
            if not line:
                await asyncio.sleep(0.1)
                continue
            
            # --- PARTE NUEVA: Identificar metadatos (PLMN, PCI, RNTI) ---
            # ¿Por qué lo hacemos aquí? Para revisar cada línea que entra al momento.
            match_meta = regex_metadata.search(line)
            
            if match_meta:
                # Si la línea coincide, extraemos el ID del usuario (ej: "UE_2")
                ue_id = f"UE_{match_meta.group(1)}"
                
                # Guardamos los 3 datos directamente en nuestra memoria de golpe.
                # match_meta.group(2) corresponde al PLMN, el (3) al PCI y el (4) al RNTI.
                ue_metadata[ue_id] = {
                    "plmn": match_meta.group(2),
                    "pci": match_meta.group(3),
                    "rnti": match_meta.group(4)
                }

            # --- PARTE ANTERIOR: Buscar volumen de datos SDAP ---
            match_vol = regex_sdap.search(line)
            if match_vol:
                ue_id = f"UE_{match_vol.group(1)}"
                volume = float(match_vol.group(2))
                
                # Mandamos el dato a la cola. ¿Por qué usamos la cola? Para que el Productor 
                # (que lee súper rápido) le pase el trabajo pesado de sumar al Consumidor.
                await q.put({"time": datetime.now(), "ue": ue_id, "vol": volume})

# ============================================================
# FASE 3, 4 y 6: Consumidor y Guardado 
# ============================================================
async def log_consumer(q):
    global agg_data_list
    
    # Ventana temporal para ir sumando los bytes de cada UE durante 1 segundo
    current_window = {}
    last_agg_time = time.time() # Reloj para saber cuándo ha pasado el segundo
    
    while True:
        try:
            # wait_for: intentamos sacar un dato de la cola. Si en 0.1s no hay nada, da un error
            # de Timeout, que capturamos abajo para que no se rompa el programa y siga revisando el reloj.
            data = await asyncio.wait_for(q.get(), timeout=0.1)
            ue, vol = data["ue"], data["vol"]
            
            # Sumamos el volumen recibido al total de ese usuario
            current_window[ue] = current_window.get(ue, 0) + vol
        except asyncio.TimeoutError: 
            pass # No pasa nada, seguimos
        
        # ¿Ha pasado 1 segundo desde la última vez que guardamos?
        if time.time() - last_agg_time >= 1.0:
            for ue_id, total in current_window.items():
                # Guardamos la suma total de este segundo en la lista final
                agg_data_list.append({"time": datetime.now(), "ue": ue_id, "vol_agg": total})
            
            # Vaciamos la ventana para empezar a sumar de cero en el próximo segundo
            current_window.clear()
            last_agg_time = time.time()

# Tarea para guardar en disco cada 30 segundos
async def parquet_saver():
    while True:
        await asyncio.sleep(30) # Duerme 30 segundos exactos
        if agg_data_list:
            # Convierte la lista en un DataFrame oficial y lo guarda
            pl.DataFrame(agg_data_list).write_parquet("volumenes_fase2.parquet")

# Función que arranca las 3 tareas en la sombra a la vez
def start_async_tasks():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(log_producer(log_file, queue))
    loop.create_task(log_consumer(queue))
    loop.create_task(parquet_saver())
    loop.run_forever()

# ============================================================
# FASE 5: Dash App (Con leyenda informativa)
# ============================================================
app = Dash(__name__)

# Diseño de la web
app.layout = html.Div([
    html.H3("Dashboard Fase 2: Visualización con Metadatos de UE"),
    dcc.Graph(id="live-graph"),
    # dcc.Interval es el motor de la web: dispara la función 'update_graph' cada 1000 milisegundos
    dcc.Interval(id="interval", interval=1000, n_intervals=0),
])

@app.callback(Output("live-graph", "figure"), Input("interval", "n_intervals"))
def update_graph(n):
    global agg_data_list, ue_metadata
    
    # Si aún no hay datos, devolvemos una gráfica en blanco
    if not agg_data_list: return go.Figure()
    
    df = pl.DataFrame(agg_data_list)
    fig = go.Figure()
    
    # Sacamos una lista de todos los usuarios únicos que han enviado datos
    for ue in df["ue"].unique().to_list():
        # Filtramos los datos para dibujar solo la línea de ESTE usuario
        df_ue = df.filter(pl.col("ue") == ue)
        
        # --- PARTE NUEVA: Construir la leyenda ---
        # Consultamos nuestra memoria. Si encontramos a este UE, cogemos sus datos.
        # Si por lo que sea aún no los tenemos, ponemos un interrogante por defecto.
        info = ue_metadata.get(ue, {"plmn": "?", "pci": "?", "rnti": "?"})
        
        # Creamos la etiqueta de texto final concatenando las variables
        etiqueta = f"{ue} | PLMN:{info['plmn']} | PCI:{info['pci']} | RNTI:{info['rnti']}"
        
        # Añadimos la línea a la gráfica asignándole nuestro nuevo nombre completo
        fig.add_trace(go.Scatter(
            x=df_ue["time"].to_list(),
            y=df_ue["vol_agg"].to_list(),
            mode="lines+markers",
            name=etiqueta # <--- Aquí se muestra la info en la web
        ))
    
    # Retocamos el diseño general de la gráfica
    fig.update_layout(xaxis_title="Tiempo", yaxis_title="Bytes/seg", title="Tráfico SDAP DL")
    return fig

# Punto de entrada del programa
if __name__ == "__main__":
    # Arrancamos los bucles asíncronos en un hilo separado (daemon) para que no bloqueen a Dash
    threading.Thread(target=start_async_tasks, daemon=True).start()
    # Arrancamos el servidor web
    app.run(debug=True, use_reloader=False)