import asyncio
import re
from pathlib import Path
from datetime import datetime
import polars as pl
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go

df = pl.read_parquet("sdap_dl_2026-01-19T08_57_06.parquet")
print(df)