"""Script de un solo uso para generar el fixture de VIZ"""

import json
import pathlib

from multitudcsd.ingestion.http_request import download_json

RAIZ_REPO = pathlib.Path(__file__).resolve().parents[1]
DESTINO = RAIZ_REPO / "tests" / "fixtures" / "viz_disruptions_sample.json"

payload = download_json("https://api.viz.berlin.de/tic3/baustellen_sperrungen_tic.json")
incidencias = payload["features"]

# Interesa que el fixture cubra las dos formas de geometria que trae el feed: el punto
# suelto y la GeometryCollection (marcador + linea del tramo), que es la unica rama no
# trivial de extract_representative_point.
puntos = [i for i in incidencias if i.get("geometry", {}).get("type") == "Point"]
colecciones = [i for i in incidencias if i.get("geometry", {}).get("type") == "GeometryCollection"]
print(f"del feed: {len(incidencias)} incidencias, {len(puntos)} Point, {len(colecciones)} GeometryCollection")

payload["features"] = puntos[:2] + colecciones[:1]

DESTINO.parent.mkdir(parents=True, exist_ok=True)
DESTINO.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
print(f"escrito {DESTINO} con {len(payload['features'])} incidencias")