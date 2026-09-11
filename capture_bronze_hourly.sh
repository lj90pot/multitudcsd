#!/usr/bin/env bash
# Captura horaria de las fuentes reales hacia Bronze hasta manana a las 17:00.

cd /d/05_MasterUCM/TFM/multitudcsd || exit 1

# Se invoca el interprete del venv directamente. Activarlo con 'source' desde Git Bash
# reescribe el PATH en formato Windows y deja al shell sin mkdir, tee ni date.
PY=".venv/Scripts/python.exe"

mkdir -p data/_logs

$PY -c "import multitudcsd; print('paquete OK')" || exit 1

# --- Una sola vez: el GTFS estatico es una foto completa, no un flujo ---
$PY -m multitudcsd.ingestion.gtfs_static

# --- Una sola vez: station_information (coordenadas de las estaciones Nextbike).
# El main de gbfs.py hace status + information; run_bronze.py solo hace status. ---
$PY -m multitudcsd.ingestion.gbfs

# --- Bucle horario: gtfs_rt + gbfs status + viz ---
FIN=$(date -d "tomorrow 17:00" +%s)
INICIO=$(date +%s)
PASADA=0

while :; do
  OBJETIVO=$(( INICIO + PASADA * 3600 ))
  [ "$OBJETIVO" -gt "$FIN" ] && break

  ESPERA=$(( OBJETIVO - $(date +%s) ))
  [ "$ESPERA" -gt 0 ] && sleep "$ESPERA"

  echo "=== pasada $((PASADA + 1)) - $(date '+%Y-%m-%d %H:%M:%S') ==="
  # Un feed caido no puede tumbar la captura entera: se registra y se sigue.
  $PY -m multitudcsd.orchestration.run_bronze || echo "fallo en la pasada $((PASADA + 1))"

  PASADA=$(( PASADA + 1 ))
done

echo "captura terminada: $PASADA pasadas"