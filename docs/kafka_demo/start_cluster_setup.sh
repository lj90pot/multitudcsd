# orden que respeta las dependencias: controllers -> brokers -> schema-registry ->
# connect -> control-center. Fuera del pipeline evaluado
# de multitudcsd; ver docs/decisiones.md para la decision de no usar Kafka ahi.

set -e  # si un paso falla, se para en vez de seguir levantando servicios a ciegas

# Situarse en la carpeta de este script, sea cual sea el directorio desde el que se lance
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

COMPOSE_FILE="docker-compose.yaml"

if [ ! -f .env ]; then
    echo "Falta el fichero .env"
    exit 1
fi

if grep -q "PEGA_AQUI_EL_UUID_GENERADO" .env; then
    echo "CLUSTER_ID no esta rellenado en .env. Genera uno con:"
    echo "  docker run --rm confluentinc/cp-kafka:7.7.0 kafka-storage random-uuid"
    exit 1
fi

echo "Iniciando entorno"

docker compose -f "$COMPOSE_FILE" up -d controller-tfm1
sleep 30

docker compose -f "$COMPOSE_FILE" up -d broker-tfm1
sleep 30

docker compose -f "$COMPOSE_FILE" up -d schema-registry-tfm
sleep 30

docker compose -f "$COMPOSE_FILE" up -d connect-tfm
sleep 30

docker compose -f "$COMPOSE_FILE" up -d control-center-tfm
sleep 60

docker compose -f docker-compose.yaml restart control-center-tfm

echo "Entorno levantado. Comprobar estado con:"
echo "  docker compose -f $COMPOSE_FILE ps"