from confluent_kafka import Producer

TOPIC = "kafka_demo"
productor = Producer({"bootstrap.servers": "localhost:9092"})

mensajes = [
    '{"mention_id": "demo_004", "platform": "mastodon"}',
    '{"mention_id": "demo_005", "platform": "bluesky"}',
    '{"mention_id": "demo_006", "platform": "x"}',
]

for mensaje in mensajes:
    productor.produce(TOPIC, value=mensaje.encode("utf-8"))
    print(f"[producer] enviado: {mensaje}")

productor.flush()
print("[producer] listo, 3 mensajes publicados")