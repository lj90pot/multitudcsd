from confluent_kafka import Producer

TOPIC = "kafka_demo"
productor = Producer({"bootstrap.servers": "localhost:9092"})

mensajes = [
    '{"mention_id": "demo_999", "platform": "a_mastodon"}',
    '{"mention_id": "demo_998", "platform": "a_bluesky"}',
    '{"mention_id": "demo_997", "platform": "a_x"}',
]

for mensaje in mensajes:
    productor.produce(TOPIC, value=mensaje.encode("utf-8"))
    print(f"[producer] enviado: {mensaje}")

productor.flush()
print("[producer] listo, 3 mensajes publicados")