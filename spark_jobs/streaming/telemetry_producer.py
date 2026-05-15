import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

vehicles = [f"VIN{i:04}" for i in range(50)]

while True:

    data = {
        "vin": random.choice(vehicles),
        "timestamp": int(time.time()),
        "lat": round(random.uniform(28.5, 28.8), 6),
        "long": round(random.uniform(77.1, 77.4), 6),
        "speed": random.randint(20, 100)
    }

    producer.send("vehicle_telemetry", data)

    print("sent:", data)

    time.sleep(2)
