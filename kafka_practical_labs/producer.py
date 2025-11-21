import time, uuid, random, json
from confluent_kafka import Producer
from fastavro import schemaless_writer, parse_schema
import io

# load schema
import json
schema = json.load(open('order.avsc'))
parsed_schema = parse_schema(schema)

p = Producer({'bootstrap.servers': 'localhost:29092'})

def avro_serialize(record, parsed_schema):
    buf = io.BytesIO()
    schemaless_writer(buf, parsed_schema, record)
    return buf.getvalue()

products = ["Item1", "Item2", "Item3"]

def produce(n=20, sleep=0.2):
    for i in range(n):
        order = {
            "orderId": str(uuid.uuid4())[:8],
            "product": random.choice(products),
            "price": round(random.uniform(5, 200),2)
        }
        payload = avro_serialize(order, parsed_schema)
        # we send raw bytes as value; key can be product to partition by product
        p.produce("orders", key=order["product"].encode('utf-8'), value=payload)
        p.poll(0)
        print("Produced", order)
        time.sleep(sleep)
    p.flush()

if __name__ == "__main__":
    produce()
