# consumer.py
import time, io, random
from confluent_kafka import Consumer, Producer, KafkaError
from fastavro import schemaless_reader, parse_schema
import json

schema = json.load(open('order.avsc'))
parsed_schema = parse_schema(schema)

KAFKA_BOOTSTRAP = 'localhost:29092'
GROUP = 'assignment-group'
MAX_RETRIES = 3

consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'group.id': GROUP,
    'auto.offset.reset': 'earliest'
})
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

consumer.subscribe(['orders'])

# running average state
count = 0
total = 0.0

def avro_deserialize(b):
    buf = io.BytesIO(b)
    return schemaless_reader(buf, parsed_schema)

def send_to_dlq(raw_value, headers=None):
    producer.produce('orders-dlq', value=raw_value, headers=headers)
    producer.flush()

def push_agg(product, running_avg):
   
    agg = {'product': product, 'running_avg': running_avg}
    producer.produce('orders-agg', value=json.dumps(agg).encode('utf-8'))
    producer.flush()

def process_order(order):
    """
    Simulated processing logic.
    We'll randomly throw temporary or permanent errors to demonstrate retry/DLQ.
    """
    r = random.random()
    if r < 0.1:
        
        raise ValueError("Permanent processing error")
    elif r < 0.25:
        
        raise RuntimeError("Temporary error")
    
    return True

def handle_message(msg):
    global count, total
    raw = msg.value()
    try:
        order = avro_deserialize(raw)
    except Exception as e:
        print("Deserialization failed, sending to DLQ:", e)
        send_to_dlq(raw, headers=[('reason','deserialization')])
        return

    order_id = order['orderId']
    product = order['product']
    price = float(order['price'])

    
    attempt = 0
    while attempt <= MAX_RETRIES:
        try:
            print(f"Processing {order_id} attempt {attempt+1}")
            process_order(order)
            
            count += 1
            total += price
            running_avg = total / count
            print(f"Processed {order_id} OK. Running avg price (global): {running_avg:.2f}")
            push_agg(product, running_avg)
            break
        except RuntimeError as te:
            # temporary -> retry
            attempt += 1
            if attempt > MAX_RETRIES:
                print(f"Exceeded retries for {order_id}. Sending to DLQ.")
                send_to_dlq(raw, headers=[('reason','retries_exhausted')])
                break
            backoff = 0.5 * (2 ** (attempt-1))
            print(f"Temporary error: {te}; backing off {backoff}s and retrying")
            time.sleep(backoff)
            continue
        except Exception as e:
            # permanent or unexpected -> DLQ
            print(f"Permanent error processing {order_id}: {e}. Sending to DLQ.")
            send_to_dlq(raw, headers=[('reason','permanent_error')])
            break

def main():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # handle consumer errors
                print("Consumer error:", msg.error())
                continue
            handle_message(msg)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
