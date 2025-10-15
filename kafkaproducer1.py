from kafka import KafkaProducer
import json, time, random

# Sample dataset
restaurants = ["Pizza Hub", "Spicy Curry", "BBQ Nation", "Sushi Express"]
customers = ["John Doe", "Sara Khan", "David Lee", "Amy Tan"]
reviews = [
    "Amazing food! Loved it.",
    "Terrible experience. Never coming back.",
    "Service was okay, but food was great.",
    "Loved the sushi, will visit again.",
    "The place was too crowded, but food was decent."
]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Send review data
while True:
    data = {
        "order_id": random.randint(1000, 5000),
        "restaurant_name": random.choice(restaurants),
        "customer_name": random.choice(customers),
        "review": random.choice(reviews),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    producer.send("restaurant_reviews", data)
    print(f"Produced: {data}")
    time.sleep(2)
