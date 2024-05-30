import json
import random
import time
import string
from string import digits
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import os
from concurrent.futures import ThreadPoolExecutor


fake = Faker()


# KAFKA CONFIGURATION
# os.environ.pop('KAFKA_BROKER')
kafka_broker = os.getenv('KAFKA_BROKER', 'broker:9092')
print(f"kafka broker name {kafka_broker}")

kafka_producer = KafkaProducer(
                            bootstrap_servers=[kafka_broker],
                            value_serializer=lambda x: json.dumps(x).encode('utf-8')
                        )

# COMMERCE DATA
customers = []
products = []


# Genetating customers data
def generate_customer():
    customer = {
        "customer_id": f''.join(random.choice(digits) for i in range(8)),
        "name": fake.name(),
        "email": fake.email(),
        "address": fake.address(),
        "age": random.randint(20, 75),
        "gender": random.choice(["Male", "Female", "Prefer not to say"]),
        "account_created": fake.past_date().isoformat(),
        "last_login": fake.date_time_this_month().isoformat()
    }

    customers.append(customer["customer_id"])
    return customer

# print(generate_customer())

# Generate Product data
def generate_product():
    categories = ['Electronics', 'Books', 'Clothing', 'Foods', 'Home & Garden']
    product = {
        "product_id": ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8)),
        "name": fake.word().title(),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 500), 2),
        "stock_quantity": random.randint(0, 100),
        "supplier": fake.company(),
        "rating": round(random.uniform(1, 5), 1)
    }
    products.append(product["product_id"])
    return product

# print(generate_product())

# Generate transaction data
def generate_transaction():
    customer_id = random.choice(customers)
    product_id = random.choice(products)
    return {
        "transaction_id": fake.uuid4(),
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "date_time": fake.date_time_this_year().isoformat(),
        "status": random.choice(["completed", "pending", "canceled"]),
        "payment_method": random.choice(["credit card", "PayPal", "bank transfer"])
    }

# Generate Product View Data
def generate_product_view():
    return {
        "view_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "view_duration": random.randint(10, 300)  # Duration in seconds
    }


# Generate System Log Data
def generate_system_log():
    log_levels = ["INFO", "WARNING", "ERROR"]
    return {
        "log_id": fake.uuid4(),
        "timestamp": fake.date_time_this_year().isoformat(),
        "level": random.choice(log_levels),
        "message": fake.sentence()
    }

# Generate User Interaction Data
def generate_user_interaction():
    interaction_types = ["wishlist_addition", "review", "rating"]
    return {
        "interaction_id": fake.uuid4(),
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "timestamp": fake.date_time_this_year().isoformat(),
        "interaction_type": random.choice(interaction_types),
        "details": fake.sentence() if interaction_types == "review" else None
    }


# Function to send data to kafka topics
def send_data():
    if random.random() < 0.5:
        customer = generate_customer()
        kafka_producer.send('ecommerce_customers', value=customer)
    else:
        product = generate_product()
        kafka_producer.send('ecommerce_product', value=product)

    if customers and product:
        transaction = generate_transaction()
        kafka_producer.send('ecommerce_transactions', value=transaction)
        product_view = generate_product_view()

        if product_view:
            kafka_producer.send('ecommerce_product_views', value=product_view)
            user_interaction = generate_user_interaction()
        if user_interaction:
            kafka_producer.send('ecommerce_user_interactions', value=user_interaction)
    kafka_producer.send('ecommerce_system_logs', value=generate_system_log())


# Parallel Data Generation
with ThreadPoolExecutor(max_workers=5) as executor:
    while True:
        executor.submit(send_data)
        time.sleep(random.uniform(0.01, 0.1))

