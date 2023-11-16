from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import time
import joblib
#from confluent_kafka import Producer
from json import dumps, loads
from database import insert_data

def kafka_producer(X_test):
    producer = KafkaProducer(
        value_serializer = lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092'],
    )

    for index, value in X_test.iterrows():
            dict_data = dict(value)
            producer.send("workshop_003", value=dict_data)
            print('Mensaje enviado')
            time.sleep(1)


def kafka_consumer():
    consumer = KafkaConsumer(
        'workshop_003',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
        )

    for m in consumer:
        message = m.value
        df = pd.DataFrame([message])
        model_loaded = joblib.load('model.pkl')
        y_pred = model_loaded.predict(df)
        df["y_pred"] = y_pred
        print(df)
        insert_data(df)