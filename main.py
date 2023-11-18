"""
    This file is to call the consumer function and receive the data sent by the producer function
"""

from services.kafka import kafka_consumer


if __name__ == "__main__":
    kafka_consumer()