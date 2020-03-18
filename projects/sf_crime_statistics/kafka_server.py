import producer_server


def run_kafka_server():
    """
    Creates a Kafka Police calls data producer
    """
    
    input_file = "police-department-calls-for-service.json"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="udacity.sfcrime.analytics.police.calls",
        bootstrap_servers="localhost:9092",
        client_id="kafka-python-producer-1"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
