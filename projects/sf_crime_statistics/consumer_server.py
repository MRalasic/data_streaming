from kafka import KafkaConsumer, KafkaClient
import asyncio
import logging

BROKER_URL = "localhost:9092"
TOPIC_NAME = "udacity.sfcrime.analytics.police.calls"

async def consume(consumer):
    logger = logging.getLogger(__name__)
    
    consumer.subscribe([TOPIC_NAME])

    for message in consumer:
        try:
            logger.info("Consumed {} messages".format(len(consumer)))
            logger.info("Consume message {}: {}".format(message.key(), message.value()))
        except Exception as e:
            logger.debug("Error mesage: {}".format(e))

        await asyncio.sleep(1)


def main():
    logger = logging.getLogger(__name__)
    consumer = KafkaConsumer(
        bootstrap_servers = BROKER_URL,
        auto_offset_reset = "earliest",
        group_id = "kafka-python-consumer-1"
    )
    try:
        asyncio.run(consume(consumer))
    except KeyboardInterrupt as e:
        logger.info("Consumer shutting down")
        consumer.close()
        logger.info("Consumer closed succesfully")

if __name__ == "__main__":
    main()
    