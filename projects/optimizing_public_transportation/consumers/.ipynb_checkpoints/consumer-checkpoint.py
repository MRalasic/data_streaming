"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen

from datetime import datetime

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "group.id": self.topic_name_pattern,
            "default.topic.config": {
                "auto.offset.reset": "earliest"
            },
        }

        if is_avro is True:
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            if self.offset_earliest is True:
                logger.debug("Topic with offset earliest".format(self.topic_name_pattern))
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("Partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(timeout=self.consume_timeout)
        except Exception as e:
                logger.error("Error message: ".format(e))
                return 0
        
        if message is None:
            logger.debug("No messages to consume")
        else:
            if message.error() is not None:
                logger.error("Error: ".format(message.error()))
                return 0
        
        logger.debug("Message sent. Key: {}, Value: {}".format(message.key(),message.value()))
        self.message_handler(message)
        return 1


    def close(self):
        """Cleans up any open kafka consumers"""
        logger.info("Close Consumer: {}".format(self.topic_name_pattern))
        self.consumer.close()
