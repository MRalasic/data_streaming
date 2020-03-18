from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        """Sends data to Kafka topic"""
        with open(self.input_file) as f:
            call_data = json.load(f)
            for line in call_data:
                message = self.dict_to_binary(line)
                self.send(self.topic, value=message)
                time.sleep(1)

    def dict_to_binary(self, json_dict):
        """Converts Python dictionary to JSON"""
        return json.dumps(json_dict).encode("utf-8")
        