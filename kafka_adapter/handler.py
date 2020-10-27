
class KafkaBaseHandler:
    def get_topic(self):
        raise NotImplementedError

    def process_payload(self, payload):
        raise NotImplementedError
