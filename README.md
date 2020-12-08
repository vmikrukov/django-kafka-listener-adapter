# django-kafka-listener-adapter
This project helps you easily listen kafka topics in your django app

Create your own class from **KafkaAdapter** and override two class methods:

Example:
```
class KafkaListenerAdapter(KafkaAdapter):

    @classmethod
    def get_available_kafka_handlers(cls):
        pass

    @classmethod
    def handle(cls, payload, handler):
        pass
```
                                         
Then you can run listening as a command in separate docker-container:

```
class Command(BaseCommand):
    def handle(self, *args, **options):
        KafkaListenerAdapter.start()
```

