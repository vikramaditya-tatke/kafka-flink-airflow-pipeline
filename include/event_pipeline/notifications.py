"""Small availability hints for the supported Kafka message-queue trigger."""


def message_metadata(message):
    """Leave event bytes in Kafka; scheduling only needs their location."""
    return {
        "topic": message.topic(),
        "partition": message.partition(),
        "offset": message.offset(),
    }
