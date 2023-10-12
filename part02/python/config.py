def get_checkpoint_location() -> str:
    return "/tmp/wfc/workshop/part02/checkpoint"


def kafka_input_topic() -> str:
    return 'numbers'


def kafka_input_enriched_topic() -> str:
    return 'numbers_enriched'
