def get_checkpoint_location(job_name: str) -> str:
    return f"/tmp/wfc/workshop/part03/checkpoint/{job_name}"


def kafka_input_topic() -> str:
    return 'visits'


def kafka_deduped_topic() -> str:
    return 'visits_deduped'
