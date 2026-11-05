"""Independent expectations for the fixed synthetic replay, not a transform."""

EXPECTED_SCHEMAS = {
    'stream-valid': [['sale_id', 'string'], ['district', 'string'], ['price', 'bigint'], ['revision', 'int'], ['partition', 'int'], ['offset', 'bigint']],
    'stream-rejected': [['partition', 'int'], ['offset', 'bigint'], ['reason', 'string']],
    'silver': [['sale_id', 'string'], ['district', 'string'], ['price', 'bigint'], ['revision', 'int']],
    'gold': [['district', 'string'], ['sale_count', 'bigint'], ['total_price', 'bigint']],
}


def verify_schemas(observed):
    if observed != EXPECTED_SCHEMAS:
        raise ValueError('Delta output schema does not match the replay contract')


def verify_replay(observed):
    expected = {
        "batch_before": 2,
        "batch_after": 2,
        "first_valid": [0, 1],
        "first_rejected": [2],
        "final_valid": [0, 1, 3, 4],
        "final_rejected": [2, 5],
        "idle_valid": [0, 1, 3, 4],
        "idle_rejected": [2, 5],
        "same_checkpoint_identity": True,
        "silver": [
            ["H1", "E10", 310000, 2], ["H2", "E5", 220000, 1],
            ["S1", "E10", 450000, 1], ["S2", "E5", 280000, 1],
        ],
        "gold": [["E10", 2, 760000], ["E5", 2, 500000]],
    }
    for key, value in expected.items():
        if key not in observed or type(observed[key]) is not type(value) or observed[key] != value:
            raise ValueError(f"Replay contract failed for {key}")
