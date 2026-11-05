import unittest

from contract import verify_replay, verify_schemas


def receipt():
    return {
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


class ReplayContractTests(unittest.TestCase):
    def test_schema_drift_blocks_packaging(self):
        with self.assertRaisesRegex(ValueError, 'schema'):
            verify_schemas({'silver': [['sale_id', 'string'], ['price', 'string']]})

    def test_expected_receipt_passes(self):
        verify_replay(receipt())

    def test_duplicate_offsets_cannot_pass(self):
        observed = receipt()
        observed["final_valid"].append(0)
        with self.assertRaisesRegex(ValueError, "final_valid"):
            verify_replay(observed)

    def test_different_checkpoint_cannot_pass(self):
        observed = receipt()
        observed["same_checkpoint_identity"] = False
        with self.assertRaisesRegex(ValueError, "same_checkpoint_identity"):
            verify_replay(observed)

    def test_wrong_gold_total_cannot_pass(self):
        observed = receipt()
        observed["gold"][0][2] -= 1
        with self.assertRaisesRegex(ValueError, "gold"):
            verify_replay(observed)
