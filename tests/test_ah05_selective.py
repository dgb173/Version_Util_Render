from scripts.ah05_model.selective_experiment import gate_result, wilson


def test_wilson_interval_contains_observed_rate():
    low, high = wilson(80, 100)
    assert low < .8 < high


def test_gate_abstains_between_symmetric_thresholds():
    result = gate_result(
        target=__import__("numpy").array([1, 0, 1]),
        probability=__import__("numpy").array([.9, .1, .55]),
        threshold=.8,
    )
    assert result["picks"] == 2
    assert result["accuracy"] == 1

