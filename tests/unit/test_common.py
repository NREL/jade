from jade import common


def test_output_dir():
    """ensure constants defined in common module"""

    assert common.OUTPUT_DIR == "output"
    assert common.JOBS_OUTPUT_DIR == "job-outputs"
    assert common.SCRIPTS_DIR == "scripts"
    assert common.CONFIG_FILE == "config.json"
    assert common.RESULTS_FILE == "results.json"
    assert common.ANALYSIS_DIR == "analysis"
