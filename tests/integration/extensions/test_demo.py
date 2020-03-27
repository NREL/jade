import os
import pathlib
import shutil
import tempfile

import pandas as pd

import jade
from jade.utils.utils import load_data
from jade.utils.subprocess_manager import run_command

JADE_PATH = jade.__path__[0]


def test_demo_extension(test_data_dir):
    """Should create a config.json file"""
    inputs = os.path.join(test_data_dir, "demo")
    config_file = os.path.join(test_data_dir, "config.json")

    if os.path.exists(config_file):
        os.remove(config_file)
    
    base = os.path.join(JADE_PATH, "extensions", "demo")
    create_demo_config = os.path.join(base, "create_demo_config.sh")
    create_merge_config = os.path.join(base, "create_merge_pred_gdp.sh")
    script = os.path.join(base, "merge_pred_gdp.py")
    config_file = os.path.join(test_data_dir, "pipeline.toml")
    output = os.path.join(test_data_dir, "output")

    try:
        cmd = f"jade pipeline create {create_demo_config} {create_merge_config} -c {config_file}"
        returncode = run_command(cmd=cmd)
        assert returncode == 0
        assert os.path.exists(config_file)

        # check result
        data = load_data(config_file)
        assert len(data["stages"]) == 2

        if os.path.exists(output):
            shutil.rmtree(output)
        returncode = run_command(f"jade pipeline submit {config_file} -o {output}")
        assert returncode == 0

        output_stage1 = os.path.join(output, "output-stage1")
        output_stage2 = os.path.join(output, "output-stage2")
        assert os.path.exists(output)
        assert os.path.exists(output_stage1)
        assert os.path.exists(output_stage2)

        job_outputs = os.path.join(output_stage1, "job-outputs")

        for country in ("australia", "brazil", "united_states"):
            results = os.listdir(os.path.join(job_outputs, country))
            assert "result.csv" in results
            assert "result.png" in results
            assert "summary.toml" in results

        pred_gdp_file = os.path.join(output_stage1, "batch-post-process", "pred_gdp.csv")
        assert os.path.exists(pred_gdp_file)
        df = pd.read_csv(pred_gdp_file)
        assert "year" in df.columns
        assert "brazil" in df.columns
        assert "united_states" in df.columns
        assert "australia" in df.columns

    finally:
        if os.path.exists(output):
            shutil.rmtree(output)
        
        if os.path.exists(config_file):
            os.remove(config_file)
