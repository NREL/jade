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
    
    script = os.path.join(JADE_PATH, "extensions", "demo", "merge_pred_gdp.py")
    with tempfile.NamedTemporaryFile("w") as f:
        command = f"python {script} run output"
        f.write(command)
        f.seek(0)

        # run command
        cmd = f"jade auto-config -b {f.name} demo {inputs} -c {config_file}"
        returncode = run_command(cmd=cmd)
        assert returncode == 0
        assert os.path.exists(config_file)

        # check result
        data = load_data(config_file)

        assert "jobs" in data
        assert len(data["jobs"]) == 3
        assert "batch_post_process_config" in data
        assert os.path.exists(data["batch_post_process_config"]["file"])
        
        # Test submit-jobs
        cmd = f"jade submit-jobs {config_file}"

        output = os.path.join(test_data_dir, "output")
        if os.path.exists(output):
            shutil.rmtree(output)

        returncode = run_command(cmd=cmd, cwd=test_data_dir)
        assert returncode == 0

        output = os.path.join(test_data_dir, "output")
        assert os.path.exists(output)

        job_outputs = os.path.join(output, "job-outputs")

        for country in ("australia", "brazil", "united_states"):
            results = os.listdir(os.path.join(job_outputs, country))
            assert "result.csv" in results
            assert "result.png" in results
            assert "summary.toml" in results

        pred_gdp_file = os.path.join(output, "batch-post-process", "pred_gdp.csv")
        assert os.path.exists(pred_gdp_file)
        df = pd.read_csv(pred_gdp_file)
        assert "year" in df.columns
        assert "brazil" in df.columns
        assert "united_states" in df.columns
        assert "australia" in df.columns

        # if os.path.exists(output):
        #     shutil.rmtree(output)
        
        # if os.path.exists(config_file):
        #     os.remove(config_file)
