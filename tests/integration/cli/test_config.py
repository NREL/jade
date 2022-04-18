import os
import shutil
import sys

import pytest

from jade.extensions.generic_command import GenericCommandConfiguration, GenericCommandParameters
from jade.utils.subprocess_manager import run_command
from jade.utils.utils import load_data


CONFIG1 = "test-config1.json"
CONFIG2 = "test-config2.json"


@pytest.fixture
def cleanup():
    yield
    for path in (CONFIG1, CONFIG2):
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


def test_config__show(cleanup):
    ret = run_command(f"jade auto-config demo tests/data/demo -c {CONFIG1}")
    assert ret == 0
    assert os.path.exists(CONFIG1)

    output = {}
    ret = run_command(f"jade config show {CONFIG1}", output=output)
    assert ret == 0

    for country in ("australia", "brazil", "united_states"):
        assert country in output["stdout"]


def test_config__filter_copy(cleanup):
    ret = run_command(f"jade auto-config demo tests/data/demo -c {CONFIG1}")
    assert ret == 0
    assert os.path.exists(CONFIG1)

    ret = run_command(f"jade config filter {CONFIG1} -o {CONFIG2}")
    assert ret == 0
    assert os.path.exists(CONFIG2)

    config1 = load_data(CONFIG1)
    config2 = load_data(CONFIG2)
    assert config1 == config2


def test_config__filter_indices(cleanup):
    ret = run_command(f"jade auto-config demo tests/data/demo -c {CONFIG1}")
    assert ret == 0
    assert os.path.exists(CONFIG1)

    ret = run_command(f"jade config filter {CONFIG1} -o {CONFIG2} 0 2")
    assert ret == 0
    assert os.path.exists(CONFIG2)

    config1 = load_data(CONFIG1)
    config2 = load_data(CONFIG2)
    assert config2["jobs"] == [config1["jobs"][0], config1["jobs"][2]]


def test_config__filter_range(cleanup):
    ret = run_command(f"jade auto-config demo tests/data/demo -c {CONFIG1}")
    assert ret == 0
    assert os.path.exists(CONFIG1)

    ret = run_command(f"jade config filter {CONFIG1} -o {CONFIG2} 0 1")
    assert ret == 0
    assert os.path.exists(CONFIG2)

    config1 = load_data(CONFIG1)
    config2 = load_data(CONFIG2)
    assert config2["jobs"] == [config1["jobs"][0], config1["jobs"][1]]


def test_config__filter_field(cleanup):
    ret = run_command(f"jade auto-config demo tests/data/demo -c {CONFIG1}")
    assert ret == 0
    assert os.path.exists(CONFIG1)

    ret = run_command(f"jade config filter {CONFIG1} -o {CONFIG2} -f country brazil")
    assert ret == 0
    assert os.path.exists(CONFIG2)

    config1 = load_data(CONFIG1)
    config2 = load_data(CONFIG2)
    assert config2["jobs"] == [config1["jobs"][1]]


def test_config__filter_show_only(cleanup):
    ret = run_command(f"jade auto-config demo tests/data/demo -c {CONFIG1}")
    assert ret == 0
    assert os.path.exists(CONFIG1)

    output = {}
    ret = run_command(f"jade config filter {CONFIG1} -f country brazil", output=output)
    assert ret == 0
    assert not os.path.exists(CONFIG2)

    assert "brazil" in output["stdout"]


def test_config__assign_blocked_by(cleanup):
    config = GenericCommandConfiguration()
    base_cmd = "bash my_script.sh"
    regular_job_names = []
    for i in range(1, 4):
        cmd = base_cmd + " " + str(i)
        name = f"job_{i}"
        job = GenericCommandParameters(
            command=cmd,
            name=name,
            append_job_name=True,
            append_output_dir=True,
        )
        config.add_job(job)
        regular_job_names.append(name)

    pp_name = "post_process"
    post_process_job = GenericCommandParameters(
        command="bash run_post_process.sh",
        name=pp_name,
        append_job_name=True,
        append_output_dir=True,
    )
    config.add_job(post_process_job)
    config_file = CONFIG1
    config.dump(config_file, indent=2)

    ret = run_command(f"jade config assign-blocked-by {CONFIG1} 3 -o {CONFIG2}")
    assert ret == 0
    assert os.path.exists(CONFIG2)

    config = load_data(CONFIG2)
    assert sorted(config["jobs"][3]["blocked_by"]) == sorted(regular_job_names)

    os.remove(CONFIG2)
    ret = run_command(f"jade config assign-blocked-by {CONFIG1} 3 1 2 -o {CONFIG2}")
    assert ret == 0
    assert os.path.exists(CONFIG2)
    config = load_data(CONFIG2)
    expected = [regular_job_names[1], regular_job_names[2]]
    assert sorted(config["jobs"][3]["blocked_by"]) == sorted(expected)

    # Include the pp job in blocking-job-indexes.
    ret = run_command(f"jade config assign-blocked-by {CONFIG1} 3 1 2 3 -o {CONFIG2}")
    assert ret != 0

    # Invalid job index
    ret = run_command(f"jade config assign-blocked-by {CONFIG1} 47 1 2 -o {CONFIG2}")
    assert ret != 0
