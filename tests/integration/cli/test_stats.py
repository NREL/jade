
from jade.utils.subprocess_manager import run_command


OUTPUT_DIR = "tests/data/example_output"


def test_stats__show():
    output = {}
    ret = run_command(f"jade stats show -o {OUTPUT_DIR}", output)
    assert ret == 0
    assert len(output["stdout"]) > 0

    for text in ("Network stat", "Memory stat", "Disk stat", "CPU stat"):
        assert text in output["stdout"]


def test_stats__bytes_consumed():
    output = {}
    ret = run_command(f"jade stats bytes-consumed -o {OUTPUT_DIR}", output)
    assert ret == 0
    assert len(output["stdout"]) > 0

    ret = run_command(f"jade stats bytes-consumed --no-human-readable -o {OUTPUT_DIR}", output)
    assert ret == 0
    bytes_consumed = int(output["stdout"].strip())
    assert bytes_consumed > 0


def test_stats__exec_time():
    output = {}
    ret = run_command(f"jade stats exec-time -o {OUTPUT_DIR}", output)
    assert ret == 0
    assert len(output["stdout"]) > 0

    ret = run_command(f"jade stats exec-time --no-human-readable -o {OUTPUT_DIR}", output)
    assert ret == 0
    exec_time = float(output["stdout"].strip())
    assert exec_time > 0
