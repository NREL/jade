from jade.extensions.generic_command import GenericCommandConfiguration, GenericCommandParameters
from jade.loggers import setup_logging

setup_logging("config", None)
config = GenericCommandConfiguration()
config.add_user_data("overall_key_1", "overall_val_1")
config.add_user_data("overall_key_2", "overall_val_2")
config.teardown_command = "python postprocess.py"

base_cmd = "julia sim.jl"
for i in range(1, 9):
    cmd = base_cmd + " " + str(i)
    name = f"job_{i}"
    job = GenericCommandParameters(command=cmd, name=name, ext={f"key_{i}": f"val_{i}"})
    config.add_job(job)

config_file = "config.json"
config.dump(config_file, indent=2)
