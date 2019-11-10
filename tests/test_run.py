import click
from click.testing import CliRunner

import jade.cli.run_qsts_scenario as run_qsts_scenario
import jade.cli.submit_jobs as submit_jobs


# TODO:  Just a placeholder...needs a lot of work


def test_run_qsts_scenario():
    runner = CliRunner()
    result = runner.invoke(run_qsts_scenario.cli, [])
    assert result.exit_code != 0


#def test_run():
#    runner = CliRunner()
#    result = runner.invoke(submit_jobs.cli, []
#                           "--control", 0])
#    assert result.exit_code == 0



if __name__ == "__main__":
    test_run_qsts_scenario()
