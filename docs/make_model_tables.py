"""Generates tables of the configuration data models."""

import os
from pathlib import Path

import click

from jade.models import *
from jade.extensions.generic_command.generic_command_parameters import (
    GenericCommandParametersModel,
)


def _get_output(_, __, value):
    return Path(value)


@click.command()
@click.option(
    "-o",
    "--output",
    default="build/model_tables",
    show_default=True,
    help="output directory",
    callback=_get_output,
)
def make_tables(output):
    os.makedirs(output, exist_ok=True)
    for cls in (
        SubmitterParams,
        SubmissionGroup,
        HpcConfig,
        SlurmConfig,
        GenericCommandParametersModel,
    ):
        schema = cls.schema()
        title = cls.__name__ + ": " + schema["description"]
        output_file = output / (cls.__name__ + ".csv")
        with open(output_file, "w") as f_out:
            header = ("Property", "Type", "Description", "Required", "Default")
            f_out.write("\t".join(header) + "\n")
            required_props = set(schema["required"])
            for prop, vals in schema["properties"].items():
                if "title" not in vals:
                    title = prop
                elif vals["title"] == prop:
                    title = prop
                else:
                    title = vals["title"] + " " + prop
                # TODO: this could be handled programmatically
                if cls is HpcConfig and prop == "hpc":
                    type_str = "Union[:ref:`model_slurm_config`, LocalHpcConfig, FakeHpcConfig]"
                elif cls is SubmitterParams and prop == "hpc_config":
                    type_str = ":ref:`model_hpc_config`"
                elif cls is SubmissionGroup and prop == "submitter_params":
                    type_str = ":ref:`model_submitter_params`"
                else:
                    type_str = vals.get("type", "Any")
                row = (
                    title,
                    type_str,
                    vals["description"],
                    str(prop in required_props),
                    str(vals.get("default", "")),
                )
                f_out.write("\t".join(row))
                f_out.write("\n")

    print(f"Generated config tables in {output}")


if __name__ == "__main__":
    make_tables()
