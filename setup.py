"""
setup.py
"""
import os
import json
import logging
from codecs import open
from pathlib import Path
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install


logger = logging.getLogger(__name__)


def read_lines(filename):
    return Path(filename).read_text().splitlines()


here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", encoding="utf-8") as f:
    readme = f.read()

with open(os.path.join(here, "jade", "version.py"), encoding="utf-8") as f:
    version = f.read()

version = version.split()[2].strip('"').strip("'")

demo_requires = ["matplotlib", "statsmodels>=0.10.1"]
dataframe_utils_requires = ["tables", "pyarrow"]
dev_requires = read_lines("dev-requirements.txt") + demo_requires + dataframe_utils_requires


class PostDevelopCommand(develop):
    """Post-installation for development mode."""

    def run(self):
        develop.run(self)
        remove_demo_extension()


class PostInstallCommand(install):
    """Post-installation for installation mode."""

    def run(self):
        install.run(self)
        remove_demo_extension()


def remove_demo_extension():
    # Older versions of Jade installed the demo extension into the registry as
    # well as its dependencies. Newer versions do not. This causes import errors
    # when a user upgrades to the newer version.
    # Remove the demo extension. The user can add it later if they want.
    registry_file = Path.home() / ".jade-registry.json"
    if not registry_file.exists():
        return

    data = json.loads(registry_file.read_text())
    for i, ext in enumerate(data["extensions"]):
        if ext["name"] == "demo":
            data["extensions"].pop(i)
            break
    with open(registry_file, "w") as f_out:
        json.dump(data, f_out)


setup(
    name="NREL-jade",
    version=version,
    description="Provides HPC workflow automation services",
    long_description=readme,
    long_description_content_type="text/markdown",
    maintainer="Daniel Thom",
    maintainer_email="daniel.thom@nrel.gov",
    url="https://github.com./NREL/jade",
    packages=find_packages(),
    package_dir={"jade": "jade"},
    entry_points={
        "console_scripts": [
            "jade=jade.cli.jade:cli",
            "jade-internal=jade.cli.jade_internal:cli",
        ],
    },
    include_package_data=True,
    license="BSD license",
    zip_safe=False,
    keywords=["jade", "hpc", "workflow"],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
    ],
    test_suite="tests",
    extras_require={
        "dev": dev_requires,
        "demo": demo_requires,
        "dataframe_utils": dataframe_utils_requires,
    },
    install_requires=read_lines("requirements.txt"),
    cmdclass={"install": PostInstallCommand, "develop": PostDevelopCommand},
)
