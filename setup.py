"""
setup.py
"""
import os
import logging
from codecs import open
from pathlib import Path
from setuptools import setup, find_packages

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
)
