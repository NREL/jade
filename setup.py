"""
setup.py
"""
import os
import logging
from codecs import open
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from subprocess import check_call
import shlex

logger = logging.getLogger(__name__)


def read_lines(filename):
    with open(filename) as f_in:
        return f_in.readlines()


try:
    from pypandoc import convert_text
except ImportError:
    convert_text = lambda string, *args, **kwargs: string

here = os.path.abspath(os.path.dirname(__file__))

with open("README.md", encoding="utf-8") as readme_file:
    readme = convert_text(readme_file.read(), "rst", format="md")

with open(os.path.join(here, "jade", "version.py"), encoding="utf-8") as f:
    version = f.read()

version = version.split()[2].strip('"').strip("'")

test_requires = ["pytest", ]

setup(
    name="jade",
    version=version,
    description="JADE",
    long_description=readme,
    author="NREL",
    maintainer_email="daniel.thom@nrel.gov",
    url="https://github.nrel.gov/Hosting-Capacity-Analysis/jade",
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
    keywords="jade",
    classifiers=[
        "Development Status :: Alpha",
        "Intended Audience :: Modelers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
    ],
    test_suite="tests",
    install_requires=read_lines("requirements.txt"),
    extras_require={
        "test": test_requires,
    },
)
