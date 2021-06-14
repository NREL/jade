# JADE

## Dev Setup
This project uses [conda](https://docs.conda.io/en/latest/) to manage software dependencies.
Please make sure `conda` has already been installed on your machine, or follow the
[guide](https://conda.io/projects/conda/en/latest/user-guide/install/index.html) to install.

### Virtual Environment
Use the commands below to create the virtual environment for Python, and
install the required packages.

Create virtual environment
```bash
conda env create -f environment.yml
# or
conda env create -f dev-environment.yml
```

Activate virtual environment
```bash
conda activate jade
```

Deactivate virtual environment
```bash
conda deactivate
```

The dev packages require that pandoc and plantuml be installed.

- Refer to https://pandoc.org/installing.html
- plantuml on Mac: ``brew install plantuml``
- plantuml on Linux: ``sudo apt-get install plantuml``
- plantuml on Windows: http://plantuml.com/starting


### Unit & Integration Test

Installation:
```
pip install -e '.[dev]'
```

This project uses [pytest](https://docs.pytest.org/en/latest/) as the framework to run unit tests
and integration tests, and generate HTML reports of coverage with the plugin
[pytest-cov](https://github.com/pytest-dev/pytest-cov). The following are some basic commands
for running tests with `pytest`.

Run unit tests
```bash
pytest --cov=jade tests/unit/ --cov-report=html -v
```

Run integration tests
```bash
pytest --cov=jade tests/integration/ --cov-report=html -v
```

Run test on a specific Python module
```bash
pytest --cov=jade tests/unit/utils/test_utils.py -v
```

Run test on a specific Python function
```bash
pytest --cov=jade tests/unit/utils/test_utils.py::test_create_chunks -v
```

Run test with debug logging activated
```bash
pytest tests/unit/jobs/test_job_queue.py --log-cli-level=debug
```

For more details and examples, please refer to the official pytest documentation.
