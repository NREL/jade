"""Contains base class for simulation configurations."""

import abc
import enum
import json
import logging
import os
import sys

import toml

from jade.common import CONFIG_FILE
from jade.exceptions import InvalidConfiguration, InvalidParameter
from jade.extensions.registry import Registry, ExtensionClassType
from jade.utils.utils import dump_data, load_data, ExtendedJSONEncoder
from jade.utils.timing_utils import timed_debug


logger = logging.getLogger()


class ConfigSerializeOptions(enum.Enum):
    """Defines option for JobConfiguration serialization."""
    JOBS = enum.auto()
    JOB_NAMES = enum.auto()
    NO_JOB_INFO = enum.auto()


class JobConfiguration(abc.ABC):
    """Base class for any simulation configuration."""

    FILENAME_DELIMITER = "_"

    def __init__(
            self,
            inputs,
            container,
            job_parameters_class,
            extension_name,
            job_global_config=None,
            job_post_process_config=None,
            batch_post_process_config=None, 
            **kwargs
        ):
        """
        Constructs JobConfiguration.

        Parameters
        ----------
        inputs : JobInputsInterface
        container : JobContainerInterface

        """
        self._extension_name = extension_name
        self._inputs = inputs
        self._jobs = container
        self._job_parameters_class = job_parameters_class
        self._job_names = None
        self._jobs_directory = kwargs.get("jobs_directory")
        self._registry = Registry()
        self._job_global_config = job_global_config
        self._job_post_process_config = job_post_process_config
        self._batch_post_process_config = batch_post_process_config

        if kwargs.get("do_not_deserialize_jobs", False):
            assert "job_names" in kwargs, str(kwargs)
            self._job_names = kwargs["job_names"]
            return

        if "jobs" in kwargs:
            self._deserialize_jobs(kwargs["jobs"])
        elif "job_names" in kwargs:
            assert self._jobs_directory is not None, str(kwargs)
            names = kwargs["job_names"]
            self._deserialize_jobs_from_names(names)

    def __repr__(self):
        """Concisely display all instance information."""
        return self.dumps()

    def _deserialize_jobs(self, jobs):
        for job_ in jobs:
            job = self._job_parameters_class.deserialize(job_)
            self.add_job(job)

    def _deserialize_jobs_from_names(self, job_names):
        for name in job_names:
            job = self._get_job_by_name(name)
            self.add_job(job)

    def _dump(self, stream=sys.stdout, fmt=".json", indent=2):
        # Note: the default is JSON here because parsing 100 MB .toml files
        # is an order of magnitude slower.
        data = self.serialize()
        if fmt == ".json":
            json.dump(data, stream, indent=indent, cls=ExtendedJSONEncoder)
        elif fmt == ".toml":
            toml.dump(data, stream)
        else:
            assert False, fmt

    def _get_job_by_name(self, name):
        assert self._jobs_directory is not None
        filename = os.path.join(self._jobs_directory, name) + ".json"
        assert os.path.exists(filename), filename
        return self._job_parameters_class.deserialize(load_data(filename))

    @abc.abstractmethod
    def _serialize(self, data):
        """Create implementation-specific data for serialization."""

    def check_job_dependencies(self):
        """Check for impossible conditions with job dependencies.

        Raises
        ------
        InvalidConfiguration
            Raised if job dependencies have an impossible condition.

        """
        # This currently only checks that all jobs defined as blocking exist.
        # It does not look for deadlocks.

        job_names = set()
        blocking_jobs = set()
        for job in self.iter_jobs():
            job_names.add(job.name)
            blocking_jobs.update(job.get_blocking_jobs())

        missing_jobs = blocking_jobs.difference(job_names)
        if missing_jobs:
            for job in missing_jobs:
                logger.error("%s is blocking a job but does not exist", job)
            raise InvalidConfiguration("job ordering definitions are invalid")

    @abc.abstractmethod
    def create_from_result(self, job, output_dir):
        """Create an instance from a result file.

        Parameters
        ----------
        job : JobParametersInterface
        output_dir : str

        Returns
        -------
        class

        """

    @property
    def extension_name(self):
        """Return the extension name for the configuration."""
        return self._extension_name

    @abc.abstractmethod
    def get_job_inputs(self):
        """Return the inputs required to run a job."""

    def add_job(self, job):
        """Add a job to the configuration.

        Parameters
        ----------
        job : JobParametersInterface

        """
        self._jobs.add_job(job)

    def clear(self):
        """Clear all configured jobs."""
        self._jobs.clear()

    @timed_debug
    def dump(self, filename=None, stream=sys.stdout, indent=2):
        """Convert the configuration to structured text format.

        Parameters
        ----------
        filename : str | None
            Write configuration to this file (must be .json or .toml).
            If None, write the text to stream.
            Recommend using .json for large files. .toml is much slower.
        stream : file
            File-like interface that supports write().
        indent : int
            If JSON, use this indentation.

        Raises
        ------
        InvalidParameter
            Raised if filename does not have a supported extenstion.

        """
        if filename is None and stream is None:
            raise InvalidParameter("must set either filename or stream")

        if filename is not None:
            ext = os.path.splitext(filename)[1]
            if ext not in (".json", ".toml"):
                raise InvalidParameter("Only .json and .toml are supported")

            with open(filename, "w") as f_out:
                self._dump(f_out, fmt=ext, indent=indent)
        else:
            self._dump(stream, indent=indent)

        logger.info("Dumped configuration to %s", filename)

    def dumps(self, fmt_module=toml, **kwargs):
        """Dump the configuration to a formatted string."""
        return fmt_module.dumps(self.serialize(), **kwargs)

    @classmethod
    def deserialize(cls, filename_or_data, do_not_deserialize_jobs=False):
        """Create a class instance from a saved configuration file.

        Parameters
        ----------
        filename : str | dict
            path to configuration file or that file loaded as a dict
        do_not_deserialize_jobs : bool
            Set to True to avoid the overhead of loading all jobs from disk.
            Job_names will be stored instead of jobs.

        Returns
        -------
        class

        Raises
        ------
        InvalidParameter
            Raised if the config file has invalid parameters.

        """
        if isinstance(filename_or_data, str):
            data = load_data(filename_or_data)
        else:
            data = filename_or_data

        # Don't create an inputs object. It can be very expensive and we don't
        # need it unless the user wants to change the config.
        # TODO: implement user-friendly error messages when they try to access
        # inputs.
        inputs = None
        data["do_not_deserialize_jobs"] = do_not_deserialize_jobs
        return cls(inputs, **data)

    def get_job(self, name):
        """Return the job matching name.

        Returns
        -------
        namedtuple

        """
        if self.get_num_jobs() == 0 and self._job_names is not None:
            # We loaded from a config file with names only.
            return self._get_job_by_name(name)

        return self._jobs.get_job(name)

    def get_parameters_class(self):
        """Return the class used for job parameters."""
        return self._job_parameters_class

    def get_num_jobs(self):
        """Return the number of jobs in the configuration.

        Returns
        -------
        int

        """
        return self._jobs.get_num_jobs()

    @property
    def job_global_config(self):
        """Return the global configs applied to all jobs."""
        return self._job_global_config

    @property
    def job_post_process_config(self):
        """Return post process config for jobs"""
        return self._job_post_process_config

    @property
    def batch_post_process_config(self):
        """Return batch post process config for task"""
        return self._batch_post_process_config

    @batch_post_process_config.setter
    def batch_post_process_config(self, data):
        self._batch_post_process_config = data

    @property
    def inputs(self):
        """Return the instance of JobInputsInterface for the job."""
        return self._inputs

    def iter_jobs(self):
        """Yields a generator over all jobs.

        Yields
        ------
        iterator over JobParametersInterface

        """
        return self._jobs.iter_jobs()

    @timed_debug
    def list_jobs(self):
        """Return a list of all jobs.

        Returns
        ------
        list
            list of JobParametersInterface

        """
        return list(self.iter_jobs())

    @timed_debug
    def reconfigure_jobs(self, jobs):
        """Reconfigure with a list of jobs.

        Parameters
        ----------
        list of DistributionConfiguration.parameter_type

        """
        self.clear()

        for job in jobs:
            self.add_job(job)

        logger.info("Reconfigured jobs.")

    def remove_job(self, job):
        """Remove a job from the configuration.

        Parameters
        ----------
        job : JobParametersInterface

        """
        return self._jobs.remove_job(job)

    def run_job(self, job, output, **kwargs):
        """Run the job.

        Parameters
        ----------
        job : JobParametersInterface
        output : str
            output directory

        Returns
        -------
        int

        """
        logger.debug("job=%s kwargs=%s", job, kwargs)
        cls = self.job_execution_class()
        job_execution = cls.create(self.get_job_inputs(), job, output)
        return job_execution.run(**kwargs)

    def serialize(self, include=ConfigSerializeOptions.JOBS):
        """Create data for serialization."""
        data = {
            "class": self.__class__.__name__,
            "extension": self.extension_name,
            "jobs_directory": self._jobs_directory,
        }
        if self._job_global_config:
            data["job_global_config"] = self._job_global_config

        if self._job_post_process_config:
            data["job_post_process_config"] = self._job_post_process_config

        if self._batch_post_process_config:
            data["batch_post_process_config"] = self._batch_post_process_config

        if include == ConfigSerializeOptions.JOBS:
            data["jobs"] = [x.serialize() for x in self.iter_jobs()]
        elif include == ConfigSerializeOptions.JOB_NAMES:
            data["job_names"] = [x.name for x in self.iter_jobs()]

        # Fill in instance-specific information.
        self._serialize(data)
        return data

    def serialize_jobs(self, directory):
        """Serializes main job data to job-specific files.

        Parameters
        ----------
        output_dir : str

        """
        for job in self.iter_jobs():
            basename = job.name + ".json"
            job_filename = os.path.join(directory, basename)
            dump_data(job.serialize(), job_filename, cls=ExtendedJSONEncoder)

        # We will need this to deserialize from a filename that includes only
        # job names.
        self._jobs_directory = directory

    def serialize_for_execution(self, scratch_dir, are_inputs_local=True):
        """Serialize config data for efficient execution.

        Parameters
        ----------
        scratch_dir : str
            Temporary storage space on the local system.
        are_inputs_local : bool
            Whether the existing input data is local to this system. For many
            configurations accessing the input data across the network by many
            concurrent workers can cause a bottleneck and so implementations
            may wish to copy the data locally before execution starts. If the
            storage access time is very fast the question is irrelevant.

        Returns
        -------
        str
            Name of serialized config file in scratch directory.

        """
        self._transform_for_local_execution(scratch_dir, are_inputs_local)

        # Split up the jobs to individual files so that each worker can just
        # read its own info.
        self.serialize_jobs(scratch_dir)
        data = self.serialize(ConfigSerializeOptions.JOB_NAMES)
        config_file = os.path.join(scratch_dir, CONFIG_FILE)
        dump_data(data, config_file)
        logger.info("Dumped config file locally to %s", config_file)

        return config_file

    def _transform_for_local_execution(self, scratch_dir, are_inputs_local):
        """Transform data for efficient execution in a local environment.
        Default implementation is a no-op. Derived classes can overridde.

        """

    def show_jobs(self):
        """Show the configured jobs."""
        for job in self.iter_jobs():
            print(job)

    def job_execution_class(self):
        """Return the class used for job execution.

        Returns
        -------
        class

        """
        return self._registry.get_extension_class(self.extension_name,
                                                  ExtensionClassType.EXECUTION)
