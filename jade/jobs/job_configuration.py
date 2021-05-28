"""Contains base class for simulation configurations."""

import abc
import enum
import json
import logging
import os
import sys
from collections import defaultdict

import toml

from jade.common import CONFIG_FILE
from jade.exceptions import InvalidConfiguration, InvalidParameter
from jade.extensions.registry import Registry, ExtensionClassType
from jade.jobs.job_container_by_name import JobContainerByName
from jade.models.submission_group import SubmissionGroup, SubmitterParams
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
    FORMAT_VERSION = "v0.2.0"

    def __init__(
        self,
        container=None,
        job_global_config=None,
        job_post_process_config=None,
        user_data=None,
        submission_groups=None,
        **kwargs,
    ):
        """
        Constructs JobConfiguration.

        Parameters
        ----------
        inputs : JobInputsInterface
        container : JobContainerInterface

        """
        self._jobs = container or JobContainerByName()
        self._job_names = None
        self._jobs_directory = kwargs.get("jobs_directory")
        self._registry = Registry()
        self._job_global_config = job_global_config
        self._job_post_process_config = job_post_process_config
        self._user_data = user_data or {}
        self._submission_groups = [SubmissionGroup(**x) for x in submission_groups or []]

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
        for _job in jobs:
            param_class = self.job_parameters_class(_job["extension"])
            job = param_class.deserialize(_job)
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
        job = load_data(filename)
        param_class = self.job_parameters_class(job["extension"])
        return param_class.deserialize(job)

    @abc.abstractmethod
    def _serialize(self, data):
        """Create implementation-specific data for serialization."""

    def add_user_data(self, key, data):
        """Add user data referenced by a key. Must be JSON-serializable

        Parameters
        ----------
        key : str
        data : any

        Raises
        ------
        InvalidParameter
            Raised if the key is already stored.

        """
        if key in self._user_data:
            raise InvalidParameter(f"{key} is already stored. Call remove_user_data first")

        self._user_data[key] = data

    def get_user_data(self, key):
        """Get the user data associated with key.

        Parameters
        ----------
        key : str

        Returns
        -------
        any

        """
        data = self._user_data.get(key)
        if data is None:
            raise InvalidParameter(f"{key} is not stored.")

        return data

    def remove_user_data(self, key):
        """Remove the key from the user data config.

        Parameters
        ----------
        key : str

        """
        self._user_data.pop(key, None)

    def list_user_data_keys(self):
        """List the stored user data keys.

        Returns
        -------
        list
            list of str

        """
        return sorted(list(self._user_data.keys()))

    def check_job_dependencies(self, submitter_params):
        """Check for impossible conditions with job dependencies.

        Parameters
        ----------
        submitter_params : SubmitterParams

        Raises
        ------
        InvalidConfiguration
            Raised if job dependencies have an impossible condition.

        """
        requires_estimated_time = submitter_params.per_node_batch_size == 0

        # This currently only checks that all jobs defined as blocking exist.
        # It does not look for deadlocks.

        job_names = set()
        blocking_jobs = set()
        missing_estimate = []
        for job in self.iter_jobs():
            job_names.add(job.name)
            blocking_jobs.update(job.get_blocking_jobs())
            if requires_estimated_time and job.estimated_run_minutes is None:
                missing_estimate.append(job.name)

        missing_jobs = blocking_jobs.difference(job_names)
        if missing_jobs:
            for job in missing_jobs:
                logger.error("%s is blocking a job but does not exist", job)
            raise InvalidConfiguration("job ordering definitions are invalid")

        if missing_estimate:
            for job in missing_estimate:
                logger.error("Job %s does not define estimated_run_minutes", job)
            raise InvalidConfiguration(
                "Submitting batches by time requires that each job define estimated_run_minutes"
            )

    def check_submission_groups(self, submitter_params):
        """Check for invalid job submission group assignments.
        Make a default group if none are defined and assign it to each job.

        Parameters
        ----------
        submitter_params : SubmitterParams

        Raises
        ------
        InvalidConfiguration
            Raised if submission group assignments are invalid.

        """
        groups = self.submission_groups
        if not groups:
            self._assign_default_submission_group(submitter_params)
            return

        first_group = next(iter(groups))
        group_params = (
            "try_add_blocked_jobs",
            "time_based_batching",
            "num_processes",
            "hpc_config",
            "per_node_batch_size",
        )
        user_overrides = ("generate_reports", "resource_monitor_interval", "dry_run", "verbose")
        user_override_if_not_set = ("node_setup_script", "node_shutdown_script")
        must_be_same = ("max_nodes", "poll_interval")
        all_params = (must_be_same, group_params, user_overrides, user_override_if_not_set)
        fields = {item for params in all_params for item in params}
        assert sorted(list(fields)) == sorted(SubmitterParams.__fields__)
        hpc_type = first_group.submitter_params.hpc_config.hpc_type
        group_names = set()
        for group in groups:
            if group.name in group_names:
                raise InvalidConfiguration(f"submission group {group.name} is listed twice")
            group_names.add(group.name)
            if group.submitter_params.hpc_config.hpc_type != hpc_type:
                raise InvalidConfiguration(f"hpc_type values must be the same in all groups")
            for param in must_be_same:
                first_val = getattr(first_group.submitter_params, param)
                this_val = getattr(group.submitter_params, param)
                if this_val != first_val:
                    raise InvalidConfiguration(f"{param} must be the same in all groups")
            for param in user_overrides:
                user_val = getattr(submitter_params, param)
                setattr(group.submitter_params, param, user_val)
            for param in user_override_if_not_set:
                user_val = getattr(submitter_params, param)
                group_val = getattr(group.submitter_params, param)
                if group_val is None:
                    setattr(group.submitter_params, param, user_val)

        jobs_by_group = defaultdict(list)
        for job in self.iter_jobs():
            if job.submission_group is None:
                raise InvalidConfiguration(
                    f"Job {job.name} does not have a submission group assigned"
                )
            if job.submission_group not in group_names:
                raise InvalidConfiguration(
                    f"Job {job.name} has an invalid submission group: {job.submission_group}"
                )
            jobs_by_group[job.submission_group].append(job.name)

        group_counts = {}
        for name, jobs in jobs_by_group.items():
            if not jobs:
                logger.warning("Submission group %s does not have any jobs defined", name)
            group_counts[name] = len(jobs)

        for name, count in sorted(group_counts.items()):
            logger.info("Submission group %s has %s jobs", name, count)

    def _assign_default_submission_group(self, submitter_params):
        default_name = "default"
        group = SubmissionGroup(name=default_name, submitter_params=submitter_params)
        for job in self.iter_jobs():
            job.submission_group = group.name
        self.append_submission_group(group)

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

        data["do_not_deserialize_jobs"] = do_not_deserialize_jobs
        return cls(**data)

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

    def get_num_jobs(self):
        """Return the number of jobs in the configuration.

        Returns
        -------
        int

        """
        return len(self._jobs)

    @property
    def job_global_config(self):
        """Return the global configs applied to all jobs."""
        return self._job_global_config

    def iter_jobs(self):
        """Yields a generator over all jobs.

        Yields
        ------
        iterator over JobParametersInterface

        """
        return iter(self._jobs)

    @timed_debug
    def list_jobs(self):
        """Return a list of all jobs.

        Returns
        ------
        list
            list of JobParametersInterface

        """
        return list(self.iter_jobs())

    def append_submission_group(self, submission_group):
        """Append a submission group.

        Parameters
        ----------
        submission_group : SubmissionGroup

        """
        self._submission_groups.append(submission_group)
        logger.info("Added submission group %s", submission_group.name)

    def get_default_submission_group(self):
        """Return the default submission group.
        If all the jobs in the config have the same group, return that one.
        If the jobs have different groups, return an arbitrary one.

        Returns
        -------
        SubmissionGroup

        """
        group_names = {x.submission_group for x in self.iter_jobs()}
        logger.debug(f"submission groups: {group_names}")
        return self.get_submission_group(next(iter(group_names)))

    def get_submission_group(self, name):
        """Return the submission group matching name.

        Parameters
        ----------
        name : str

        Returns
        -------
        SubmissionGroup

        """
        for group in self.submission_groups:
            if group.name == name:
                return group

        raise InvalidParameter(f"submission group {name} is not stored")

    @property
    def submission_groups(self):
        """Return the submission groups.

        Returns
        -------
        list

        """
        return self._submission_groups

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

    def serialize(self, include=ConfigSerializeOptions.JOBS):
        """Create data for serialization."""
        data = {
            "jobs_directory": self._jobs_directory,
            "configuration_module": self.__class__.__module__,
            "configuration_class": self.__class__.__name__,
            "format_version": self.FORMAT_VERSION,
            "user_data": self._user_data,
            "submission_groups": [x.dict() for x in self.submission_groups],
        }
        if self._job_global_config:
            data["job_global_config"] = self._job_global_config

        if self._job_post_process_config:
            data["job_post_process_config"] = self._job_post_process_config

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
        directory : str

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
        dump_data(data, config_file, cls=ExtendedJSONEncoder)
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

    def job_execution_class(self, extension_name):
        """Return the class used for job execution.

        Parameters
        ----------
        extension_name : str

        Returns
        -------
        class

        """
        return self._registry.get_extension_class(extension_name, ExtensionClassType.EXECUTION)

    def job_parameters_class(self, extension_name):
        """Return the class used for job parameters.

        Parameters
        ----------
        extension_name : str

        Returns
        -------
        class

        """
        return self._registry.get_extension_class(extension_name, ExtensionClassType.PARAMETERS)
