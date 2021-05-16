"""Collects information about the source code repository."""

import logging
import os
import re

from jade.exceptions import ExecutionError, InvalidParameter
from jade.utils.subprocess_manager import run_command


logger = logging.getLogger(__name__)


class RepositoryInfo:
    """Collects information about the source code repository for a package."""

    def __init__(self, package):
        # This will be the directory containing the package.
        self._path = os.path.dirname(getattr(package, "__path__")[0])

        if not os.path.exists(os.path.join(self._path, ".git")):
            raise InvalidParameter("{package} is not in a git repository")

        self._patch_filename = None

    def _run_command(self, cmd):
        orig = os.getcwd()
        os.chdir(self._path)

        try:
            output = {}
            ret = run_command(cmd, output=output)
            if ret != 0:
                raise ExecutionError(f"[{cmd}] failed: {ret}: {output['stderr']}")

            return output["stdout"].strip()
        finally:
            os.chdir(orig)

    def current_branch(self):
        """Return the current branch.

        Returns
        -------
        str

        """
        cmd = "git rev-parse --abbrev-ref HEAD"
        output = self._run_command(cmd)
        return output

    def last_commit(self):
        """Return the last commit.

        Returns
        -------
        str

        """
        output = self._run_command("git log -n 1")

        regex = re.compile(r"^commit (\w+)")
        match = regex.search(output)
        assert match, output
        commit = match.group(1)

        return commit

    def status(self):
        """Return the current status.

        Returns
        -------
        str

        """
        cmd = "git status --porcelain=v2 --branch --verbose " "--untracked-files=no"
        output = self._run_command(cmd)
        return output

    def write_diff_patch(self, filename):
        """Write any repo diffs to filename.

        Parameters
        ----------
        filename : str

        """
        output = self._run_command("git diff")
        if output:
            with open(filename, "w") as f_out:
                f_out.write(output)
                logger.info("Wrote diff to %s", filename)
                self._patch_filename = filename
        else:
            logger.info("No diff detected in repository.")

    def summary(self):
        """Return a summary of the repository status.

        Returns
        -------
        dict

        """
        return {
            "current_branch": self.current_branch(),
            "diff_patch_file": self._patch_filename,
            "last_commit": self.last_commit(),
            "status": self.status(),
        }
