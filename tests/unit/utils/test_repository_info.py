"""
Unit tests for functions used for getting repository info
"""
import os
import tempfile

import pandas
import pytest

import jade
from jade.exceptions import InvalidParameter
from jade.utils.repository_info import RepositoryInfo


def test_repository_info():
    """Should return the current branch"""
    repo = RepositoryInfo(jade)

    # current branch
    branch = repo.current_branch()
    assert isinstance(branch, str)

    # last commit
    last_commit = repo.last_commit()
    assert isinstance(last_commit, str)

    # status
    status = repo.status()
    assert last_commit in status

    # diff
    if repo._run_command("git diff"):
        diff_file = os.path.join(
            tempfile.gettempdir(),
            "jade-repo-diff-file.txt",
        )
        repo.write_diff_patch(diff_file)
        assert os.path.exists(diff_file)
    else:
        diff_file = None

    # summary
    summary = repo.summary()
    assert summary["current_branch"] == branch
    assert summary["diff_patch_file"] == diff_file
    assert summary["last_commit"] == last_commit
    assert summary["status"] == status

    if diff_file and os.path.exists(diff_file):
        os.remove(diff_file)


def test_repository_info__exception():
    """Should raise exception if not a jade repo"""
    os.chdir("tests")
    try:
        with pytest.raises(InvalidParameter):
            # Pass a package that is not a repo.
            RepositoryInfo(pandas)
    finally:
        os.chdir("..")
