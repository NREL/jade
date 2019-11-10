"""
Unit tests for timing utility functions
"""
import time
from jade.utils.timing_utils import timed_info, timed_debug


def test_timed_info():
    """Test timed_info decorator"""

    @timed_info
    def target():
        return "hello world"

    result = target()

    assert result == "hello world"


def test_timed_debug():
    """Test timed_debug decorator"""

    @timed_debug
    def target():
        time.sleep(1)
        return "hello world"

    result = target()

    assert result == "hello world"
