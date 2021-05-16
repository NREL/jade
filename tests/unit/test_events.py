"""
Unit tests for job event object and methods
"""
import os
from jade.events import (
    StructuredLogEvent,
    StructuredErrorLogEvent,
    EventsSummary,
    EVENT_NAME_UNHANDLED_ERROR,
)


def test_structured_event__create():
    """Test class initialization and methods"""
    event = StructuredLogEvent(
        source="job_1",
        category="ParameterError",
        name="test-error",
        message="Something happens",
        country="Canada",
        foo="foo info",
        bar="bar info",
    )
    assert "timestamp" in str(event)
    assert "source" in str(event)
    assert "foo" in event.data
    assert "bar" in event.data


def test_structured_error_event__create():
    """Test class initialization and methods"""
    try:
        raise Exception("test")
    except Exception:
        event = StructuredErrorLogEvent(
            source="job_1",
            category="ParameterError",
            name="test-error",
            message="Something happens",
        )
        assert "timestamp" in str(event)
        assert "error" in event.data
        assert "filename" in event.data
        assert "lineno" in event.data


def test_event_summary__show_events(test_data_dir, capsys):
    """Should print tabular events in terminal"""
    event_dir = os.path.join(test_data_dir, "events", "job-outputs", "australia")
    event_summary = EventsSummary(event_dir)
    event_summary.show_events(EVENT_NAME_UNHANDLED_ERROR)
    captured = capsys.readouterr()
    assert "Exception" in captured.out
    assert "australia" in captured.out
    assert "united_states" not in captured.out
