"""
Unit tests for job event object and methods
"""
import os
from jade.events import StructuredJobEvent, EventsSummary


def test_structured_job_event__create():
    """Test class initialization and methods"""
    event = StructuredJobEvent(
        job_name="job_1",
        category="ParameterError",
        code="400",
        message="Something happens",
        country="Canada",
        foo="foo info",
        bar="bar info"
    )
    assert "timestamp" in str(event)
    assert "exception" not in str(event)

    event.parse_traceback()
    assert "exception" in str(event)


def test_event_summary__show_events(test_data_dir, capsys):
    """Should print tabular events in terminal"""
    event_dir = os.path.join(test_data_dir, "events")
    event_summary = EventsSummary(event_dir)
    event_summary.show_events()
    captured = capsys.readouterr()
    assert "400" in captured.out
    assert "canada" in captured.out
    assert "australia" in captured.out
    assert "united_states" not in captured.out
