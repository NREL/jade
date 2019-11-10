import mock
from jade.loggers import setup_logging


@mock.patch("jade.loggers.logging.config.dictConfig")
@mock.patch("jade.loggers.logging.getLogger")
def test_setup_logging(mock_get_logger, mock_dict_config):
    """Should called dictConfig and getLogger methods"""
    # Call
    name = "this_is_a_logger_name"
    filename = "this_is_a_file_name"
    setup_logging(name, filename)

    # Assertions
    mock_dict_config.assert_called_once()
    mock_get_logger.assert_called_with(name)
