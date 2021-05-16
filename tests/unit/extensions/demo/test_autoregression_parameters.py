"""
Unit tests for auto-regression parameters class methods and properties
"""
from collections import namedtuple
from jade.extensions.demo.autoregression_parameters import AutoRegressionParameters

PARAMETERS_TYPE = namedtuple("AutoRegression", "country")


def test_init():
    """Should return an AutoRegressionParameters instance with desired properties"""
    arp = AutoRegressionParameters(country="United States", data="/path/to/data.csv")
    assert arp.name == "united_states"


def test_name():
    """Should create a name for job"""
    arp = AutoRegressionParameters(country="United States", data="/path/to/data.csv")
    assert arp.name == "united_states"


def test_serialize():
    """Should be serialized"""
    arp = AutoRegressionParameters(country="United States", data="/path/to/data.csv")
    data = arp.serialize()
    expected = {
        "country": "united_states",
        "data": "/path/to/data.csv",
        "extension": "demo",
    }
    assert data == expected


def test_deserialize():
    """Param data should be deserialized as AutoRegressionParameters instance"""
    param = {"country": "United States", "data": "/path/to/data.csv"}
    ari = AutoRegressionParameters.deserialize(param)
    assert isinstance(ari, AutoRegressionParameters)
    assert ari.country == "United States"
    assert ari.data == "/path/to/data.csv"
    assert ari.name == "united_states"
