"""
Unit tests for utility read functions
"""
import os
import tempfile

import mock
from jade.utils.read_functions import json, np, pd
from jade.utils.read_functions import Read_Init_File, Read_Customer_Data


def init_data(*args, **kwargs):
    """data loaded from init json file"""

    # TODO: Use dict data as side effect, instead of mock.
    # return {
    #     "GenPVCases": {
    #         "Number of Scenarios": 2
    #     }
    # }

    return mock.MagicMock()


@mock.patch("jade.utils.read_functions.json.load", side_effect=init_data)
@mock.patch("builtins.open")
def test_read_init_file(mock_open, mock_json_load):
    """Should read DPVInitizationFile file into Python dict"""
    init_file = os.path.join(tempfile.gettempdir(), "init-file.json")
    dpv_init_data = Read_Init_File(init_file, None, verbose=True)

    assert "Num_PV_Scenarios" in dpv_init_data
    assert "Cap_Buses" in dpv_init_data


def custom_data(*args, **kwargs):
    """data loaded from custom excel file"""
    # TODO: Use fake custom data, instead of mock
    data = mock.MagicMock()
    data.__len__.return_value = 1
    return data


@mock.patch("jade.utils.read_functions.np")
@mock.patch("jade.utils.read_functions.pd.read_excel", side_effect=custom_data)
def test_read_customer_data(mock_read_excel, mock_np):
    """Should return cust_data.xls into Python dict"""
    # TODO: Refactor Read_Customer_Data then test again
    # custom_file = os.path.join(tempfile.gettempdir(), "custom-file.xls")
    # Read_Customer_Data(custom_file, verbose=True)
    pass
