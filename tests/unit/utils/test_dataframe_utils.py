"""
Unit tests for postprocessing and analysis functions
"""
import datetime
import tempfile
from pandas.testing import assert_frame_equal
import pytest
from pytest import mark

from jade.utils.dataframe_utils import *


def get_timeseries(length, delta=datetime.timedelta(hours=1)):
    """Generate timeseries data"""
    start = datetime.datetime.now()
    timeseries = [start]

    for i in range(length - 1):
        timeseries.append(timeseries[i] + delta)

    return timeseries


def create_dataframe():
    """Create a sample pandas dataframe."""
    data = {
        "A": range(1, 11),
        "B": range(11, 21),
        "timestamp": get_timeseries(10),
    }

    df = pd.DataFrame(data)
    df.set_index("timestamp", inplace=True)
    return df


def test_read_dataframe__csv():
    """Should create identical dataframe on reading csv file"""
    df1 = create_dataframe()
    with tempfile.NamedTemporaryFile(suffix=".csv") as temp:
        df1.to_csv(temp.name)
        df2 = read_dataframe(temp.name, index_col="timestamp", parse_dates=True)
        assert_frame_equal(df1, df2, check_index_type=False)


def test_read_dataframe__json():
    """Should create identical dataframe on reading json file"""
    df1 = create_dataframe()
    with tempfile.NamedTemporaryFile(suffix=".json") as temp:
        df1.to_json(temp.name, orient="index", date_unit="ns")
        df2 = read_dataframe(temp.name, orient="index", date_unit="ns")
        df2.index.name = "timestamp"
        assert_frame_equal(df1, df2)


def test_read_dataframe__feather():
    """Should create identical dataframe on reading feather file

    feather does not support serializing a non-default index for the index
    """
    df1 = create_dataframe()
    with tempfile.NamedTemporaryFile(suffix=".feather") as temp:
        df1.reset_index().to_feather(temp.name)
        df2 = read_dataframe(temp.name, parse_dates=True, index_col="timestamp")
        assert_frame_equal(df1, df2)


def test_read_dataframe__h5():
    """Should create identical dataframe on reading HDF5 file

    HDF5 does not support serializing a non-default index for the index
    """
    df1 = create_dataframe()
    with tempfile.NamedTemporaryFile(suffix=".h5") as temp:
        df1.reset_index().to_hdf(temp.name, "data")
        df2 = read_dataframe(temp.name, parse_dates=True, index_col="timestamp")
        assert_frame_equal(df1, df2)


def test_read_dataframe__file_not_found():
    """Should raise exception if file not found"""
    filename = "/dataframe/file/does/not/exist"
    with pytest.raises(FileNotFoundError) as exc:
        read_dataframe(filename)

        assert "does not exit" in str(exc.value)


def test_read_dataframe__invalid_parameter():
    """Should raise exception if function does not support that extension"""
    df1 = create_dataframe()
    ext = ".stata"
    with tempfile.NamedTemporaryFile(suffix=ext) as temp, pytest.raises(InvalidParameter) as exc:
        df1.to_stata(temp.name)
        read_dataframe(temp.name, parse_dates=True, index_col="timestamp")

    assert f"unsupported file extension {ext}" in str(exc.value)


def test_read_dataframe_handle_missing():
    """Should return None if the file is missing"""
    filename = "/dataframe/file/does/not/exist"
    with pytest.raises(InvalidParameter) as exc:
        df = read_dataframe_handle_missing(filename)
        assert "director={} does not exist." in str()


def test_read_dataframe_by_substring():
    """Should return a dataframe if the file contains a substring."""
    # TODO: further test using files with desired naming patterns
    directory = os.path.join(tempfile.gettempdir(), "jade/")
    os.makedirs(directory, exist_ok=True)
    substring = "test"

    df = read_dataframe_by_substring(directory, substring)
    assert df is None


def test_read_dataframes_by_substrings():
    """Should return a dict of dataframes if the file contains desirsubstring."""
    # TODO: further test using files with desired naming patterns
    directory = os.path.join(tempfile.gettempdir(), "jade/")
    os.makedirs(directory, exist_ok=True)
    substrings = ["test", "hello"]

    dfs = read_dataframes_by_substrings(directory, substrings)
    for substring in substrings:
        assert substring not in dfs


@mark.parametrize("compress", [False, True])
def test_write_dataframe__csv(compress):
    """Should write dataframe into a file with matching extension"""
    df1 = create_dataframe()
    df_no_index = df1.reset_index()

    with tempfile.NamedTemporaryFile(suffix=".csv") as temp:
        expected_name = temp.name if not compress else temp.name + ".gz"
        write_dataframe(df_no_index, temp.name, compress=compress, keep_original=True, index=False)

        df2 = read_dataframe(expected_name, index_col="timestamp", parse_dates=True)
        assert_frame_equal(df1, df2)


@mark.parametrize("compress", [False, True])
def test_write_dataframe__feather(compress):
    """Should write dataframe into a file with matching extension"""
    df1 = create_dataframe()
    df_no_index = df1.reset_index()

    with tempfile.NamedTemporaryFile(suffix=".feather") as temp:
        expected_name = temp.name if not compress else temp.name + ".gz"
        write_dataframe(df_no_index, temp.name, compress=compress, keep_original=True)
        df2 = read_dataframe(expected_name, index_col="timestamp", parse_dates=True)
        assert_frame_equal(df1, df2)


@mark.parametrize("compress", [False, True])
def test_write_dataframe__h5(compress):
    """Should write dataframe into a file with matching extension"""
    df1 = create_dataframe()
    df_no_index = df1.reset_index()

    with tempfile.NamedTemporaryFile(suffix=".h5") as temp:
        write_dataframe(df_no_index, temp.name, compress=compress)
        df2 = read_dataframe(temp.name, index_col="timestamp", parse_dates=True)
        assert_frame_equal(df1, df2)


@mark.parametrize("compress", [False, True])
def test_write_dataframe__json(compress):
    """Should write dataframe into a file with matching extension"""
    df1 = create_dataframe()
    df_no_index = df1.reset_index()

    with tempfile.NamedTemporaryFile(suffix=".json") as temp:
        expected_name = temp.name if not compress else temp.name + ".gz"
        kwargs = {"orient": "index", "date_unit": "ns"}
        write_dataframe(df_no_index, temp.name, compress=compress, keep_original=True, **kwargs)
        df2 = read_dataframe(expected_name, index_col="timestamp", parse_dates=True, **kwargs)
        df2.set_index("timestamp", inplace=True)


def test_write_dataframe__invalid_parameter():
    """Should raise exception if file extension does not get support by this function"""
    with pytest.raises(InvalidParameter) as exc:
        df = create_dataframe()
        write_dataframe(df, os.path.join(tempfile.gettempdir(), "test.sql"))

    assert "unsupported file extension .sql" in str(exc.value)


# @mark.parametrize("compress", [False, True])
# def test_convert_csv_to_feather(compress):
#    """Should convert csv to feather file"""
#    df1 = create_dataframe()
#    df1.reset_index(inplace=True)
#    with tempfile.NamedTemporaryFile(suffix=".csv") as temp:
#        df1.to_csv(temp.name)
#        convert_csv_to_feather(temp.name, compress=compress, keep_original=True)
#        assert os.path.exists(temp.name)
#
#        expected_name = temp.name.replace(".csv", ".feather")
#        if compress:
#            expected_name += ".gz"
#        df2 = read_dataframe(expected_name, index_col="timestamp", parse_dates=True)
#        assert_frame_equal(df1, df2)
#        os.remove(expected_name)


# def test_convert_csvs_to_feather():
#    """Should convert many csv files to feather files"""
#    directory = tempfile.gettempdir()
#    exclude_substrings = ["test", "hello"]
#    convert_csvs_to_feather(directory, exclude_substrings=exclude_substrings)
