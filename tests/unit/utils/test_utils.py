"""
Unit tests for utility functions
"""
import stat
import tempfile

from mock import patch
import pytest
from pytest import mark

from jade.utils.utils import *


def test_create_chunks():
    """Should return chunked item"""
    items = list(range(0, 100))
    size = 3

    chunks = create_chunks(items, size)

    current = next(chunks)
    assert len(current) == size
    assert current == [0, 1, 2]

    current = next(chunks)
    assert current == [3, 4, 5]


@mark.parametrize("executable, expected", [(True, 33252), (False, 33188)])
def test_create_script(executable, expected):
    """Should create script with given text"""
    filename = os.path.join(tempfile.gettempdir(), "hello_world")
    text = "echo 'Hello World'"

    create_script(filename, text, executable)
    assert os.path.exists(filename)

    # Disabling because it doesn't work on Windows.
    # s = os.stat(filename)
    # assert s.st_mode == expected

    if os.path.exists(filename):
        os.remove(filename)


def test_make_file_read_only():
    """Should change file's mode to read-only"""

    filename = os.path.join(tempfile.gettempdir(), "jade-test-file.txt")
    if os.path.exists(filename):
        os.chmod(filename, stat.S_IWRITE)
        os.remove(filename)

    with open(filename, "w") as f:
        f.write("Hello World")

    prev_mode = os.stat(filename)
    make_file_read_only(filename)
    # Disabling because it doesn't work on Windows.
    # s = os.stat(filename)
    # assert s.st_mode != prev_mode
    # assert s.st_mode == 33060

    if os.path.exists(filename):
        os.chmod(filename, stat.S_IWRITE)
        os.remove(filename)


@patch("jade.utils.utils.make_file_read_only")
def test_make_directory_read_only(mock_make_file_read_only):
    """Should set all files in the directory read only"""

    tmpdir = os.path.join(tempfile.gettempdir(), "jade-test-tmp87alkj8ew")
    os.makedirs(tmpdir, exist_ok=True)

    tmpfile = os.path.join(tmpdir, "jade-test-file.txt")
    with open(tmpfile, "w") as f:
        f.write("Hello World")

    make_directory_read_only(tmpdir)
    mock_make_file_read_only.assert_called()

    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


def test_data_dump_and_load():
    """Should dump data using according module based on file extension"""
    raw_data = {"A": 1, "B": 2}

    # Dump json
    json_file = os.path.join(tempfile.gettempdir(), "jade-unit-test-file.json")
    dump_data(raw_data, json_file)
    assert os.path.exists(json_file)

    # Load json
    json_data = load_data(json_file)
    assert json_data == raw_data

    if os.path.exists(json_file):
        os.remove(json_file)

    # Dump toml
    toml_file = os.path.join(tempfile.gettempdir(), "jade-unit-test-file.toml")
    dump_data(raw_data, toml_file)
    assert os.path.exists(toml_file)

    # Load toml
    toml_data = load_data(toml_file)
    assert toml_data == raw_data

    if os.path.exists(toml_file):
        os.remove(toml_file)

    # Re-enable if we add support again.
    # Dump yaml
    # yaml_file = os.path.join(tempfile.gettempdir(), "jade-unit-test-file.yaml")
    # dump_data(raw_data, yaml_file)
    # assert os.path.exists(yaml_file)

    ## Load yaml
    # yaml_data = load_data(yaml_file)
    # assert yaml_data == raw_data

    # if os.path.exists(yaml_file):
    #    os.remove(yaml_file)


def test_aggregate_data_from_files():
    """Should aggregate data as expected"""
    tmpdir = os.path.join(tempfile.gettempdir(), "jade-test-tmp87alkj8ew")
    os.makedirs(tmpdir, exist_ok=True)

    for name in ["a_control.json", "b_control.json"]:
        filename = os.path.join(tmpdir, name)
        dump_data({"A": 1, "B": 2}, filename)

    data = aggregate_data_from_files(tmpdir, "_control.json")
    expected = [{"A": 1, "B": 2}, {"A": 1, "B": 2}]

    assert data == expected

    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


def test_rmtree():
    """Should remove dir properly"""

    tmpdir = os.path.join(tempfile.gettempdir(), "jade-test-tmp87alkj8ew")
    os.makedirs(tmpdir, exist_ok=True)

    assert os.path.exists(tmpdir)
    rmtree(tmpdir)


def test_modify_file():
    """Should modify file properly"""

    def replace_word(line):
        return line.replace("World", "Disco")

    txt_file = os.path.join(tempfile.gettempdir(), "jade-unit-test-file.txt")
    if os.path.exists(txt_file):
        os.remove(txt_file)
    with open(txt_file, "w") as f:
        f.write("Hello World")

    modify_file(txt_file, replace_word)
    with open(txt_file, "r") as f:
        data = f.read()
        assert data == "Hello Disco"

    if os.path.exists(txt_file):
        os.remove(txt_file)


@mark.skip
def test_cli_string():
    """Should return command-line arguments issued"""
    cmd = get_cli_string()
    assert "pytest" in cmd


def test_handle_key_error():
    """Test decorator to catch KeyError exception"""

    @handle_key_error
    def get_item(key):
        data = {"A": 1, "B": 2}
        return data[key]

    value = get_item("A")
    assert value == 1

    with pytest.raises(InvalidParameter) as exc:
        get_item("C")

    assert "C" in str(exc.value)


def test_decompress_file():
    """Should decompress file properly"""
    gz_file = os.path.join(
        tempfile.gettempdir(),
        "jade-unit-test-file.gz",
    )
    with gzip.open(gz_file, "wb") as f:
        f.write(b"Hello World")
    assert os.path.exists(gz_file)

    new_file = decompress_file(gz_file)
    assert os.path.exists(new_file)
    with open(new_file, "r") as f:
        data = f.read()
        assert data == "Hello World"

    if os.path.exists(gz_file):
        os.remove(gz_file)

    if os.path.exists(new_file):
        os.remove(new_file)


def test_get_directory_size_bytes():
    """Test calculation of sizes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir2 = os.path.join(tmpdir, "tmp")
        os.makedirs(tmpdir2, exist_ok=True)
        files = [
            os.path.join(tmpdir, "file1.bin"),
            os.path.join(tmpdir, "file2.bin"),
            os.path.join(tmpdir, "tmp", "file3.bin"),
            os.path.join(tmpdir, "tmp", "file4.bin"),
        ]
        data = "1234567890"
        for filename in files:
            with open(filename, "w") as f_out:
                f_out.write(data)

        assert get_directory_size_bytes(tmpdir, recursive=True) == 40
        assert get_directory_size_bytes(tmpdir, recursive=False) == 20


def test_get_filenames_in_path():
    """Should filter filename properly"""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir2 = os.path.join(tmpdir, "tmp")
        os.makedirs(tmpdir2, exist_ok=True)

        data = {"A": 1, "B": 2}
        json_file1 = os.path.join(tmpdir, "a.json")
        json_file2 = os.path.join(tmpdir2, "a.json")
        dump_data(data, json_file1)
        dump_data(data, json_file2)

        # These should not get included.
        toml_file1 = os.path.join(tmpdir, "b.toml")
        toml_file2 = os.path.join(tmpdir2, "b.toml")
        dump_data(data, toml_file1)
        dump_data(data, toml_file2)

        filenames = list(get_filenames_in_path(tmpdir, "a.json"))
        assert filenames == [json_file1, json_file2]


def test_get_filenames_by_ext():
    """Should filter filename properly"""
    tmpdir = os.path.join(tempfile.gettempdir(), "jade-test-tmp87alkj8ew")
    os.makedirs(tmpdir, exist_ok=True)

    data = {"A": 1, "B": 2}
    json_file = os.path.join(tmpdir, "a.json")
    dump_data(data, json_file)

    toml_file = os.path.join(tmpdir, "b.toml")
    dump_data(data, toml_file)

    filenames = get_filenames_by_ext(tmpdir, ".json")
    assert "a.json" in next(filenames)

    filenames = get_filenames_by_ext(tmpdir, ".toml")
    assert "b.toml" in next(filenames)


def test_interpret_datetime():
    """Should return formatted datetime string"""
    timestamps = [
        "2019-01-01 01:01:01",
        "2019-01-01 01:01:01.000001",
        "2019-01-01T01:01:01Z",
        "2019-01-01T01:01:01.000001Z",
        "2019-01-01_01:01:01.000001",
        "2019-01-01_01-01-01-000000",
    ]

    for timestamp in timestamps:
        dt = interpret_datetime(timestamp)
        assert isinstance(dt, datetime)
        if "." in timestamp:
            assert dt == datetime(2019, 1, 1, 1, 1, 1, 1)
        else:
            assert dt == datetime(2019, 1, 1, 1, 1, 1)


def test_rotate_filenames():
    """Should rotate filenames in directory"""
    tmpdir = os.path.join(tempfile.gettempdir(), "jade-test-tmp87alkj8ew")
    os.makedirs(tmpdir, exist_ok=True)

    data = {"A": 1, "B": 2}
    json_file1 = os.path.join(tmpdir, "a1.json")
    dump_data(data, json_file1)

    json_file2 = os.path.join(tmpdir, "a2.json")
    dump_data(data, json_file2)

    rotate_filenames(tmpdir, ".json")

    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)


def test_check_filename():
    valid = [
        "foo",
        "1234",
        "foo-1234_bar",
        "foo.txt",
    ]
    invalid = [
        "",
        "foo:",
        "foo!",
        "/foo",
        "\\foo",
        "bar,",
        "x" * (MAX_PATH_LENGTH + 1),
    ]

    for name in valid:
        check_filename(name)

    for name in invalid:
        with pytest.raises(InvalidParameter):
            check_filename(name)
