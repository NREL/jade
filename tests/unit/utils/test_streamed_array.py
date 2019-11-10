"""
Unit test for utility functions in StreamedArray class
"""
import numpy as np
from numpy.testing import assert_array_equal
from jade.utils.streamed_array import StreamedArray


def test_streamed_array():
    """Test streamed array"""
    volt = StreamedArray(max_size=4)

    # add line
    volt.add_line([1, 2, 3, 4])
    volt.add_line([5, 6, 7, 8])
    volt.add_line([9, 10, 11, 12])

    assert len(volt.array) == 3

    # get lines
    lines = volt.get_lines([1, 2])
    expected = np.array([[5, 6, 7, 8], [9, 10, 11, 12]])
    assert_array_equal(lines, expected)

    # add line
    volt.add_line([13, 14, 15, 16])
    volt.add_line([17, 18, 19, 20])
    volt.add_line([21, 22, 23, 24])

    assert len(volt.array) == 4
    assert volt.array == [
        [9, 10, 11, 12],
        [13, 14, 15, 16],
        [17, 18, 19, 20],
        [21, 22, 23, 24]
    ]

    # get lines
    lines = volt.get_lines([3, 4])
    expected = np.array([[13., 14., 15., 16.], [17., 18., 19., 20.]])
    assert_array_equal(lines, expected)

    # average
    avg = volt.average(axis=0)
    expected = np.array([15., 16., 17., 18.])
    assert_array_equal(avg, expected)

    mask_avg = volt.average(mask=[3, 4], axis=0)
    expected = np.array([15., 16., 17., 18.])
    assert_array_equal(mask_avg, expected)
