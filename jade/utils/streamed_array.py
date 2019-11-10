"""Defines StreamedArray."""

import numpy as np

class StreamedArray:
    """
    StreamedArray class.

    Usage

    Instantiation
        >>>from streamed_array import StreamedArray
        >>>volt = StreamedArray(max_size=4)

    Add lines
        >>>volt.add_line([1,2,3,4])
        >>>volt.add_line([5,6,7,8])
        >>>volt.add_line([9,10,11,12])

    Get lines
        >>>volt.get_lines([1,2])
        array([[ 5,  6,  7,  8], [ 9, 10, 11, 12]])

    Add more lines
        >>>volt.add_line([13,14,15,16])
        >>>volt.add_line([17,18,19,20])
        >>>volt.add_line([21,22,23,24])

    Look at the array
        >>>volt.array
        [[9, 10, 11, 12], [13, 14, 15, 16], [17, 18, 19, 20], [21, 22, 23, 24]]

    Get lines
        >>>volt.get_lines([3,4])
        array([[13, 14, 15, 16], [17, 18, 19, 20]])

    Average
        >>>volt.average(axis=0)
        array([ 15.,  16.,  17.,  18.])

        >>>volt.average(mask=[3,4], axis=0)
        array([ 15.,  16.,  17.,  18.]) #Same because the example is trivial...

    **Author**
    Nicolas Gensollen. nicolas.gensollen@nrel.gov
    """
    def __init__(self, **kwargs):
        """Class Constrcutor."""
        #Internal list of lists representation
        self.array = []

        #Index of the first line.
        #Initialized at 0
        self.index = 0

        #Maximum number of lines that can be stored in memory
        if "max_size" in kwargs:
            self.max_size = kwargs["max_size"]
        else:
            self.max_size = 100 #Default max size. You can probably put a large number here...

    def add_line(self, line):
        """
        Add a new line to the array.
        If the addition of this line causes the array to get larger than the max size,
        then delete the first half of the array and update the index.
        """
        #If the size will get bigger than the limit...
        if len(self.array) + 1 > self.max_size:
            #...only keep the second half of the array in memory
            self.array = self.array[int(self.max_size/2.0)-1:]
            #Update the index
            self.index += int(self.max_size/2.0)-1
        #Finally add the new line to the array
        self.array.append(line)

    def average(self, mask=None, axis=0):
        """
        Returns the average over the given axis.
        You can implement any numpy function in the same way here.
        """
        if mask is None:
            ar = np.array(self.array)
            return np.mean(ar,axis=axis)
        else:
            return np.mean(self.get_lines(mask),axis=axis)

    def get_lines(self, mask):
        """Returns the requested lines. Mask should be a list of indices"""
        #Rescaled the mask using the index of the first line
        rescaled_mask = [x - self.index for x in mask]
        ar = np.array(self.array)
        return ar[rescaled_mask]
