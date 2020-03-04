import abc


class AbstractBatchPostProcess(abc.ABC):

    @abc.abstractmethod
    def run(self, data, output):
        pass
