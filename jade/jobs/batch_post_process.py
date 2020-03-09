import abc

from jade.extensions.registry import Registry, ExtensionClassType


class BatchPostProcess:
    """A class for configuring batch-post process extension.
    """

    def __init__(self, extension):
        self._extension = extension
    
    @property
    def name(self):
        """The name of batch post-process"""
        return "batch-post-proces"
    
    @property
    def extension(self):
        return self._extension
    
    def auto_config(self, inputs, **kwargs):
        """A wrapper function for creating config object 
        with given configuration class used for batch post-process.
        
        Parameters
        ----------
        inputs : str
            The inputs directory.
        """
        registry = Registry()
        config_class = registry.get_extension_class(
            extension_name=self.extension,
            class_type=ExtensionClassType.CONFIGURATION
        )
        config = config_class.auto_config(inputs, **kwargs)
        return config
    
    def serialize(self):
        """Serialize batch post-process object"""
        data = {
            "name": self.name,
            "extension": self.extension
        }
    