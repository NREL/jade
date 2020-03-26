import os
import tempfile

from jade.jobs.batch_post_process import BatchPostProcess


def test_batch_post_process():
    """Should run batch post process"""
    
    commands = [
        "ls .",
        "ls an-invalid-path"
    ]
    
    with tempfile.NamedTemporaryFile("w") as f:
        f.write("\n".join(commands))
        f.seek(0)
        bpp = BatchPostProcess(config_file=f.name)
    
        data = bpp.serialize()
        assert os.path.exists(data["file"])

        config = bpp.auto_config()
        assert config.get_num_jobs() == 2
        
        tempdir = tempfile.gettempdir()
        bpp_config_file = os.path.join(tempdir, "bpp-config.json")
        config.dump(bpp_config_file)
        assert os.path.exists(bpp_config_file)
        
        if os.path.exists(bpp_config_file):
            os.remove(bpp_config_file)
