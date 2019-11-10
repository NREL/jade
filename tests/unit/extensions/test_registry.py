"""Test registry."""

from jade.extensions.registry import show_extensions, EXTENSION_REGISTRY


def test_show_extensions(capsys):
    """Test functionality of show_extensions."""
    show_extensions()
    captured = capsys.readouterr()
    for extension in EXTENSION_REGISTRY:
        assert extension in captured.out
