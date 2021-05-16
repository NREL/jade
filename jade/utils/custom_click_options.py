"""Defines custom click options for cli commands"""

import click


class CustomOptions(click.Option):
    """Custom option class extending base click option"""

    def __init__(self, *args, **kwargs):
        if "allowed_values" in kwargs:
            self.allowed_values = kwargs.pop("allowed_values")

        if "not_required_if" in kwargs:
            self.not_required_if = kwargs.pop("not_required_if")

        if "required_if" in kwargs:
            self.required_if = kwargs.pop("required_if")

        super(CustomOptions, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        self.handle_custom_options(opts)

        return super(CustomOptions, self).handle_parse_result(ctx, opts, args)

    def handle_custom_options(self, opts):
        """Handles custom options that have been created"""
        if hasattr(self, "allowed_values") and isinstance(self.allowed_values, list):
            if self.name in opts and opts[self.name] not in self.allowed_values:
                raise ValueError(
                    "Invalid value given, only allowed values are " + f"{self.allowed_values}"
                )

        if hasattr(self, "not_required_if"):
            not_required_if_present = self.not_required_if in opts.keys()

            if not_required_if_present:
                self.required = False
                self.prompt = None

        if hasattr(self, "required_if"):
            required_if_present = self.required_if in opts.keys()

            if required_if_present and opts[self.required_if] is not None:
                self.required = True
