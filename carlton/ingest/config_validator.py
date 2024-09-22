# config_validator.py
from carlton.utils.validator import Validator


class ConfigValidator:
    @staticmethod
    def validate_args(required_args, config):
        """
        Validate that all required arguments are present in the config.

        :param required_args: List of required argument keys.
        :param config: Configuration dictionary to validate.
        """
        Validator.validate_args(required_args, config)
