# config_validator.py
from carlton.utils.logger import log_info
from carlton.utils.validator import Validator


class ConfigValidator:
    @staticmethod
    def validate_args(required_args, config):
        """
        Validate that all required arguments are present in the config.

        :param required_args: List of required argument keys.
        :param config: Configuration dictionary to validate.
        """

        log_info(f'Validating configuration arguments: {required_args}')
        Validator.validate_args(required_args, config)
