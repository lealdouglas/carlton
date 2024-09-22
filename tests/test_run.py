from unittest.mock import MagicMock, patch

import pytest

from carlton.run import main, process_args


def test_process_args():
    """
    Testa a função process_args.
    Tests the process_args function.
    """
    args = ['-arg1', 'value1', '-arg2', 'value2']
    expected_output = {'arg1': 'value1', 'arg2': 'value2'}
    assert process_args(args) == expected_output
