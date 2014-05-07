from importlib import reload
import logging
import pytest

from fusegridfs.log import setup_logging


@pytest.yield_fixture
def sanitize_logging():
    logging.shutdown()
    reload(logging)
    yield
    logging.shutdown()
    reload(logging)


@pytest.mark.parametrize('debug,expected_level', [
    (True, logging.DEBUG),
    (False, logging.INFO),
])
def test_setup_logging(sanitize_logging, debug, expected_level):
    setup_logging(debug)
    assert logging.getLogger().getEffectiveLevel() is expected_level
