from logging import basicConfig, DEBUG, INFO


def setup_logging(debug=False):
    basicConfig(level=debug and DEBUG or INFO)
