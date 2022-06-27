import yaml
import os
from debussy_concert.core.service.debussy_yaml_safe_loader import DebussyYamlSafeLoader


def test_yaml_parsing():
    os.environ['DEBUSSY_CONCERT__TEST_ENV'] = 'my_test_env_var'
    yaml_data = """
    test: ${DEBUSSY_CONCERT__TEST_ENV}
    """
    parsed_data = yaml.load(yaml_data, Loader=DebussyYamlSafeLoader)
    assert parsed_data == {'test': 'my_test_env_var'}
