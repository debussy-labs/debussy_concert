import yaml
import re
import os


env_var_tag = '!env_var'
env_var_regexp = re.compile(r'.*\$\{([^}^{]+)\}')


def env_var_constructor(loader: yaml.Loader, node: yaml.Node):
    node_value = loader.construct_scalar(node)
    group_regexp = re.compile(r'\$\{([^}^{]+)\}')
    regexp_matches = group_regexp.findall(node_value)
    for match in regexp_matches:
        env_var_value = os.getenv(match)
        matched__replacement_str = '${' + match + '}'
        if env_var_value is None:
            raise RuntimeError(f"Environment variable not found: {match}")
        node_value = node_value.replace(matched__replacement_str, env_var_value)
    return node_value


class DebussyYamlSafeLoader(yaml.SafeLoader):
    pass


DebussyYamlSafeLoader.add_implicit_resolver(env_var_tag, env_var_regexp, None)
DebussyYamlSafeLoader.add_constructor(env_var_tag, env_var_constructor)
