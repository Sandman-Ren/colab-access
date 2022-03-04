
TOOLBOX_CONFIG = {
    'host': '34.139.85.83',
    'port': 6379,
    'db': 0
}

def get_key_from_values(key_format: str, **kwargs) -> str:
    return key_format.format(**kwargs)