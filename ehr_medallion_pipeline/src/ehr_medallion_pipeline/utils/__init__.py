import yaml
from pathlib import Path

def load_config(env: str = "dev") -> dict:
    """
    Load pipeline config for the given env

    Args:
        env: environment e.g 'dev' or 'prod'

    Returns:
        dict: config parameters
    """

    config_path = Path(__file__).parent.parent.parent.parent / "config" / f"{env}.yml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. "
            f"Make sure config/{env}.yml exists."
        )

    with open(config_path, "r") as f:
        return yaml.safe_load(f)