from contextlib import contextmanager
from shlex import split
import gin
from sh import helm, kubectl
from greenflow.adaptive import os
import greenflow.g
from greenflow.playbook import kafka, kafka_kraft, p, redpanda


def embed(globals, locals):
    from ptpython.repl import embed
    from os import getenv

    embed(
        history_filename=f"{getenv('DEVENV_ROOT')}/.devenv/.ptpython-history",
        globals=globals,
        locals=locals,
    )


# Configure NTFY_URL in .secrets.env to get notifications on your phone!
# Once the experiment is complete or if there is an error, you will get a notification
ntfy_url = os.getenv("NTFY_URL", "http://ntfy.sh/YOUR_URL_HERE")


def patch_global_g(deployment_type, storage_type="mongo"):
    import greenflow.g
    from greenflow.g import _g
    import gin

    with gin.unlock_config():
        gin.bind_parameter("greenflow.g._g.deployment_type", deployment_type)
        gin.bind_parameter("greenflow.g._g.storage_type", storage_type)
    g = _g.get_g()
    try:
        _ = greenflow.g.g
    except AttributeError:
        greenflow.g.g = g
    from greenflow import provision, destroy

    return g


def setup_gin_config(g, exp_name, config_files):
    import gin
    import os
    
    gin_path = f"{g.gitroot}/gin"
    
    # Debug: verify the path exists
    if not os.path.exists(gin_path):
        raise ValueError(f"Gin config directory does not exist: {gin_path}")
    
    # Clear and set search paths explicitly

    
    # Debug: print what we're about to parse
    # print(f"Gin search paths: {gin.config._CONFIG_FILE_SEARCH_PATHS}")
    # print(f"Config files to parse: {config_files}")

    with gin.unlock_config():
        gin.add_config_file_search_path(gin_path)
        gin.parse_config_files_and_bindings(
            [f"{g.gitroot}/gin/{file}" for file in config_files],
            [],
        )



@contextmanager
def kafka_context():
    # from entrypoint import load_gin
    # load_gin("ingest-kafka")
    p(kafka)
    yield
    kubectl(split("delete kafka theodolite-kafka"))
    helm(split("uninstall -n default kminion"))

@contextmanager
def kafka_kraft_context():
    p(kafka_kraft)
    yield
    kubectl(split("delete kafka theodolite-kafka"))
    kubectl(split("delete kafkanodepool --all")) 
    helm(split("uninstall -n default kminion"))



@contextmanager
def redpanda_context():

    # from entrypoint import load_gin
    # load_gin("ingest-redpanda")
    p(redpanda)
    yield
    helm(split("uninstall -n redpanda redpanda"))
    helm(split("uninstall -n redpanda kminion"))
    kubectl(split("delete -n redpanda pvc --all"))
