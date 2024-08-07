import requests
import gin
from greenflow import destroy, g, provision
from greenflow.adaptive import threshold
from greenflow.playbook import (
    deploy_k3s,
    p,
    kafka,
    prometheus,
    scaphandre,
    strimzi,
    redpanda,
)
from dataclasses import dataclass
from bpdb import set_trace, post_mortem
from sh import kubectl, helm
import click
from shlex import split
from contextlib import contextmanager

import logging
from logfmter import Logfmter

datefmt = "%Y.%m.%d.%a.%H-%M-%S"
formatter = Logfmter(
    keys=[
        "ts",
        "lvl",
        "at",
        "lno",
    ],
    mapping={
        "ts": "asctime",
        "lvl": "levelname",
        "at": "pathname",
        "lno": "lineno",
    },
    datefmt=datefmt,
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)


logging.basicConfig(handlers=[handler], level=logging.INFO)


def embed(globals, locals):
    from ptpython.repl import embed
    from os import getenv

    embed(
        history_filename=f"{getenv('DEVENV_ROOT')}/.devenv/.ptpython-history",
        globals=globals,
        locals=locals,
    )


ntfy_url = "https://ntfy.sh/4d5a7713-8b2a-46c8-8407-0014b19aa54a-greenflow"


def load_gin(exp_name="ingest-kafka"):
    with gin.unlock_config():
        gin.parse_config_files_and_bindings(
            [
                f"{g.g.gitroot}/gin/vmon-defaults.gin",
                f"{g.g.gitroot}/gin/g5k/defaults.gin",
                # f"{g.g.gitroot}/gin/g5k/paravance.gin",
                # f"{g.g.gitroot}/gin/g5k/parasilo.gin",
                f"{g.g.gitroot}/gin/g5k/montcalm.gin",
                # f"{g.g.gitroot}/gin/g5k/chirop.gin",
                # f"{g.g.gitroot}/gin/g5k/neowise.gin",
                f"{g.g.gitroot}/gin/{exp_name}.gin",
            ],
            [],
        )


def rebind_parameters(**kwargs):
    parameter_mapping = {
        "load": "greenflow.factors.exp_params.load",
        "instances": "greenflow.factors.exp_params.instances",
        "message_size": "greenflow.factors.exp_params.messageSize",
        "partitions": "greenflow.factors.exp_params.partitions",
        "bootstrap_servers": "greenflow.factors.kafka_bootstrap_servers",
        "redpanda_write_caching": "greenflow.factors.exp_params.redpanda_write_caching",
        "durationSeconds": "greenflow.factors.exp_params.durationSeconds",
    }

    with gin.unlock_config():
        for key, value in kwargs.items():
            if value is not None and key in parameter_mapping:
                gin.bind_parameter(parameter_mapping[key], value)


# Context manager for kafka setup and teardown
@contextmanager
def kafka_context():
    load_gin("ingest-kafka")
    p(kafka)
    yield
    kubectl(split("delete kafka theodolite-kafka"))
    helm(split("uninstall -n default kminion"))


@contextmanager
def redpanda_context():
    load_gin("ingest-redpanda")
    p(redpanda)
    yield
    helm(split("uninstall -n redpanda redpanda"))
    helm(split("uninstall -n redpanda kminion"))


@click.command("setup")
@click.argument("exp_name", type=str)
@click.option("--workers", type=int)
def setup(exp_name, workers):
    load_gin(exp_name)
    if workers is not None:
        with gin.unlock_config():
            gin.bind_parameter(
                "greenflow.g5k.G5KPlatform.get_conf.num_worker", int(workers)
            )
    try:
        provision.provision()
        deploy_k3s()
        p(prometheus)
        p(scaphandre)
        p(strimzi)
        # # Warm-up Kafka and Redpanda in the first time setup
        with kafka_context():
            pass
        with redpanda_context():
            pass
    except:
        send_notification("Error in setup. Dropped into shell")
        post_mortem()

    send_notification("Setup complete")


@click.command("ingest")
@click.argument("exp_name", type=str, default="ingest-redpanda")
@click.option("--load", type=str)
@click.option("--message_size", type=int)
@click.option("--instances", type=int)
@click.option("--partitions", type=int)
def ingest(exp_name, **kwargs):
    from greenflow.playbook import exp

    exp_description = (
        "cluster=montcalm type=threshold-explore instances=10 partitions=1"
    )

    message_sizes = [
        128,
        512,
    ] + list(range(1024, 10241, 1024))
    try:
        with kafka_context():
            logging.info(threshold("ingest-kafka", exp_description, message_sizes))
        with redpanda_context():
            logging.info(threshold("ingest-redpanda", exp_description, message_sizes))
    except:
        send_notification("Error in experiment. Debugging with shell")
        post_mortem()

    send_notification("Experiment complete. On to the next.")


@click.command("killjob")
def killjob():
    load_gin()
    destroy.killjob()


def send_notification(text, priority="low"):
    requests.post(ntfy_url, headers={"priority": priority}, data=text, timeout=10)


@click.command("i")
@click.argument("exp_name", type=str, default="ingest-redpanda")
@click.option("--load", type=str)
@click.option("--message_size", type=int)
@click.option("--instances", type=int)
@click.option("--partitions", type=int)
def i(exp_name, **kwargs):
    load_gin(exp_name)
    rebind_parameters(**kwargs)
    embed(globals(), locals())


@click.group()
def cli():
    pass


cli.add_command(setup)
cli.add_command(ingest)
cli.add_command(i)
cli.add_command(killjob)


if __name__ == "__main__":
    cli()
