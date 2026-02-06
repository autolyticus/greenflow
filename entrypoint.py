import traceback
import requests
import gin
from greenflow.adaptive import *
from greenflow.glue import (
    embed,
    ntfy_url,
    patch_global_g,
    setup_gin_config,
    kafka_context,
    kafka_kraft_context,
    redpanda_context,
)
from greenflow.playbook import (
    deploy_k3s,
    p,
    prometheus,
    scaphandre,
    strimzi,
    kafka,
    redpanda,
)
from dataclasses import dataclass
from bpdb import set_trace, post_mortem
import click
import kr8s

import logging
from logfmter import Logfmter
from click.testing import CliRunner


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


logging.basicConfig(handlers=[handler], level=logging.WARN)
# logging.basicConfig(handlers=[handler], level=logging.INFO)

_gin_loaded = {}  # Module-level cache to track loaded configurations


def load_gin(exp_name="ingest-redpanda", cluster="grappe", test=False):
    cache_key = (exp_name, test)

    # If this exact configuration was already loaded, return early
    if cache_key in _gin_loaded:
        return

    if test:
        os.environ["EXPERIMENT_STORAGE_URL"] = os.environ["TEST_EXPERIMENT_STORAGE_URL"]
        os.environ["PROMETHEUS_URL"] = os.environ["TEST_PROMETHEUS_URL"]
        os.environ["EXPERIMENT_PUSHGATEWAY_URL"] = os.environ[
            "TEST_EXPERIMENT_PUSHGATEWAY_URL"
        ]
        os.environ["DASHBOARD_BASE_URL"] = os.environ["TEST_DASHBOARD_BASE_URL"]
        os.environ["NTFY_URL"] = os.environ["TEST_NTFY_URL"]
        g = patch_global_g("test")
        config_files = [
            "test-platform.gin",
            f"{exp_name}.gin",
            "test-platform.gin",
        ]
    else:
        g = patch_global_g("production")
        config_files = [
            # "vmon-defaults.gin",
            "aws/defaults.gin",
            "aws/t3.small.gin",
            # "g5knos/defaults.gin",
            # "g5k/paravance.gin",
            # "g5k/parasilo.gin",
            # "g5k/paradoxe.gin",
            # "g5k/montcalm.gin",
            # "g5k/chirop.gin",
            # "g5k/neowise.gin",
            # f"g5knos/{cluster}.gin",
            # "g5k/ecotype.gin",
            # "g5k/gros.gin",
            # "g5k/grappe.gin",
            # "g5k/chiclet.gin",
            f"ingest_base.gin",
            f"{exp_name}.gin",
        ]

    setup_gin_config(g, exp_name, config_files)
    return g


def rebind_parameters(**kwargs):
    parameter_mapping = {
        "bootstrap_servers": "greenflow.factors.kafka_bootstrap_servers",
        "brokerCpu": "broker_cpu/macro.value",
        "brokerMem": "greenflow.factors.exp_params.broker_mem",
        "brokerReplicas": "greenflow.factors.exp_params.broker_replicas",
        "consumerInstances": "greenflow.factors.exp_params.consumer_instances",
        "durationSeconds": "greenflow.factors.exp_params.durationSeconds",
        "load": "greenflow.factors.exp_params.load",
        "messageSize": "greenflow.factors.exp_params.messageSize",
        "partitions": "greenflow.factors.exp_params.partitions",
        "producerInstances": "greenflow.factors.exp_params.producer_instances",
        "redpanda_write_caching": "greenflow.factors.exp_params.redpanda_write_caching",
        "replicationFactor": "greenflow.factors.exp_params.replicationFactor",
        "topic_name": "greenflow.factors.exp_params.topic_name",
    }

    succeeded_rebindings = {}
    with gin.unlock_config():
        for key, value in kwargs.items():
            if value is not None and key in parameter_mapping:
                gin.bind_parameter(parameter_mapping[key], value)
                succeeded_rebindings[key] = value
    logging.warning({"msg": "Rebound", **succeeded_rebindings})


@click.command("ingest")
@click.argument("cluster", type=str)
@click.argument("exp_description", type=str)
def ingest_set(cluster, exp_description, **kwargs):
    load_gin(exp_name="ingest-kafka", cluster=cluster)
    exp_description = f"cluster={cluster} " + exp_description
    _ingest_set(exp_description)


def _ingest_set(exp_description):
    from greenflow.protocols import (
        demonstrate_binary_search,
        scaling_behaviour,
        memory_cpu_impact_10_10_60,
        memory_cpu_impact_1_1_1,
        proportionality,
        smoketest,
        latency,
        partitioning,
        idle,
        system,
        baseline,
        safety_curve,
    )

    if "type=scalingBehaviour" in exp_description:
        scaling_behaviour(exp_description)
    elif "type=baseline" in exp_description:
        baseline(exp_description)
    elif "type=demonstrate" in exp_description:
        demonstrate_binary_search(exp_description)
    elif "type=safetyCurve" in exp_description:
        safety_curve(exp_description)
    elif "type=memImpact" in exp_description:
        # memory_cpu_impact_1_1_1(exp_description)
        memory_cpu_impact_10_10_60(exp_description)
    elif "type=proportionality" in exp_description:
        proportionality(exp_description)
    elif "type=smoketest" in exp_description:
        smoketest(exp_description)
    elif "type=latency" in exp_description:
        latency(exp_description)
    elif "type=partitioning" in exp_description:
        partitioning(exp_description)
    elif "type=idle" in exp_description:
        idle(exp_description)
    elif "type=system" in exp_description:
        system(exp_description)


@click.command("srun")
@click.argument("cluster", type=str)
@click.option("--num_worker", type=int, default=1)
@click.option("--num_broker", type=int, default=3)
@click.argument("exp_description", type=str)
def setup_and_run(cluster, num_worker, num_broker, exp_description):
    _setup(cluster, num_worker, num_broker)
    exp_description = f"cluster={cluster} " + exp_description
    _ingest_set(exp_description)


@click.command("setup")
@click.argument("cluster", type=str, default="grappe")
@click.option("--num_worker", type=int, default=1)
@click.option("--num_broker", type=int, default=3)
def setup(cluster, num_worker, num_broker):
    _setup(cluster, num_worker, num_broker)


def _setup(cluster, num_worker, num_broker):
    load_gin(exp_name="ingest-kafka", cluster=cluster)

    from greenflow import provision

    if num_worker is not None:
        with gin.unlock_config():
            logging.warning(f"Setting num_worker to {num_worker}")
            gin.bind_parameter("greenflow.g5k.G5KPlatform.get_conf.num_worker", num_worker)
            gin.bind_parameter(
                "greenflow.g5knos.G5KNixOSPlatform.get_conf.num_worker", num_worker
            )
            gin.bind_parameter("greenflow.aws.AwsPlatform.get_conf.num_worker", num_worker)
            gin.bind_parameter("greenflow.factors.exp_params.num_worker", num_worker)
    if num_broker is not None:
        with gin.unlock_config():
            logging.warning(f"Setting num_broker to {num_broker}")
            gin.bind_parameter("greenflow.g5k.G5KPlatform.get_conf.num_broker", num_broker)
            gin.bind_parameter(
                "greenflow.g5knos.G5KNixOSPlatform.get_conf.num_broker", num_broker
            )
            gin.bind_parameter("greenflow.aws.AwsPlatform.get_conf.num_broker", num_broker)
            gin.bind_parameter("greenflow.factors.exp_params.num_broker", num_broker)
    try:
        provision.provision()
        # deploy_k3s()
        p(prometheus)
        # p(scaphandre)
        p(strimzi)
        # Warm-up Kafka and Redpanda in the first time setup
        # with kafka_context():
        #     pass
        # with redpanda_context():
        #     pass
    except KeyboardInterrupt:
        return
    except:
        send_notification("Error in setup. Dropped into shell")
        post_mortem()

    send_notification("Setup complete")


@click.command("test")
@click.argument("exp_name", type=str, default="ingest-redpanda")
def test(exp_name: str):
    load_gin(exp_name=exp_name, test=True)

    from greenflow.exp_ng.exp_ng import exp as exp, killexp
    from greenflow.exp_ng.hammer import hammer
    from greenflow import provision

    logging.warning({"exp_name": exp_name})
    # provision.provision()
    # deploy_k3s()
    # p(
    # p(prometheus)
    # p(strimzi)
    # with redpanda_context():
    #     ...
    # with kafka_context():
    #     ...
    # p(kafka)
    # p(redpanda)
    # rebind_parameters(
    #     load=5.0 * 10**3,
    #     messageSize=1024,
    #     durationSeconds=100,
    # )
    hammer()
    # messageSizes = [
    #     128,
    #     512,
    # ] + list(range(1024, 10241, 1024))
    # threshold_hammer("test", messageSizes)


@click.command("killjob")
@click.argument("cluster", type=str)
def killjob(cluster):
    load_gin(cluster=cluster)

    from greenflow import destroy

    destroy.killjob()


@click.command("kexp")
@click.argument("exp_name", type=str, default="ingest-redpanda")
def kexp(exp_name: str):
    load_gin(exp_name=exp_name)

    from greenflow.exp_ng.exp_ng import killexp

    killexp()


@click.command("tkexp")
@click.argument("exp_name", type=str, default="ingest-redpanda")
def tkexp(exp_name: str):
    load_gin(exp_name=exp_name, test=True)
    from greenflow.exp_ng.exp_ng import killexp

    killexp()


def send_notification(text, priority="low"):
    requests.post(ntfy_url, headers={"priority": priority}, data=text, timeout=10)


@click.command("send")
@click.argument("text", nargs=-1, type=str)
@click.option("--priority", type=str, default="low")
def send(text, priority="low"):
    message = " ".join(text)
    requests.post(ntfy_url, headers={"priority": priority}, data=message, timeout=10)


@click.command("i")
@click.argument("exp_name", type=str, default="ingest-redpanda")
@click.option("--load", type=str)
@click.option("--messageSize", type=int)
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
cli.add_command(ingest_set)
cli.add_command(i)
cli.add_command(killjob)
cli.add_command(tkexp)
cli.add_command(kexp)
cli.add_command(test)
cli.add_command(send)
cli.add_command(setup_and_run)


if __name__ == "__main__":
    cli()
