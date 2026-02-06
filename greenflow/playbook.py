#!/usr/bin/env python3

from sys import exit
import ansible_runner

from .factors import factors
from .state import get_deployment_state_vars, get_experiment_state_vars

__all__ = [
    "p",
    "exp",
    "deploy_k3s",
    "scaphandre",
    "prometheus",
    "strimzi",
    "kafka",
    "kafka_kraft",
    "theodolite",
    "killexp",
    "redpanda",
    "redpanda_test",
    "kminion",
    "base",
    "blowaway",
]

# extra_vars = get_deployment_state_vars() | get_experiment_state_vars() | factors()


def _playbook(playbook_name, extra=None):
    from .g import g

    extra = extra or {}
    rc = ansible_runner.run(
        playbook=playbook_name,
        private_data_dir=f"{g.gitroot}/ansible",
        extravars=extra,
    ).rc
    if rc > 0:
        raise SystemExit("Error: Playbook execution interrupted/failed.")


scaphandre = "scaphandre"
prometheus = "prometheus"
strimzi = "strimzi"
kafka = "kafka"
kafka_kraft = "kafka-kraft"
theodolite = "theodolite"
killexp = "killexp"
redpanda = "redpanda"
redpanda_test = "redpanda_test"
kminion = "kminion"
base = "base"
blowaway = "blowaway"
aws_deploy_k3s = "blowaway"


def p(playbook_name_without_yaml):
    extra_vars = get_deployment_state_vars() | get_experiment_state_vars() | factors()
    _playbook(f"{playbook_name_without_yaml}.yaml", extra_vars)


def quirks(flavour):
    extra_vars = get_deployment_state_vars() | get_experiment_state_vars() | factors()
    extra_vars["flavour"] = flavour
    _playbook("quirks.yaml", extra_vars)


def deploy_k3s():
    _playbook("deploy_k3s.yaml", get_deployment_state_vars())

def deploy_nos_k3s():
    _playbook("nos_deploy_k3s.yaml", get_deployment_state_vars() | get_experiment_state_vars() | factors())


def deploy_aws_k3s():
    _playbook("aws_deploy_k3s.yaml", get_deployment_state_vars() | get_experiment_state_vars() | factors())

def run_playbook(playbook_name):
    extra_vars = get_deployment_state_vars() | factors()
    _playbook(playbook_name, extra_vars)


def exp(exp_name, experiment_description):
    from .g import g

    g.init_exp(exp_name, experiment_description)
    extra_vars = get_deployment_state_vars() | get_experiment_state_vars() | factors()
    _playbook("generate_experiment.yaml", extra_vars)
    _playbook("_current_exp.yaml", extra_vars)
    g.end_exp()
