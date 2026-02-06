import logging
import threading
import traceback
from kr8s.objects import Job
import time
import pendulum
from sh import kubectl, helm
from shlex import split

from box import Box, BoxList

from .synchronized_perf_script import producer_script

from .prometheus import reinit_prometheus, scale_prometheus

from ..factors import factors
from ..playbook import _playbook
from ..state import get_deployment_state_vars, get_experiment_state_vars


def create_job(extra_vars) -> Job:
    pushgateway_url = extra_vars["prometheus_pushgateway_url"]
    exp_params = extra_vars["exp_params"]

    # First, check if the topic exists
    check_topic_args = BoxList(
        [
            "--bootstrap-server",
            exp_params.kafka_bootstrap_servers,
            "--topic",
            "input",
            "--describe",
        ]
    )

    create_topic_args = BoxList(
        [
            "--bootstrap-server",
            exp_params.kafka_bootstrap_servers,
            "--topic",
            "input",
            "--partitions",
            f"{exp_params.partitions}",
            "--replication-factor",
            f"{exp_params.replicationFactor}",
            "--create",
        ]
    )

    return Job(
        dict(
            apiVersion="batch/v1",
            kind="Job",
            metadata={"name": "create-kafka-topic", "namespace": "default"},
            spec={
                "backoffLimit": 0,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "create-topic",
                                "image": "quay.io/strimzi/kafka:0.44.0-kafka-3.8.0",
                                "command": ["/bin/sh"],
                                "args": [
                                    "-c",
                                    f"""
                                    NUM_ATTEMPTS=40
                                    SLEEP_SECONDS=3
                                    
                                    for i in $(seq 1 $NUM_ATTEMPTS); do
                                        echo "Executing: /opt/kafka/bin/kafka-topics.sh {' '.join(check_topic_args)}"
                                        if /opt/kafka/bin/kafka-topics.sh {' '.join(check_topic_args)} 2>/dev/null; then
                                            echo "Topic 'input' already exists."
                                            exit 0
                                        fi
                                        
                                        echo "Executing: /opt/kafka/bin/kafka-topics.sh {' '.join(create_topic_args)}"
                                        output=$(/opt/kafka/bin/kafka-topics.sh {' '.join(create_topic_args)} 2>&1)
                                        status=$?
                                        if [ $status -eq 0 ]; then
                                            echo "Topic created successfully"
                                            exit 0
                                        elif echo "$output" | grep -q "marked for deletion"; then
                                            echo "Topic is marked for deletion, attempt $i of $NUM_ATTEMPTS. Waiting $SLEEP_SECONDS seconds..."
                                            sleep $SLEEP_SECONDS
                                            continue
                                        else
                                            echo "Failed to create topic: $output"
                                            exit 1
                                        fi
                                    done
                                    echo "Failed after $NUM_ATTEMPTS attempts - topic still marked for deletion"
                                    exit 1

                                    """,
                                ],
                            }
                        ],
                    }
                },
            },
        )
    )


def synchronized_exp_job(extra_vars) -> Job:
    exp_params = extra_vars["exp_params"]
    total_messages = int(
        exp_params["load"]
        * exp_params["durationSeconds"]
        / exp_params["producer_instances"]
    )
    start_timestamp = int(time.time()) + 15

    return Job(
        dict(
            apiVersion="batch/v1",
            kind="Job",
            metadata={"name": "kafka-producer-perf-test", "namespace": "default"},
            spec={
                "parallelism": exp_params["producer_instances"],
                "completions": exp_params["producer_instances"],
                "backoffLimit": 0,
                # "ttlSecondsAfterFinished": 100,
                "template": {
                    "metadata": {"labels": {"app": "kafka-producer-perf-test"}},
                    "spec": {
                        "restartPolicy": "Never",
                        "terminationGracePeriodSeconds": 0,
                        "nodeSelector": {"node.kubernetes.io/worker": "true"},
                        "containers": [
                            {
                                "name": "kafka-producer-perf-test",
                                "image": "registry.gitlab.inria.fr/gkovilkk/greenflow/cp-kafka:7.7.0",
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "/bin/sh",
                                    "-c",
                                    f"""
cat << 'EOF' > /tmp/synchronized_kafka_perf_test.sh
{producer_script}
EOF
chmod +x /tmp/synchronized_kafka_perf_test.sh
/tmp/synchronized_kafka_perf_test.sh \
    --topic input \
    --num-records {int(total_messages)} \
    --record-size {exp_params['messageSize']} \
    --throughput {int(exp_params['load'] // exp_params['producer_instances'])} \
    --producer-props bootstrap.servers={exp_params['kafka_bootstrap_servers']} \
    --durationSeconds {exp_params['durationSeconds']} \
    --start-timestamp {start_timestamp} \
                                    """,
                                ],
                            }
                        ],
                    },
                },
            },
        )
    )


def delete_job(extra_vars) -> Job:
    exp_params = extra_vars["exp_params"]
    return Job(
        dict(
            apiVersion="batch/v1",
            kind="Job",
            metadata={"name": "delete-kafka-topic", "namespace": "default"},
            spec={
                # "ttlSecondsAfterFinished": 5,
                "backoffLimit": 0,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "containers": [
                            {
                                "name": "delete-topic",
                                "image": "registry.gitlab.inria.fr/gkovilkk/greenflow/redpanda:v24.1.8",
                                "command": ["rpk"],
                                "args": [
                                    "topic",
                                    "delete",
                                    "input",
                                    "-X",
                                    f"brokers={exp_params.kafka_bootstrap_servers}",
                                ],
                            }
                        ],
                    }
                },
            },
        )
    )


def create_kafka_topic(extra_vars):
    job = create_job(extra_vars)
    job.create()
    job.wait(["condition=Complete", "condition=Failed"])
    if job.status.conditions[0].type == "Complete":
        job.delete(propagation_policy="Foreground")
        return
    else:
        raise RuntimeError("Failed to create kafka topic")


def deploy_experiment(extra_vars) -> Job:
    job = synchronized_exp_job(extra_vars)
    # job = exp_job_custom(extra_vars)
    job.create()

    # Assume that it can take up to 20 seconds to start the job
    gracePeriod = 20
    totalDuration = extra_vars["exp_params"]["durationSeconds"] + gracePeriod

    try:
        job.wait(["condition=Complete", "condition=Failed"], timeout=totalDuration * 10)
        if job.status.conditions[0].type == "Complete":
            job.delete(propagation_policy="Foreground")
            return
        else:
            breakpoint()
            raise RuntimeError("Failed to run experiment")
    except TimeoutError:
        breakpoint()
        # job.delete(propagation_policy="Foreground")
        return


def delete_kafka_topic(extra_vars):
    job = delete_job(extra_vars)
    job.create()
    job.wait(["condition=Complete", "condition=Failed"])
    if job.status.conditions[0].type == "Complete":
        job.delete(propagation_policy="Foreground")
        return
    else:
        raise RuntimeError("Failed to delete kafka topic")


def exp(experiment_description) -> float:
    exp_name = factors()["exp_name"]
    from ..g import g

    g.init_exp(experiment_description)
    extra_vars = get_deployment_state_vars() | get_experiment_state_vars() | factors()
    extra_vars = Box(extra_vars)
    logging.warning(
        {
            "load": extra_vars.exp_params.load,
            "messageSize": extra_vars.exp_params.messageSize,
        }
    )

    reinit_prometheus(
        extra_vars["deployment_started_ts"], extra_vars["experiment_started_ts"]
    )
    create_kafka_topic(extra_vars)
    deploy_experiment(extra_vars)

    # Let the metrics get scraped before deleting the kafka topic
    time.sleep(10)
    delete_kafka_topic(extra_vars)
    scale_prometheus(0)

    g.end_exp()


def killexp():
    from ..g import g

    extra_vars = get_deployment_state_vars() | get_experiment_state_vars() | factors()
    extra_vars = Box(extra_vars)
    g.end_exp()
    try:
        delete_kafka_topic(extra_vars)
    except:
        ...
        traceback.print_exc()
    for job_name in [
        "create-kafka-topic",
        "throughput",
        "delete-kafka-topic",
        "kafka-producer-perf-test",
        "kafka-consumer-perf-test",
    ]:
        try:
            job = Job(Box(metadata=Box(name=job_name, namespace="default")))
            job.delete(propagation_policy="Foreground")
            kubectl(split(f"delete job {job_name} -n default"))
        except:
            ...
            # traceback.print_exc()
            # breakpoint()
        try:
            pass
            # TODO: Re-enable when stable
            # kubectl(split("delete kafka theodolite-kafka"), _ok_code=[0, 1])
            # helm(split("uninstall -n default kminion"), _ok_code=[0, 1])
            # helm(split("uninstall -n redpanda redpanda"), _ok_code=[0, 1])
            # helm(split("uninstall -n redpanda kminion"), _ok_code=[0, 1])
            # kubectl(split("delete pvc -n redpanda --all"), _ok_code=[0, 1])
            # Restart the Strimzi operator deployment
            # kubectl(
            #     split("rollout restart deployment strimzi-cluster-operator -n default"),
            #     _ok_code=[0, 1],
            # )
        except:
            traceback.print_exc()
            ...
    scale_prometheus(0)
