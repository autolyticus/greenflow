from pdb import post_mortem
import time
from box import Box
from greenflow.exp_ng.hammer import hammer, stress_test
from greenflow.exp_ng.exp_ng import killexp
from entrypoint import (
    rebind_parameters,
    load_gin,
    send_notification,
    kafka_context,
    kafka_kraft_context,
    redpanda_context,
)
import traceback
import logging
from time import sleep

from greenflow.exp_ng.prometheus import reinit_prometheus, scale_prometheus
from ..state import get_deployment_state_vars, get_experiment_state_vars
from ..factors import factors


def idle(exp_description) -> None:
    for exp_name in ["ingest-kafka", "ingest-redpanda"]:
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)

        from ..g import g

        with ctx_manager():
            rebind_parameters(durationSeconds=300)
            stress_test(
                target_load=0,
                exp_description=exp_description,
            )

    send_notification("Idle test complete.")


def baseline(exp_description) -> None:
    exp_names = {
        "ingest-kafka": kafka_context,
        "ingest-kafka-kraft": kafka_kraft_context,
        # "ingest-redpanda": redpanda_context,
    }
    rep = 3
    for exp_name, ctx_manager in exp_names.items():
        load_gin(exp_name)
        rebind_parameters(messageSize=4096)

        from ..g import g

        # rebind_parameters(brokerReplicas=1)
        # with ctx_manager():
        #     for _ in range(rep):
        #         rebind_parameters(
        #             partitions=20,
        #             consumerInstances=0,
        #             producerInstances=8,
        #             replicationFactor=1,
        #         )
        #         stress_test(
        #             target_load=10**9,
        #             exp_description=exp_description,
        #         )

        rebind_parameters(brokerReplicas=3)
        with ctx_manager():
            for _ in range(rep):
                # rebind_parameters(
                #     partitions=1,
                #     consumerInstances=0,
                #     producerInstances=8,
                #     replicationFactor=1,
                # )
                # stress_test(
                #     target_load=10**9,
                #     exp_description=exp_description,
                # )
                rebind_parameters(
                    partitions=1,
                    consumerInstances=0,
                    producerInstances=8,
                    replicationFactor=3,
                )
                stress_test(
                    target_load=10**9,
                    exp_description=exp_description,
                )
                rebind_parameters(
                    partitions=60,
                    consumerInstances=0,
                    producerInstances=8,
                    replicationFactor=3,
                )
                stress_test(
                    target_load=10**9,
                    exp_description=exp_description,
                )
                # rebind_parameters(partitions=1, consumerInstances=0, producerInstances=10, replicationFactor=2)
                # stress_test(
                #     target_load=10**9,
                #     exp_description=exp_description,
                # )
                # rebind_parameters(replicationFactor=3)
                # rebind_parameters(partitions=1, consumerInstances=0, producerInstances=24)
                # stress_test(
                #     target_load=10**9,
                #     exp_description=exp_description,
                # )
                # rebind_parameters(
                #     partitions=1200, consumerInstances=0, producerInstances=10
                # )
                # stress_test(
                #     target_load=10**9,
                #     exp_description=exp_description,
                # )
                # # rebind_parameters(partitions=30, consumerInstances=10, producerInstances=10)
                # # stress_test(
                # #     target_load=10**9,
                # #     exp_description=exp_description,
                # # )

    send_notification("Idle test complete.")


def smoketest(exp_description) -> None:
    for exp_name in ["ingest-kafka", "ingest-redpanda"]:
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)

        from ..g import g

        replica = 3
        multiplier = 1

        rebind_parameters(
            brokerReplicas=replica,
            partitions=replica * multiplier,
            consumerInstances=10,
            producerInstances=10,
        )
        with ctx_manager():
            rebind_parameters(durationSeconds=30)
            max_throughput = stress_test(
                target_load=1 * 10**9,  # High initial load to find limits
                exp_description=exp_description,
            )
            print("Observed max throughput: ", max_throughput)
    send_notification("Smoketest complete.")

def latency(exp_description) -> None:
    exp_names = [
        "ingest-kafka",
        "ingest-redpanda",
    ]
    # Run both systems, find the min max throughput for each
    max_throughputs = []
    for exp_name in exp_names:
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)

        from ..g import g

        rebind_parameters(
            brokerReplicas=3,
            partitions=1,
            producerInstances=10,
            messageSize=1024,
        )
        with ctx_manager():
            max_throughput = stress_test(
                target_load=1 * 10**9,  # High initial load to find limits
                exp_description=exp_description,
            )
            print("Observed max throughput: ", max_throughput)
            max_throughputs.append(max_throughput)

    min_max_throughput = min(max_throughputs)
    target_latency_load = min_max_throughput * 0.3
    # target_latency_load = 74820
    for exp_name in exp_names:
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)

        from ..g import g
        repeats = 3

        rebind_parameters(
            partitions=1,
            durationSeconds=300,
            producerInstances=10,
            # messageSize=128,
            messageSize=1024,
        )
        with ctx_manager():
            rebind_parameters(topic_name="kminion-end-to-end")
            for i in range(repeats):
                max_throughput = stress_test(
                    target_load=target_latency_load,
                    exp_description=exp_description,
                )
    send_notification("Latency experiments complete.")


def system(exp_description) -> None:
    rep = 3
    for exp_name in ["ingest-kafka", "ingest-redpanda"]:
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)

        rebind_parameters(
            partitions=60,
            producerInstances=8,
            consumerInstances=0,
            messageSize=4096,
        )
        for _ in range(rep):
            with ctx_manager():
                max_throughput = stress_test(
                    target_load=1 * 10**9,
                    exp_description=exp_description,
                )
                print("Observed max throughput: ", max_throughput)

    send_notification("Smoketest complete.")


def demonstrate_binary_search(exp_description) -> None:
    from greenflow.playbook import exp
    from greenflow.adaptive import threshold_hammer, threshold

    exp_name = "ingest-kafka"
    load_gin("ingest-kafka")
    rebind_parameters(consumerInstances=0, producerInstances=8, partitions=60)
    messageSizes = [
        10,
        50,
        100,
        500,
        1000,
        2000,
        3000,
        4000,
        5000,
        6000,
        7000,
        8000,
        9000,
        10000,
    ]

    # Message sizes up to 1MB (with
    with kafka_context():
        threshold("ingest-kafka", exp_description, messageSizes)

    send_notification("Experiment complete. On to the next.")


def run_single_hammer(exp_name, *, exp_description, **params):
    ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
    params = Box(params)

    try:
        rebind_parameters(**params)
        with ctx_manager():
            return hammer(exp_description)
    except Exception as e:
        traceback.print_exc()
        send_notification(
            f"Error in {exp_name} experiment with: {str(e)}",
            priority="max",
        )
        traceback.print_exc()
        post_mortem()
        try:
            killexp()  # Clean up
        except Exception as cleanup_error:
            logging.error(f"Error during cleanup: {str(cleanup_error)}")


def safety_curve(exp_description) -> None:
    rep = 3
    messageSizes = [2**i for i in range(5, 21)]

    for exp_name in ["ingest-kafka", "ingest-redpanda"]:
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)

        rebind_parameters(
            partitions=60,
            producerInstances=8,
            consumerInstances=0,
        )

        # Message sizes up to 1MB (with
        with ctx_manager():
            for _ in range(rep):
                for messageSize in messageSizes:
                    rebind_parameters(messageSize=messageSize)
                    stress_test(
                        target_load=1 * 10**9,
                        exp_description=exp_description,
                    )

    send_notification("Experiment complete. On to the next.")


def partitioning(exp_description) -> None:
    partitions = [
        1,
        3,
        9,
        18,
        30,
        60,
        120,
        300,
        600,
        900,
        1500,
        3000,
    ]
    workers = get_workers()

    for exp_name in ["ingest-kafka", "ingest-redpanda"]:
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)

        rebind_parameters(
            consumerInstances=0,
            producerInstances=workers * 8,
            messageSize=4096,
        )
        with ctx_manager():
            for partition in partitions:
                rebind_parameters(partitions=partition)
                for _ in range(3):
                    stress_test(
                        target_load=1 * 10**9,
                        exp_description=exp_description,
                    )

    send_notification("Experiment complete. On to the next.")


def scaling_behaviour(exp_description) -> None:
    rep = 3
    mult = 20
    brokerReplicaList = list(range(3, 11))

    workers = get_workers()

    exp_name = "ingest-kafka"
    load_gin(exp_name)
    rebind_parameters(consumerInstances=0)

    for replicas in brokerReplicaList:
        rebind_parameters(
            partitions=replicas * mult,
            brokerReplicas=replicas,
            producerInstances=8 * workers,
        )
        with kafka_context():
            for messageSize in [4096]:
                rebind_parameters(messageSize=messageSize)
                for _ in range(rep):
                    stress_test(
                        target_load=1 * 10**9,
                        exp_description=exp_description,
                    )

    exp_name = "ingest-redpanda"
    load_gin(exp_name)
    rebind_parameters(consumerInstances=0)
    brokerReplicaList = list(range(3, 11))

    for replicas in brokerReplicaList:
        rebind_parameters(
            partitions=replicas * mult,
            brokerReplicas=replicas,
            producerInstances=8 * workers,
        )
        with redpanda_context():
            for messageSize in [4096]:
                rebind_parameters(messageSize=messageSize)
                for _ in range(rep):
                    stress_test(
                        target_load=1 * 10**9,
                        exp_description=exp_description,
                    )

    send_notification("Experiment complete. On to the next.")


def get_workers():
    from ..g import g

    workers = len(
        Box(
            g.root.current_deployment.metadata
        ).ansible_inventory.all.children.worker.hosts
    )

    return workers


def proportionality(exp_description) -> None:
    # Common parameters
    test_duration = 100  # seconds for non-idle tests
    broker_replicas = [3]
    rep = 3
    mult = 20
    baseline = {
        "ovhnvme": {
            "ingest-kafka": 156306.43,
            "ingest-redpanda": 237742.98,
        },
        "grappe": {
            "ingest-kafka": 120700,
            "ingest-redpanda": 110794,
        },
        "taurus": {
            "ingest-kafka": 18559.73,
            "ingest-redpanda": 6747.87,
        },
    }

    for exp_name, baseline in baseline["ovhnvme"].items():
        ctx_manager = kafka_context if exp_name == "ingest-kafka" else redpanda_context
        load_gin(exp_name)
        rebind_parameters(
            messageSize=4096,
        )

        from ..g import g

        for replica in broker_replicas:
            for _ in range(rep):
                rebind_parameters(
                    partitions=replica * mult,
                )
                # rebind_parameters(
                #     brokerReplicas=replica,
                #     partitions=replica * mult,
                #     messageSize=4096,
                #     consumerInstances=0,
                #     producerInstances=16,
                # )
                with ctx_manager():
                    # sleep(30)

                    # rebind_parameters(durationSeconds=300)
                    # stress_test(
                    #     target_load=0,  # Idle load to find idle power
                    #     exp_description=exp_description,
                    # )

                    # Proportionality tests at 10% intervals
                    for percentage in range(1, 11, 1):
                        load_factor = percentage / 100.0
                        stress_test(
                            target_load=baseline * load_factor,
                            exp_description=exp_description,
                        )
                        time.sleep(60)

    send_notification("Proportionality experiments complete.")
