#!/usr/bin/env python3
"""Simple CycloneDDS subscriber with throughput measurement."""

import argparse
import os
import time

# Use minimal CycloneDDS config to avoid buffer size issues
os.environ["CYCLONEDDS_URI"] = ""

from cyclonedds.core import Qos, Policy
from cyclonedds.domain import DomainParticipant
from cyclonedds.sub import DataReader, Subscriber
from cyclonedds.topic import Topic
from cyclonedds.idl import IdlStruct
from cyclonedds.idl.types import sequence, uint8
from dataclasses import dataclass


@dataclass
class BenchmarkData(IdlStruct):
    """DDS message type for benchmarking with variable-size byte payload."""
    data: sequence[uint8]


def main():
    parser = argparse.ArgumentParser(description="DDS Subscriber Benchmark")
    parser.add_argument("--size", type=int, default=1024, help="Expected message size in bytes")
    parser.add_argument("--count", type=int, default=1000, help="Expected number of messages")
    parser.add_argument("--topic", type=str, default="benchmark/dds", help="Topic name")
    parser.add_argument("--domain", type=int, default=0, help="DDS domain ID")
    parser.add_argument("--qos", type=str, default="reliable", choices=["reliable", "high-throughput", "best-effort"],
                        help="QoS preset: reliable (guaranteed delivery), high-throughput (optimized for speed), best-effort (minimal overhead)")
    parser.add_argument("--timeout", type=float, default=30.0, help="Timeout in seconds")
    args = parser.parse_args()

    # Create QoS based on preset
    if args.qos == "reliable":
        qos = Qos(
            Policy.Reliability.Reliable(max_blocking_time=1_000_000_000),  # 1 second
            Policy.History.KeepLast(10000),  # Large buffer for no loss
            Policy.Durability.Volatile,
        )
        print("Using RELIABLE QoS (guaranteed delivery, large history)")
    elif args.qos == "high-throughput":
        qos = Qos(
            Policy.Reliability.BestEffort,
            Policy.History.KeepLast(1000),  # Larger buffer to reduce loss
            Policy.Durability.Volatile,
        )
        print("Using HIGH-THROUGHPUT QoS (best-effort, larger buffer)")
    else:  # best-effort
        qos = Qos(
            Policy.Reliability.BestEffort,
            Policy.History.KeepLast(100),
            Policy.Durability.Volatile,
        )
        print("Using BEST-EFFORT QoS (no guarantees, moderate buffering)")

    # Create DDS entities
    participant = DomainParticipant(domain_id=args.domain)
    topic = Topic(participant, args.topic, BenchmarkData, qos=qos)
    subscriber = Subscriber(participant)
    reader = DataReader(subscriber, topic, qos=qos)

    print(f"Topic: {args.topic}")
    print(f"Domain: {args.domain}")
    print(f"Expected message size: {args.size} bytes")
    print(f"Expected message count: {args.count}")
    print("Waiting for messages...")

    received_count = 0
    total_bytes = 0
    first_msg_time = None
    last_msg_time = None

    start_wait = time.perf_counter()

    while received_count < args.count:
        # Check timeout
        if time.perf_counter() - start_wait > args.timeout:
            print(f"Timeout reached after {args.timeout} seconds")
            break

        # Try to read messages (large batch for throughput)
        samples = reader.take(1000)

        if samples:
            now = time.perf_counter()

            for sample in samples:
                # Skip invalid samples (disposed/unregistered instances)
                if not isinstance(sample, BenchmarkData):
                    continue

                if first_msg_time is None:
                    first_msg_time = now
                last_msg_time = now

                received_count += 1
                total_bytes += len(sample.data)

                # Print progress every 100 messages
                if received_count % 100 == 0:
                    print(f"Received {received_count}/{args.count} messages...")
        else:
            # Small sleep to avoid busy waiting
            time.sleep(0.001)

    # Calculate throughput
    if first_msg_time and last_msg_time and received_count > 1:
        elapsed = last_msg_time - first_msg_time
        if elapsed > 0:
            throughput_msgs = (received_count - 1) / elapsed  # -1 because we measure between first and last
            throughput_bytes = total_bytes / elapsed
            throughput_mbps = throughput_bytes / (1024 * 1024)
        else:
            throughput_msgs = float('inf')
            throughput_mbps = float('inf')
    else:
        elapsed = 0
        throughput_msgs = 0
        throughput_mbps = 0

    print(f"\n--- Subscriber Results ---")
    print(f"Messages received: {received_count}/{args.count}")
    print(f"Total bytes received: {total_bytes}")
    print(f"Elapsed time: {elapsed:.3f} seconds")
    print(f"Throughput: {throughput_msgs:.2f} msg/s")
    print(f"Throughput: {throughput_mbps:.2f} MB/s")
    print(f"Throughput: {throughput_mbps * 8:.2f} Mbps")

    if received_count < args.count:
        loss_rate = (args.count - received_count) / args.count * 100
        print(f"Message loss: {loss_rate:.1f}%")


if __name__ == "__main__":
    main()
