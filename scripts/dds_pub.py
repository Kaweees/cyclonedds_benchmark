#!/usr/bin/env python3
"""Simple CycloneDDS publisher with throughput measurement."""

import argparse
import os
import time

# Use minimal CycloneDDS config to avoid buffer size issues
os.environ["CYCLONEDDS_URI"] = ""

from cyclonedds.core import Qos, Policy
from cyclonedds.domain import DomainParticipant
from cyclonedds.pub import DataWriter, Publisher
from cyclonedds.topic import Topic
from cyclonedds.idl import IdlStruct
from cyclonedds.idl.types import sequence, uint8
from dataclasses import dataclass


@dataclass
class BenchmarkData(IdlStruct):
    """DDS message type for benchmarking with variable-size byte payload."""
    data: sequence[uint8]


def make_data_bytes(size: int) -> bytes:
    """Generate bytes of given size."""
    return bytes(i % 256 for i in range(size))


def main():
    parser = argparse.ArgumentParser(description="DDS Publisher Benchmark")
    parser.add_argument("--size", type=int, default=1024, help="Message size in bytes")
    parser.add_argument("--count", type=int, default=1000, help="Number of messages to send")
    parser.add_argument("--topic", type=str, default="benchmark/dds", help="Topic name")
    parser.add_argument("--domain", type=int, default=0, help="DDS domain ID")
    parser.add_argument("--qos", type=str, default="reliable", choices=["reliable", "high-throughput", "best-effort"],
                        help="QoS preset: reliable (guaranteed delivery), high-throughput (optimized for speed), best-effort (minimal overhead)")
    parser.add_argument("--rate", type=float, default=0, help="Target publish rate in msg/s (0 = unlimited)")
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
    publisher = Publisher(participant)
    writer = DataWriter(publisher, topic, qos=qos)

    # Generate message
    msg = BenchmarkData(data=make_data_bytes(args.size))

    print(f"Topic: {args.topic}")
    print(f"Domain: {args.domain}")
    print(f"Message size: {args.size} bytes")
    print(f"Message count: {args.count}")
    print("Waiting 2 seconds for subscriber to connect...")
    time.sleep(2)

    print("Starting publish...")
    if args.rate > 0:
        target_mbps = (args.rate * args.size) / (1024 * 1024)
        print(f"Target rate: {args.rate} msg/s ({target_mbps:.2f} MB/s)")
        interval = 1.0 / args.rate
    else:
        print("Rate: unlimited")
        interval = 0

    start_time = time.perf_counter()

    for i in range(args.count):
        writer.write(msg)
        if interval > 0:
            # Sleep to maintain target rate
            elapsed = time.perf_counter() - start_time
            expected = (i + 1) * interval
            sleep_time = expected - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    # Calculate throughput
    total_bytes = args.size * args.count
    throughput_msgs = args.count / elapsed 
    throughput_bytes = total_bytes / elapsed
    throughput_mbps = throughput_bytes / (1024 * 1024)

    print(f"\n--- Publisher Results ---")
    print(f"Elapsed time: {elapsed:.3f} seconds")
    print(f"Messages sent: {args.count}")
    print(f"Throughput: {throughput_msgs:.2f} msg/s")
    print(f"Throughput: {throughput_mbps:.2f} MB/s")
    print(f"Throughput: {throughput_mbps * 8:.2f} Mbps")

    # Keep alive briefly for reliable delivery
    if args.qos == "reliable":
        time.sleep(1)


if __name__ == "__main__":
    main()
