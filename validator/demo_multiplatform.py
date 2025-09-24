#!/usr/bin/env python3
"""
Demo script for multi-platform scoring system.
This demonstrates the new platform configuration and scoring functionality.
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from validator.platform_config import PlatformManager, PlatformConfig
from validator.weights import WeightsManager
from interfaces.types import NodeData
from unittest.mock import Mock
import time


def demo_platform_manager():
    """Demonstrate PlatformManager functionality."""
    print("=" * 60)
    print("DEMO: Platform Manager")
    print("=" * 60)

    manager = PlatformManager()

    print(f"Initialized platforms: {manager.get_platform_names()}")
    print(f"Total emission weight: {manager.get_total_emission_weight()}")

    for platform_name in manager.get_platform_names():
        platform = manager.get_platform(platform_name)
        print(f"\nPlatform: {platform.name}")
        print(f"  Emission weight: {platform.emission_weight * 100:.1f}%")
        print(f"  Success metrics: {platform.success_metrics}")
        print(f"  Error metrics: {platform.error_metrics}")
        print(f"  All metrics: {platform.metrics}")

    return manager


def create_sample_nodes():
    """Create sample NodeData objects for testing."""
    print("\n" + "=" * 60)
    print("DEMO: Sample Node Data")
    print("=" * 60)

    # Node 1: Twitter-only miner with good performance
    node1 = NodeData(
        hotkey="twitter_miner_1",
        worker_id="worker_1",
        uid=1,
        boot_time=int(time.time()) - 7200,  # 2 hours ago
        last_operation_time=int(time.time()) - 600,  # 10 minutes ago
        current_time=int(time.time()),
        twitter_auth_errors=0,
        twitter_errors=2,
        twitter_ratelimit_errors=1,
        twitter_returned_other=0,
        twitter_returned_profiles=150,
        twitter_returned_tweets=500,
        twitter_scrapes=200,
        web_errors=0,
        web_success=100,
        timestamp=int(time.time()),
        platform_metrics={
            "twitter": {
                "returned_tweets": 500,
                "returned_profiles": 150,
                "scrapes": 200,
                "auth_errors": 0,
                "errors": 2,
                "ratelimit_errors": 1,
            }
        },
    )
    node1.time_span_seconds = 7200  # 2 hours

    # Node 2: TikTok-only miner with excellent performance
    node2 = NodeData(
        hotkey="tiktok_miner_1",
        worker_id="worker_2",
        uid=2,
        boot_time=int(time.time()) - 3600,  # 1 hour ago
        last_operation_time=int(time.time()) - 300,  # 5 minutes ago
        current_time=int(time.time()),
        twitter_auth_errors=0,
        twitter_errors=0,
        twitter_ratelimit_errors=0,
        twitter_returned_other=0,
        twitter_returned_profiles=0,
        twitter_returned_tweets=0,
        twitter_scrapes=0,
        web_errors=0,
        web_success=50,
        timestamp=int(time.time()),
        platform_metrics={
            "tiktok": {"transcription_success": 75, "transcription_errors": 0}
        },
    )
    node2.time_span_seconds = 3600  # 1 hour

    # Node 3: Multi-platform miner with mixed performance
    node3 = NodeData(
        hotkey="multi_platform_miner",
        worker_id="worker_3",
        uid=3,
        boot_time=int(time.time()) - 5400,  # 1.5 hours ago
        last_operation_time=int(time.time()) - 120,  # 2 minutes ago
        current_time=int(time.time()),
        twitter_auth_errors=1,
        twitter_errors=3,
        twitter_ratelimit_errors=0,
        twitter_returned_other=0,
        twitter_returned_profiles=75,
        twitter_returned_tweets=250,
        twitter_scrapes=100,
        web_errors=1,
        web_success=60,
        timestamp=int(time.time()),
        platform_metrics={
            "twitter": {
                "returned_tweets": 250,
                "returned_profiles": 75,
                "scrapes": 100,
                "auth_errors": 1,
                "errors": 3,
                "ratelimit_errors": 0,
            },
            "tiktok": {"transcription_success": 30, "transcription_errors": 1},
        },
    )
    node3.time_span_seconds = 5400  # 1.5 hours

    # Node 4: Poor performance miner (high error rate)
    node4 = NodeData(
        hotkey="poor_performance_miner",
        worker_id="worker_4",
        uid=4,
        boot_time=int(time.time()) - 3600,  # 1 hour ago
        last_operation_time=int(time.time()) - 1800,  # 30 minutes ago
        current_time=int(time.time()),
        twitter_auth_errors=15,
        twitter_errors=20,
        twitter_ratelimit_errors=10,
        twitter_returned_other=0,
        twitter_returned_profiles=5,
        twitter_returned_tweets=10,
        twitter_scrapes=50,
        web_errors=5,
        web_success=10,
        timestamp=int(time.time()),
        platform_metrics={
            "twitter": {
                "returned_tweets": 10,
                "returned_profiles": 5,
                "scrapes": 50,
                "auth_errors": 15,
                "errors": 20,
                "ratelimit_errors": 10,
            }
        },
    )
    node4.time_span_seconds = 3600  # 1 hour

    nodes = [node1, node2, node3, node4]

    print(f"Created {len(nodes)} sample nodes:")
    for node in nodes:
        print(
            f"  {node.hotkey}: {list(node.platform_metrics.keys()) if node.platform_metrics else 'No platform metrics'}"
        )

    return nodes


def demo_platform_scoring(manager, nodes):
    """Demonstrate platform-specific scoring."""
    print("\n" + "=" * 60)
    print("DEMO: Platform-Specific Scoring")
    print("=" * 60)

    # Create a mock validator and weights manager
    mock_validator = Mock()
    weights_manager = WeightsManager(mock_validator)

    print(
        f"Scoring {len(nodes)} nodes across {len(manager.get_platform_names())} platforms\n"
    )

    for node in nodes:
        print(f"Node: {node.hotkey}")
        total_weighted_score = 0.0

        for platform_name in manager.get_platform_names():
            platform_config = manager.get_platform(platform_name)
            platform_score = weights_manager.calculate_platform_score(
                node, platform_name
            )
            weighted_score = platform_score * platform_config.emission_weight
            total_weighted_score += weighted_score

            # Show error rate calculation
            if hasattr(node, "platform_metrics") and node.platform_metrics:
                platform_metrics = node.platform_metrics.get(platform_name, {})
                if platform_metrics:
                    error_count = sum(
                        platform_metrics.get(m, 0)
                        for m in platform_config.error_metrics
                    )
                    success_count = sum(
                        platform_metrics.get(m, 0)
                        for m in platform_config.success_metrics
                    )
                    time_span_hours = getattr(node, "time_span_seconds", 0) / 3600
                    error_rate = (
                        error_count / time_span_hours if time_span_hours > 0 else 0
                    )

                    print(f"  Platform {platform_name}:")
                    print(f"    Success metrics: {success_count}")
                    print(f"    Error count: {error_count}")
                    print(f"    Error rate: {error_rate:.2f}/hour")
                    print(f"    Platform score: {platform_score:.4f}")
                    print(
                        f"    Emission weight: {platform_config.emission_weight * 100:.1f}%"
                    )
                    print(f"    Weighted score: {weighted_score:.4f}")
                else:
                    print(
                        f"  Platform {platform_name}: No metrics (score: {platform_score:.4f})"
                    )

        print(f"  Total weighted score: {total_weighted_score:.4f}")
        print()


def demo_emission_distribution(manager, nodes):
    """Demonstrate how emissions are distributed across platforms."""
    print("\n" + "=" * 60)
    print("DEMO: Emission Distribution")
    print("=" * 60)

    mock_validator = Mock()
    weights_manager = WeightsManager(mock_validator)

    # Calculate platform scores for all nodes
    platform_totals = {}
    node_scores = {}

    for platform_name in manager.get_platform_names():
        platform_totals[platform_name] = 0.0
        platform_config = manager.get_platform(platform_name)

        for node in nodes:
            platform_score = weights_manager.calculate_platform_score(
                node, platform_name
            )
            platform_totals[platform_name] += platform_score

            if node.hotkey not in node_scores:
                node_scores[node.hotkey] = {}
            node_scores[node.hotkey][platform_name] = platform_score

    # Show distribution
    print("Platform contribution to total emissions:")
    total_score = sum(platform_totals.values())

    for platform_name in manager.get_platform_names():
        platform_config = manager.get_platform(platform_name)
        platform_total = platform_totals[platform_name]
        weighted_contribution = platform_total * platform_config.emission_weight
        percentage = (
            (weighted_contribution / total_score * 100) if total_score > 0 else 0
        )

        print(f"  {platform_name}:")
        print(f"    Unweighted total: {platform_total:.4f}")
        print(f"    Emission weight: {platform_config.emission_weight * 100:.1f}%")
        print(f"    Weighted contribution: {weighted_contribution:.4f}")
        print(f"    Percentage of total: {percentage:.1f}%")

    print(f"\nTotal weighted score: {total_score:.4f}")


def main():
    """Run the multi-platform scoring demo."""
    print("Multi-Platform Scoring System Demo")
    print("This demonstrates the implementation for Subnet 42")

    # Initialize platform manager
    manager = demo_platform_manager()

    # Create sample nodes
    nodes = create_sample_nodes()

    # Demonstrate platform scoring
    demo_platform_scoring(manager, nodes)

    # Demonstrate emission distribution
    demo_emission_distribution(manager, nodes)

    print("\n" + "=" * 60)
    print("DEMO COMPLETED")
    print("=" * 60)
    print("\nKey Features Demonstrated:")
    print("✓ Platform configuration with 90/10 Twitter/TikTok split")
    print("✓ Individual platform scoring")
    print("✓ Multi-platform emission weight distribution")
    print("✓ Backward compatibility with existing metrics")
    print("\nImplementation Status:")
    print("✓ PlatformConfig dataclass")
    print("✓ PlatformManager class")
    print("✓ NodeData platform_metrics field")
    print("✓ Multi-platform scoring in WeightsManager")
    print("✓ Error rates tracked for observability (no score punishment)")


if __name__ == "__main__":
    main()
