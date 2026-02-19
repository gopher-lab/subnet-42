import unittest
from unittest.mock import Mock
import numpy as np

from validator.platform_config import PlatformConfig, PlatformManager
from validator.weights import WeightsManager
from interfaces.types import NodeData


class TestPlatformConfig(unittest.TestCase):
    """Test cases for PlatformConfig dataclass."""

    def test_valid_platform_config(self):
        """Test creating a valid platform configuration."""
        config = PlatformConfig(
            name="twitter",
            emission_weight=0.9,
            metrics=["scrapes", "returned_tweets"],
            error_metrics=["errors"],
            success_metrics=["returned_tweets"],
        )

        self.assertEqual(config.name, "twitter")
        self.assertEqual(config.emission_weight, 0.9)
        self.assertEqual(config.metrics, ["scrapes", "returned_tweets"])
        self.assertEqual(config.error_metrics, ["errors"])
        self.assertEqual(config.success_metrics, ["returned_tweets"])

    def test_invalid_emission_weight(self):
        """Test that invalid emission weights raise ValueError."""
        with self.assertRaises(ValueError):
            PlatformConfig(
                name="test",
                emission_weight=1.5,  # Invalid: > 1.0
                metrics=["test"],
                error_metrics=["errors"],
                success_metrics=["test"],
            )

        with self.assertRaises(ValueError):
            PlatformConfig(
                name="test",
                emission_weight=-0.1,  # Invalid: < 0.0
                metrics=["test"],
                error_metrics=["errors"],
                success_metrics=["test"],
            )

    def test_empty_metrics_lists(self):
        """Test that empty metrics lists raise ValueError."""
        with self.assertRaises(ValueError):
            PlatformConfig(
                name="test",
                emission_weight=0.5,
                metrics=[],  # Empty metrics
                error_metrics=["errors"],
                success_metrics=["test"],
            )

        with self.assertRaises(ValueError):
            PlatformConfig(
                name="test",
                emission_weight=0.5,
                metrics=["test"],
                error_metrics=["errors"],
                success_metrics=[],  # Empty success metrics
            )


class TestPlatformManager(unittest.TestCase):
    """Test cases for PlatformManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = PlatformManager()

    def test_default_platforms(self):
        """Test that default platforms are initialized correctly."""
        platforms = self.manager.get_all_platforms()

        self.assertIn("twitter", platforms)
        self.assertIn("tiktok-transcription", platforms)

        twitter_config = platforms["twitter"]
        self.assertEqual(twitter_config.emission_weight, 0.7)
        self.assertIn("tweets", twitter_config.success_metrics)

        tiktok_config = platforms["tiktok-transcription"]
        self.assertEqual(tiktok_config.emission_weight, 0.05)
        self.assertIn("transcriptions", tiktok_config.success_metrics)

    def test_emission_weights_sum_to_one(self):
        """Test that emission weights sum to 1.0."""
        total_weight = self.manager.get_total_emission_weight()
        self.assertAlmostEqual(total_weight, 1.0, places=6)

    def test_get_platform(self):
        """Test getting individual platform configurations."""
        twitter_config = self.manager.get_platform("twitter")
        self.assertEqual(twitter_config.name, "twitter")

        with self.assertRaises(ValueError):
            self.manager.get_platform("unknown_platform")

    def test_get_platform_names(self):
        """Test getting list of platform names."""
        names = self.manager.get_platform_names()
        self.assertIn("twitter", names)
        self.assertIn("tiktok-transcription", names)


class TestMultiPlatformScoring(unittest.TestCase):
    """Test cases for multi-platform scoring in WeightsManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_validator = Mock()
        self.weights_manager = WeightsManager(self.mock_validator)

    def test_platform_manager_initialization(self):
        """Test that PlatformManager is initialized correctly."""
        self.assertIsInstance(self.weights_manager.platform_manager, PlatformManager)
        self.assertEqual(
            len(self.weights_manager.platform_manager.get_platform_names()), 7
        )

    def test_calculate_platform_score_no_metrics(self):
        """Test platform score calculation with no platform metrics."""
        node = NodeData(
            hotkey="test_hotkey",
            worker_id="test_worker",
            uid=1,
            boot_time=0,
            last_operation_time=0,
            current_time=0,
            timestamp=0,
            stats_json={},
            platform_metrics={},
        )

        score = self.weights_manager.calculate_platform_score(node, "twitter")
        self.assertEqual(score, 0.0)

    def test_calculate_platform_score_with_metrics(self):
        """Test platform score calculation with platform metrics."""
        node = NodeData(
            hotkey="test_hotkey",
            worker_id="test_worker",
            uid=1,
            boot_time=0,
            last_operation_time=1000,
            current_time=2000,
            timestamp=0,
            stats_json={},
            platform_metrics={
                "twitter": {
                    "tweets": 100,
                    "errors": 2,
                    "auth_errors": 1,
                },
                "tiktok-transcription": {"transcriptions": 25, "errors": 0},
            },
        )

        twitter_score = self.weights_manager.calculate_platform_score(node, "twitter")
        tiktok_score = self.weights_manager.calculate_platform_score(
            node, "tiktok-transcription"
        )

        # Twitter should have a score based on tweets.
        self.assertGreater(twitter_score, 0.0)

        # TikTok should have a score based on transcriptions
        self.assertGreater(tiktok_score, 0.0)

    def test_calculate_platform_score_high_error_rate(self):
        """High error rates no longer zero the score (success-only)."""
        node = NodeData(
            hotkey="test_hotkey",
            worker_id="test_worker",
            uid=1,
            boot_time=0,
            last_operation_time=1000,
            current_time=2000,
            timestamp=0,
            stats_json={},
            platform_metrics={
                "twitter": {
                    "tweets": 100,
                    "errors": 50,  # High error count
                    "auth_errors": 25,
                }
            },
        )

        score = self.weights_manager.calculate_platform_score(node, "twitter")
        # Score should reflect success metrics regardless of errors
        self.assertGreater(score, 0.0)

    def test_calculate_platform_score_unknown_platform(self):
        """Test handling of unknown platform names."""
        node = NodeData(
            hotkey="test_hotkey",
            worker_id="test_worker",
            uid=1,
            boot_time=0,
            last_operation_time=0,
            current_time=0,
            timestamp=0,
            stats_json={},
            platform_metrics={"unknown": {"metric": 100}},
        )

        score = self.weights_manager.calculate_platform_score(node, "unknown_platform")
        self.assertEqual(score, 0.0)


if __name__ == "__main__":
    unittest.main()
