# from typing import Optional, List
from typing import Dict
from cryptography.fernet import Fernet
from dataclasses import dataclass, asdict


@dataclass
class JSONSerializable:
    def to_dict(self):
        return asdict(self)


@dataclass
class ConnectedNode(JSONSerializable):
    address: str
    symmetric_key: str
    symmetric_key_uuid: str
    fernet: Fernet


@dataclass
class NodeData(JSONSerializable):
    """
    Dynamic NodeData class that stores all telemetry stats as JSON.
    No hardcoded field names - everything is driven by platform config.
    """

    hotkey: str
    worker_id: str
    uid: int
    boot_time: int
    last_operation_time: int
    current_time: int
    timestamp: int

    # Dynamic stats storage - contains all telemetry data as received
    stats_json: Dict = None

    # Platform metrics extracted from stats for scoring
    platform_metrics: Dict[str, Dict[str, int]] = None

    def __post_init__(self):
        """Initialize default values if not provided."""
        if self.stats_json is None:
            self.stats_json = {}
        if self.platform_metrics is None:
            self.platform_metrics = {}

    def get_stat_value(self, stat_name: str, default: int = 0) -> int:
        """Get a stat value by name from the dynamic stats_json."""
        return self.stats_json.get(stat_name, default)

    def set_stat_value(self, stat_name: str, value: int):
        """Set a stat value by name in the dynamic stats_json."""
        if self.stats_json is None:
            self.stats_json = {}
        self.stats_json[stat_name] = value

    def get_platform_metric(self, platform: str, metric: str, default: int = 0) -> int:
        """Get a platform-specific metric value."""
        if not self.platform_metrics:
            return default
        return self.platform_metrics.get(platform, {}).get(metric, default)

    def set_platform_metric(self, platform: str, metric: str, value: int):
        """Set a platform-specific metric value."""
        if self.platform_metrics is None:
            self.platform_metrics = {}
        if platform not in self.platform_metrics:
            self.platform_metrics[platform] = {}
        self.platform_metrics[platform][metric] = value

    def populate_legacy_fields(self):
        """
        Populate legacy individual fields for backward compatibility.
        Since we now use properties that read from stats_json,
        we only need to ensure platform_metrics is set.
        """
        if not self.stats_json:
            return

        # Extract platform metrics using PlatformManager for proper format
        try:
            from validator.platform_config import PlatformManager

            manager = PlatformManager()

            # Extract platform metrics from raw stats using field mappings
            self.platform_metrics = manager.extract_platform_metrics_from_stats(
                self.stats_json
            )

        except Exception as e:
            # Fallback: if platform manager fails, try to use existing platform_metrics
            if "platform_metrics" in self.stats_json and isinstance(
                self.stats_json["platform_metrics"], dict
            ):
                self.platform_metrics = self.stats_json["platform_metrics"]
            else:
                self.platform_metrics = {}

    @staticmethod
    def validate_stats_integrity(stats_json: Dict) -> bool:
        """
        Validate the integrity of stats JSON structure.
        Now validation is structure-agnostic - only checks basic types.
        """
        if not isinstance(stats_json, dict):
            return False

        # Validate that all stat values are non-negative numbers
        for key, value in stats_json.items():
            if key == "platform_metrics":
                # Validate platform_metrics structure
                if not isinstance(value, dict):
                    return False
                for platform, metrics in value.items():
                    if not isinstance(metrics, dict):
                        return False
                    for metric_name, metric_value in metrics.items():
                        if (
                            not isinstance(metric_value, (int, float))
                            or metric_value < 0
                        ):
                            return False
            elif key == "worker_version":
                # Allow string or number for worker_version; do not enforce format here
                if not isinstance(value, (str, int, float)):
                    return False
            else:
                # All other stats should be non-negative numbers
                if not isinstance(value, (int, float)) or value < 0:
                    return False

        return True

    # Legacy property accessors for backward compatibility during migration
    @property
    def twitter_auth_errors(self) -> int:
        return self.get_stat_value("twitter_auth_errors", 0)

    @property
    def twitter_errors(self) -> int:
        return self.get_stat_value("twitter_errors", 0)

    @property
    def twitter_ratelimit_errors(self) -> int:
        return self.get_stat_value("twitter_ratelimit_errors", 0)

    @property
    def twitter_returned_other(self) -> int:
        return self.get_stat_value("twitter_returned_other", 0)

    @property
    def twitter_returned_profiles(self) -> int:
        return self.get_stat_value("twitter_returned_profiles", 0)

    @property
    def twitter_returned_tweets(self) -> int:
        return self.get_stat_value("twitter_returned_tweets", 0)

    @property
    def twitter_scrapes(self) -> int:
        return self.get_stat_value("twitter_scrapes", 0)

    @property
    def web_errors(self) -> int:
        return self.get_stat_value("web_errors", 0)

    @property
    def web_success(self) -> int:
        return self.get_stat_value("web_success", 0)

    @property
    def tiktok_transcription_success(self) -> int:
        return self.get_stat_value("tiktok_transcription_success", 0)

    @property
    def tiktok_transcription_errors(self) -> int:
        return self.get_stat_value("tiktok_transcription_errors", 0)
