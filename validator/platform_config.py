from dataclasses import dataclass
from typing import Dict, List
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


@dataclass
class PlatformConfig:
    """Configuration for each supported platform."""

    name: str
    emission_weight: float
    metrics: List[str]
    error_metrics: List[str]
    success_metrics: List[str]
    # Field mapping from raw telemetry field names to platform metric names
    field_mappings: Dict[str, str] = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not 0 <= self.emission_weight <= 1:
            raise ValueError(
                f"Emission weight must be between 0 and 1, got {self.emission_weight}"
            )
        if not self.metrics:
            raise ValueError("Metrics list cannot be empty")
        if not self.success_metrics:
            raise ValueError("Success metrics list cannot be empty")
        if self.field_mappings is None:
            self.field_mappings = {}

    def get_platform_metric_name(self, raw_field_name: str) -> str:
        """
        Map raw telemetry field name to platform metric name.
        Returns the mapped name or the original name if no mapping exists.
        """
        return self.field_mappings.get(raw_field_name, raw_field_name)

    def get_raw_field_name(self, platform_metric_name: str) -> str:
        """
        Reverse lookup: get raw field name from platform metric name.
        """
        for raw_name, platform_name in self.field_mappings.items():
            if platform_name == platform_metric_name:
                return raw_name
        return platform_metric_name

    def get_all_raw_field_names(self) -> List[str]:
        """Get all raw field names that this platform cares about."""
        raw_fields = []
        for metric in self.metrics:
            raw_field = self.get_raw_field_name(metric)
            if raw_field not in raw_fields:
                raw_fields.append(raw_field)
        return raw_fields


class PlatformManager:
    """Manager for multi-platform scoring configuration."""

    def __init__(self):
        """Initialize the PlatformManager with default platform configurations."""
        self.platforms: Dict[str, PlatformConfig] = {
            "twitter": PlatformConfig(
                name="twitter",
                emission_weight=0.5,  # 50% of emissions
                metrics=[
                    "scrapes",
                    "returned_tweets",
                    "auth_errors",
                    "errors",
                    "ratelimit_errors",
                ],
                error_metrics=["auth_errors", "errors", "ratelimit_errors"],
                success_metrics=["returned_tweets"],
                field_mappings={
                    # Map raw telemetry field names to clean platform metric names
                    "twitter_scrapes": "scrapes",
                    "twitter_returned_tweets": "returned_tweets",
                    "twitter_auth_errors": "auth_errors",
                    "twitter_errors": "errors",
                    "twitter_ratelimit_errors": "ratelimit_errors",
                    "twitter_returned_other": "returned_other",
                },
            ),
            "twitter-profile-apify": PlatformConfig(
                name="twitter-profile-apify",
                emission_weight=0,  # 0% of emissions
                metrics=[
                    "returned_profiles",
                ],
                error_metrics=[],
                success_metrics=["returned_profiles"],
                field_mappings={
                    "twitter_returned_profiles": "returned_profiles",
                },
            ),
            "twitter-followers-apify": PlatformConfig(
                name="twitter-followers-apify",
                emission_weight=0.2,  # 20% of emissions
                metrics=[
                    "returned_followers",
                ],
                error_metrics=[],
                success_metrics=["returned_followers"],
                field_mappings={
                    "twitter_returned_followers": "returned_followers",
                },
            ),
            "tiktok": PlatformConfig(
                name="tiktok",
                emission_weight=0.1,  # 10% of emissions
                metrics=["transcription_success", "transcription_errors"],
                error_metrics=["transcription_errors"],
                success_metrics=["transcription_success"],
                field_mappings={
                    # Map raw telemetry field names to clean platform metric names
                    "tiktok_transcription_success": "transcription_success",
                    "tiktok_transcription_errors": "transcription_errors",
                },
            ),
            "web": PlatformConfig(
                name="web",
                emission_weight=0.0,  # Not counted in emissions yet, but tracked
                metrics=["success", "errors"],
                error_metrics=["errors"],
                success_metrics=["success"],
                field_mappings={
                    "web_success": "success",
                    "web_errors": "errors",
                },
            ),
            "reddit": PlatformConfig(
                name="reddit",
                emission_weight=0.2,  # 20% of emissions
                metrics=["reddit_returned_items", "reddit_errors", "reddit_queries"],
                error_metrics=["reddit_errors"],
                success_metrics=["reddit_returned_items"],
                field_mappings={
                    "reddit_returned_items": "reddit_returned_items",
                    "reddit_errors": "reddit_errors",
                    "reddit_queries": "reddit_queries",
                },
            ),
        }

        # Validate that emission weights sum to 1.0
        total_weight = sum(config.emission_weight for config in self.platforms.values())
        if abs(total_weight - 1.0) > 1e-6:
            raise ValueError(
                f"Platform emission weights must sum to 1.0, got {total_weight}"
            )

        logger.info(f"Initialized PlatformManager with {len(self.platforms)} platforms")
        for name, config in self.platforms.items():
            logger.info(f"Platform {name}: {config.emission_weight*100:.1f}% emissions")

    def get_platform(self, name: str) -> PlatformConfig:
        """Get platform configuration by name."""
        if name not in self.platforms:
            raise ValueError(f"Unknown platform: {name}")
        return self.platforms[name]

    def get_all_platforms(self) -> Dict[str, PlatformConfig]:
        """Get all platform configurations."""
        return self.platforms

    def get_platform_names(self) -> List[str]:
        """Get list of all platform names."""
        return list(self.platforms.keys())

    def get_all_raw_field_names(self) -> List[str]:
        """Get all raw field names across all platforms."""
        all_fields = []
        for platform_config in self.platforms.values():
            for raw_field in platform_config.get_all_raw_field_names():
                if raw_field not in all_fields:
                    all_fields.append(raw_field)
        return all_fields

    def extract_platform_metrics_from_stats(
        self, stats_json: Dict
    ) -> Dict[str, Dict[str, int]]:
        """
        Extract platform metrics from raw stats JSON using field mappings.
        This is where the magic happens - raw telemetry gets organized by platform.
        """
        platform_metrics = {}

        for platform_name, platform_config in self.platforms.items():
            platform_metrics[platform_name] = {}

            # Extract metrics for this platform using field mappings
            for (
                raw_field_name,
                platform_metric_name,
            ) in platform_config.field_mappings.items():
                if raw_field_name in stats_json:
                    value = stats_json[raw_field_name]
                    if isinstance(value, (int, float)) and value >= 0:
                        platform_metrics[platform_name][platform_metric_name] = int(
                            value
                        )

        return platform_metrics

    def get_total_emission_weight(self) -> float:
        """Get total emission weight (should be 1.0)."""
        return sum(config.emission_weight for config in self.platforms.values())

    def add_platform(self, config: PlatformConfig) -> None:
        """Add a new platform configuration."""
        if config.name in self.platforms:
            raise ValueError(f"Platform {config.name} already exists")

        self.platforms[config.name] = config

        # Validate emission weights still sum to 1.0
        total_weight = sum(c.emission_weight for c in self.platforms.values())
        if abs(total_weight - 1.0) > 1e-6:
            logger.warning(f"Platform emission weights sum to {total_weight}, not 1.0")

        logger.info(
            f"Added platform {config.name} with {config.emission_weight*100:.1f}% emissions"
        )

    def update_platform_weights(self, weights: Dict[str, float]) -> None:
        """Update emission weights for platforms."""
        for platform_name, weight in weights.items():
            if platform_name not in self.platforms:
                raise ValueError(f"Unknown platform: {platform_name}")
            self.platforms[platform_name].emission_weight = weight

        # Validate that weights sum to 1.0
        total_weight = sum(config.emission_weight for config in self.platforms.values())
        if abs(total_weight - 1.0) > 1e-6:
            raise ValueError(
                f"Platform emission weights must sum to 1.0, got {total_weight}"
            )

        logger.info("Updated platform emission weights")
        for name, config in self.platforms.items():
            logger.info(f"Platform {name}: {config.emission_weight*100:.1f}% emissions")
