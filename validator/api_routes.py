from fastapi import FastAPI, Depends, HTTPException, Header, Body
from typing import Optional, Callable
import os
from fiber.logging_utils import get_logger
from datetime import datetime
import aiohttp

logger = get_logger(__name__)


def get_api_key(api_key: Optional[str] = Header(None, alias="X-API-Key")) -> str:
    """
    Dependency to check the API key in the header.

    :param api_key: The API key provided in the header.
    :return: The API key if valid.
    :raises HTTPException: If the API key is invalid or missing.
    """
    if not api_key:
        raise HTTPException(status_code=401, detail="API Key header missing")
    return api_key


def require_api_key(api_key: str = Depends(get_api_key), config=None) -> None:
    """
    Dependency to validate the API key against the configured value.

    :param api_key: The API key from the request header.
    :param config: The configuration object with API_KEY defined.
    :raises HTTPException: If the API key doesn't match the configured value or
                           no API key is configured.
    """
    # Check if the API key is valid
    if not config or not hasattr(config, "API_KEY") or not config.API_KEY:
        return  # No API key configured, skip validation

    if api_key != config.API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")


def register_routes(app: FastAPI, healthcheck_func):
    """
    Register API routes for the FastAPI application.

    :param app: The FastAPI application instance.
    :param healthcheck_func: The healthcheck function to register.
    """
    app.add_api_route(
        "/healthcheck",
        healthcheck_func,
        methods=["GET"],
        tags=["healthcheck"],
    )


class ValidatorAPI:
    def __init__(self, validator):
        self.validator = validator
        self.app = FastAPI()
        self.register_routes()

    def _nodedata_to_dict(self, data, format_type="dynamic") -> dict:
        """
        Convert NodeData object to dictionary using dynamic JSON structure.
        This replaces the old hardcoded field approach.

        Args:
            data: NodeData object
            format_type: "dynamic" for new format, "structured" for organized by platform
        """
        try:
            if format_type == "structured":
                # Structured format for user-friendly endpoints
                return {
                    "hotkey": data.hotkey,
                    "uid": data.uid,
                    "worker_id": getattr(data, "worker_id", None),
                    "timestamp": data.timestamp,
                    "timing": {
                        "boot_time": data.boot_time,
                        "last_operation_time": data.last_operation_time,
                        "current_time": data.current_time,
                    },
                    # Dynamic platform metrics organized by platform
                    "platform_metrics": getattr(data, "platform_metrics", {}) or {},
                    # All raw stats available dynamically
                    "raw_stats": getattr(data, "stats_json", {}) or {},
                }
            else:
                # Dynamic format for monitoring endpoints
                return {
                    # Core fields
                    "hotkey": data.hotkey,
                    "uid": data.uid,
                    "worker_id": getattr(data, "worker_id", None),
                    "timestamp": data.timestamp,
                    "boot_time": data.boot_time,
                    "last_operation_time": data.last_operation_time,
                    "current_time": data.current_time,
                    # Dynamic stats - include all telemetry data
                    "stats": getattr(data, "stats_json", {}) or {},
                    # Platform metrics organized by platform
                    "platform_metrics": getattr(data, "platform_metrics", {}) or {},
                }
        except Exception as e:
            # Fallback to basic structure if there are any errors
            return {
                "hotkey": getattr(data, "hotkey", "unknown"),
                "uid": getattr(data, "uid", 0),
                "timestamp": getattr(data, "timestamp", 0),
                "error": f"Error converting NodeData: {str(e)}",
                "raw_stats": getattr(data, "stats_json", {}),
                "platform_metrics": getattr(data, "platform_metrics", {}),
            }

    def _calculate_delta_summary(self, telemetry_data):
        """
        Calculate delta summary showing changes over time for key metrics.
        Uses the same delta calculation logic as the scoring system.
        """
        if len(telemetry_data) < 2:
            # Not enough data for delta calculation
            return {
                "delta_twitter_tweets": 0,
                "delta_twitter_profiles": 0,
                "delta_web_success": 0,
                "delta_tiktok_success": 0,
                "time_period": "N/A - insufficient data",
                "note": "Insufficient data for delta calculation (need at least 2 records)",
            }

        try:
            # Use the WeightsManager delta calculation logic
            from validator.weights import WeightsManager

            weights_manager = WeightsManager(self.validator)

            # Calculate deltas using the same logic as scoring
            delta_node_data = weights_manager._get_delta_node_data(telemetry_data)

            # Find delta data for this hotkey
            target_hotkey = telemetry_data[0].hotkey if telemetry_data else None
            delta_data = None

            for delta_node in delta_node_data:
                if delta_node.hotkey == target_hotkey:
                    delta_data = delta_node
                    break

            if delta_data and delta_data.platform_metrics:
                # Calculate time period for context
                sorted_data = sorted(telemetry_data, key=lambda x: x.timestamp)
                time_span_hours = (
                    sorted_data[-1].timestamp - sorted_data[0].timestamp
                ) / 3600

                return {
                    "delta_twitter_tweets": (
                        delta_data.platform_metrics.get("twitter", {}).get(
                            "returned_tweets", 0
                        )
                    ),
                    "delta_twitter_profiles": (
                        delta_data.platform_metrics.get(
                            "twitter-profile-apify", {}
                        ).get("returned_profiles", 0)
                    ),
                    "delta_web_success": (
                        delta_data.platform_metrics.get("web", {}).get("success", 0)
                    ),
                    "delta_tiktok_success": (
                        delta_data.platform_metrics.get("tiktok", {}).get(
                            "transcription_success", 0
                        )
                    ),
                    "time_period": f"{time_span_hours:.1f} hours ({len(telemetry_data)} records)",
                    "calculation_method": "WeightsManager delta logic (with restart detection)",
                }
            else:
                # Fallback to simple oldest->newest calculation
                return self._simple_delta_calculation(telemetry_data)

        except Exception as e:
            logger.error(f"Error calculating delta summary: {e}")
            # Fallback to simple calculation
            return self._simple_delta_calculation(telemetry_data)

    def _simple_delta_calculation(self, telemetry_data):
        """
        Simple delta calculation as fallback - newest minus oldest values.
        """
        if len(telemetry_data) < 2:
            return {
                "delta_twitter_tweets": 0,
                "delta_twitter_profiles": 0,
                "delta_web_success": 0,
                "delta_tiktok_success": 0,
            }

        # Sort by timestamp (oldest first for delta calculation)
        sorted_data = sorted(telemetry_data, key=lambda x: x.timestamp)
        oldest = sorted_data[0]
        newest = sorted_data[-1]

        def get_platform_metric(data, platform, metric, default=0):
            """Helper to safely get platform metrics"""
            if hasattr(data, "platform_metrics") and data.platform_metrics:
                return data.platform_metrics.get(platform, {}).get(metric, default)
            return default

        # Calculate time period for context
        time_span_hours = (newest.timestamp - oldest.timestamp) / 3600

        return {
            "delta_twitter_tweets": max(
                0,
                get_platform_metric(newest, "twitter", "returned_tweets")
                - get_platform_metric(oldest, "twitter", "returned_tweets"),
            ),
            "delta_twitter_profiles": max(
                0,
                get_platform_metric(
                    newest, "twitter-profile-apify", "returned_profiles"
                )
                - get_platform_metric(
                    oldest, "twitter-profile-apify", "returned_profiles"
                ),
            ),
            "delta_web_success": max(
                0,
                get_platform_metric(newest, "web", "success")
                - get_platform_metric(oldest, "web", "success"),
            ),
            "delta_tiktok_success": max(
                0,
                get_platform_metric(newest, "tiktok", "transcription_success")
                - get_platform_metric(oldest, "tiktok", "transcription_success"),
            ),
            "time_period": f"{time_span_hours:.1f} hours ({len(telemetry_data)} records)",
            "calculation_method": "Simple delta (newest - oldest)",
        }

    def get_api_key_dependency(self) -> Callable:
        """Get a dependency function that checks the API key against config."""

        def check_api_key():
            return require_api_key(config=self.validator.config)

        return check_api_key

    def register_routes(self) -> None:
        self.app.add_api_route(
            "/healthcheck",
            self.healthcheck,
            methods=["GET"],
            tags=["healthcheck"],
        )

        api_key_dependency = self.get_api_key_dependency()

        self.app.add_api_route(
            "/monitor/worker-registry",
            self.monitor_worker_registry,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/routing-table",
            self.monitor_routing_table,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/telemetry",
            self.monitor_telemetry,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/telemetry/all",
            self.monitor_all_telemetry,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/telemetry/{hotkey}",
            self.monitor_telemetry_by_hotkey,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/worker/{worker_id}",
            self.monitor_worker_hotkey,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/score-breakdown/{hotkey}",
            self.monitor_score_breakdown,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/leaderboard",
            self.monitor_leaderboard,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/monitor/integrity-summary",
            self.monitor_integrity_summary,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )
        self.app.add_api_route(
            "/add-unregistered-tee",
            self.add_unregistered_tee,
            methods=["POST"],
            tags=["management"],
            dependencies=[Depends(api_key_dependency)],
        )

    async def monitor_score_breakdown(self, hotkey: str):
        """
        Provide detailed score breakdown for a specific hotkey.
        Shows exactly how the score is calculated with platform-specific analysis,
        global rankings, and contribution details.
        """
        try:
            from validator.weights import WeightsManager
            from validator.platform_config import PlatformManager
            import numpy as np

            # Get telemetry data for scoring
            hours = 24
            # Get all telemetry data and filter by time
            all_telemetry_data = self.validator.telemetry_storage.get_all_telemetry()

            # Filter to last N hours
            import time

            current_time = int(time.time())
            cutoff_time = current_time - (hours * 3600)  # hours * 3600 seconds

            telemetry_data = []
            for data in all_telemetry_data:
                # Handle different timestamp formats
                data_time = data.timestamp
                if isinstance(data_time, str):
                    try:
                        from datetime import datetime

                        dt = datetime.fromisoformat(data_time.replace(" ", "T"))
                        data_time = int(dt.timestamp())
                    except:
                        continue
                elif data_time == 0:
                    # Skip records with no timestamp
                    continue

                if data_time >= cutoff_time:
                    telemetry_data.append(data)

            if not telemetry_data:
                return {
                    "hotkey": hotkey,
                    "error": "No telemetry data available",
                    "hours_analyzed": hours,
                }

            # Initialize scoring components
            weights_manager = WeightsManager(self.validator)
            platform_manager = PlatformManager()

            # Get delta data (same as used in actual scoring)
            delta_data = weights_manager._get_delta_node_data(telemetry_data)

            # Find the target node
            target_node = None
            for node in delta_data:
                if node.hotkey == hotkey:
                    target_node = node
                    break

            if not target_node:
                return {
                    "hotkey": hotkey,
                    "error": "Hotkey not found in current telemetry data",
                    "available_hotkeys": [
                        node.hotkey[:16] + "..." for node in delta_data[:5]
                    ],
                    "hours_analyzed": hours,
                }

            # Update platform metrics for all nodes
            weights_manager._update_platform_metrics(delta_data)

            # Calculate detailed breakdown
            breakdown = {
                "hotkey": hotkey,
                "analysis_timestamp": (
                    telemetry_data[0].timestamp if telemetry_data else 0
                ),
                "hours_analyzed": hours,
                "total_miners_analyzed": len(delta_data),
                "platforms": {},
                "final_score": 0.0,
                "global_ranking": {"position": 0, "percentile": 0.0},
                "scoring_methodology": {
                    "type": "success_only_normalized",
                    "description": "Per-platform successes normalized to [0,1], emission-weighted sum, then kurtosis",
                    "kurtosis_applied": True,
                },
            }

            # Calculate platform-specific normalized scores and rankings
            total_weighted_score = 0.0

            # Compute normalized arrays per platform across delta_data
            platform_names = platform_manager.get_platform_names()
            platform_to_raw = {name: [] for name in platform_names}
            for node in delta_data:
                for p in platform_names:
                    platform_to_raw[p].append(
                        weights_manager.calculate_platform_score(node, p)
                    )

            platform_to_raw = {k: np.array(v) for k, v in platform_to_raw.items()}
            platform_to_norm = {}
            for p, arr in platform_to_raw.items():
                if arr.size == 0:
                    platform_to_norm[p] = arr
                    continue
                max_val = float(np.max(arr))
                min_val = float(np.min(arr))
                if max_val - min_val > 0:
                    platform_to_norm[p] = (arr - min_val) / (max_val - min_val)
                else:
                    platform_to_norm[p] = np.zeros_like(arr)

            # Find target index
            target_index = next(
                (i for i, n in enumerate(delta_data) if n.hotkey == hotkey), -1
            )

            for platform_name in platform_names:
                platform_config = platform_manager.get_platform(platform_name)

                target_platform_score = (
                    float(platform_to_norm[platform_name][target_index])
                    if target_index >= 0 and platform_to_norm[platform_name].size > 0
                    else 0.0
                )
                weighted_contribution = (
                    target_platform_score * platform_config.emission_weight
                )
                total_weighted_score += weighted_contribution

                # Rankings on normalized scores
                all_platform_scores = platform_to_norm[platform_name]
                target_rank = (
                    int(np.sum(all_platform_scores > target_platform_score)) + 1
                    if all_platform_scores.size > 0
                    else len(delta_data)
                )
                percentile = (
                    (
                        (len(all_platform_scores) - target_rank + 1)
                        / len(all_platform_scores)
                    )
                    * 100
                    if all_platform_scores.size > 0 and len(all_platform_scores) > 0
                    else 0.0
                )

                # Metric totals (for display only)
                target_metrics = target_node.platform_metrics.get(platform_name, {})
                success_total = sum(
                    target_metrics.get(metric, 0)
                    for metric in platform_config.success_metrics
                )

                breakdown["platforms"][platform_name] = {
                    "platform_score": round(target_platform_score, 4),
                    "emission_weight": platform_config.emission_weight,
                    "weighted_contribution": round(weighted_contribution, 4),
                    "global_ranking": {
                        "position": int(target_rank),
                        "out_of": len(delta_data),
                        "percentile": round(percentile, 1),
                    },
                    "metrics_breakdown": {
                        "success_metrics": {
                            metric: target_metrics.get(metric, 0)
                            for metric in platform_config.success_metrics
                        },
                        "success_total": success_total,
                    },
                }

            # Calculate final score with kurtosis using normalized platform sums
            all_combined_scores = []
            for idx, node in enumerate(delta_data):
                node_total = 0.0
                for platform_name in platform_names:
                    platform_config = platform_manager.get_platform(platform_name)
                    norm_score = (
                        float(platform_to_norm[platform_name][idx])
                        if platform_to_norm[platform_name].size > 0
                        else 0.0
                    )
                    node_total += norm_score * platform_config.emission_weight
                all_combined_scores.append(node_total)

            # Apply kurtosis to get final scores
            from validator.weights import apply_kurtosis_custom

            final_scores = apply_kurtosis_custom(np.array(all_combined_scores))

            # Find target node's final score and ranking
            target_final_score = 0.0
            for idx, node in enumerate(delta_data):
                if node.hotkey == hotkey:
                    target_final_score = final_scores[idx]
                    break

            final_rank = np.sum(final_scores > target_final_score) + 1
            final_percentile = (
                (len(final_scores) - final_rank + 1) / len(final_scores)
            ) * 100

            breakdown["final_score"] = round(float(target_final_score), 4)
            breakdown["pre_kurtosis_score"] = round(total_weighted_score, 4)
            breakdown["global_ranking"] = {
                "position": int(final_rank),
                "out_of": len(delta_data),
                "percentile": round(final_percentile, 1),
            }

            # Add summary explanation
            breakdown["summary"] = {
                "explanation": f"Final score of {breakdown['final_score']} achieved through:",
                "contributions": [
                    f"{platform}: {data['weighted_contribution']} (rank #{data['global_ranking']['position']}/{data['global_ranking']['out_of']})"
                    for platform, data in breakdown["platforms"].items()
                    if data["weighted_contribution"] > 0
                ],
                "kurtosis_effect": f"Kurtosis curve applied: {breakdown['pre_kurtosis_score']} → {breakdown['final_score']}",
            }

            return breakdown

        except Exception as e:
            logger.error(f"Failed to generate score breakdown for {hotkey}: {str(e)}")
            import traceback

            traceback.print_exc()
            return {
                "hotkey": hotkey,
                "error": f"Failed to generate breakdown: {str(e)}",
                "traceback": traceback.format_exc(),
            }

    async def monitor_leaderboard(
        self,
        hours: int = 24,
        limit: int = 1000,
        offset: int = 0,
        sort_by: str = "final_score",
    ):
        """
        Get comprehensive leaderboard with score breakdowns for all miners.
        Shows detailed scoring information for the entire network.

        Args:
            hours: Hours of telemetry data to analyze (default: 24)
            limit: Maximum number of miners to return (default: 1000, use 0 for ALL)
            offset: Number of miners to skip before returning results (default: 0)
            sort_by: Sort criteria - "final_score", "total_activity", "total_weighted_score" (default: "final_score")
        """
        try:
            from validator.weights import WeightsManager, apply_kurtosis_custom
            from validator.platform_config import PlatformManager
            import numpy as np
            import time

            safe_hours = max(1, int(hours))
            safe_limit = max(0, int(limit))
            safe_offset = max(0, int(offset))

            logger.info(
                f"Generating leaderboard for {safe_hours}h with limit {safe_limit}, "
                f"offset {safe_offset}, sorted by {sort_by}"
            )

            # Get telemetry data for scoring
            all_telemetry_data = self.validator.telemetry_storage.get_all_telemetry()

            # Filter to last N hours
            current_time = int(time.time())
            cutoff_time = current_time - (safe_hours * 3600)

            telemetry_data = []
            for data in all_telemetry_data:
                # Handle different timestamp formats
                data_time = data.timestamp
                if isinstance(data_time, str):
                    try:
                        from datetime import datetime

                        dt = datetime.fromisoformat(data_time.replace(" ", "T"))
                        data_time = int(dt.timestamp())
                    except:
                        continue
                elif data_time == 0:
                    continue

                if data_time >= cutoff_time:
                    telemetry_data.append(data)

            if not telemetry_data:
                return {
                    "error": "No telemetry data available for the specified time period",
                    "hours_analyzed": safe_hours,
                    "leaderboard": [],
                }

            # Initialize scoring components
            weights_manager = WeightsManager(self.validator)
            platform_manager = PlatformManager()

            # Get delta data (same as used in actual scoring)
            delta_data = weights_manager._get_delta_node_data(telemetry_data)

            if not delta_data:
                return {
                    "error": "No delta data available",
                    "hours_analyzed": safe_hours,
                    "leaderboard": [],
                }

            # Calculate scores for all miners
            leaderboard = []

            for target_node in delta_data:
                miner_data = {
                    "hotkey": target_node.hotkey,
                    "uid": getattr(target_node, "uid", "unknown"),
                    "platforms": {},
                    "total_activity": 0,
                    "total_weighted_score": 0.0,
                    "final_score": 0.0,
                }

                # Calculate platform-specific scores
                for platform_name in platform_manager.get_platform_names():
                    platform_config = platform_manager.get_platform(platform_name)
                    target_metrics = target_node.platform_metrics.get(platform_name, {})

                    # Calculate totals for this platform
                    success_total = sum(
                        target_metrics.get(m, 0)
                        for m in platform_config.success_metrics
                    )
                    error_total = sum(
                        target_metrics.get(m, 0) for m in platform_config.error_metrics
                    )

                    # Calculate error rate
                    time_span_seconds = getattr(target_node, "time_span_seconds", 0)
                    if time_span_seconds > 0:
                        hours_span = time_span_seconds / 3600
                        error_rate = error_total / hours_span
                    else:
                        error_rate = float("inf") if error_total > 0 else 0.0

                    error_quality = (
                        1.0 / (1.0 + error_rate) if error_rate != float("inf") else 0.0
                    )

                    # Apply the same bug fix as in scoring
                    if success_total == 0:
                        error_quality = 0.0

                    # Get platform score from actual scoring system
                    platform_score = weights_manager.calculate_platform_score(
                        target_node, platform_name
                    )
                    weighted_contribution = (
                        platform_score * platform_config.emission_weight
                    )

                    miner_data["platforms"][platform_name] = {
                        "success_total": success_total,
                        "error_total": error_total,
                        "error_rate_per_hour": round(
                            error_rate if error_rate != float("inf") else 0, 2
                        ),
                        "error_quality": round(error_quality, 4),
                        "platform_score": round(platform_score, 4),
                        "emission_weight": platform_config.emission_weight,
                        "weighted_contribution": round(weighted_contribution, 4),
                    }

                    # Add to totals
                    miner_data["total_activity"] += success_total
                    miner_data["total_weighted_score"] += weighted_contribution

                # Calculate final score with kurtosis (same as actual scoring)
                all_combined_scores = [
                    node.get_stat_value("combined_score", 0) for node in delta_data
                ]

                if len(all_combined_scores) > 1 and any(
                    score > 0 for score in all_combined_scores
                ):
                    try:
                        kurtosis_scores = apply_kurtosis_custom(
                            np.array(all_combined_scores)
                        )
                        # Find this miner's position in delta_data to get corresponding kurtosis score
                        for i, node in enumerate(delta_data):
                            if node.hotkey == target_node.hotkey:
                                miner_data["final_score"] = round(
                                    float(kurtosis_scores[i]), 6
                                )
                                break
                    except Exception as e:
                        logger.warning(
                            f"Kurtosis calculation failed for {target_node.hotkey}: {e}"
                        )
                        miner_data["final_score"] = round(
                            miner_data["total_weighted_score"], 6
                        )
                else:
                    miner_data["final_score"] = round(
                        miner_data["total_weighted_score"], 6
                    )

                leaderboard.append(miner_data)

            # Sort leaderboard based on criteria
            if sort_by == "total_activity":
                leaderboard.sort(key=lambda x: x["total_activity"], reverse=True)
            elif sort_by == "total_weighted_score":
                leaderboard.sort(key=lambda x: x["total_weighted_score"], reverse=True)
            else:  # default: final_score
                leaderboard.sort(key=lambda x: x["final_score"], reverse=True)

            # Apply offset + limit paging. limit=0 means "all from offset onward".
            total_miners = len(leaderboard)
            if safe_limit == 0:
                returned_leaderboard = leaderboard[safe_offset:]
                returned_count = len(returned_leaderboard)
                limit_description = "ALL"
            else:
                returned_leaderboard = leaderboard[safe_offset : safe_offset + safe_limit]
                returned_count = len(returned_leaderboard)
                limit_description = str(safe_limit)

            # Add global rankings (rank reflects full sorted leaderboard, not page index)
            for i, miner in enumerate(returned_leaderboard):
                miner["rank"] = safe_offset + i + 1

            return {
                "success": True,
                "hours_analyzed": safe_hours,
                "cutoff_time": cutoff_time,
                "analysis_time": current_time,
                "total_miners": total_miners,
                "returned_miners": returned_count,
                "limit_applied": limit_description,
                "offset_applied": safe_offset,
                "has_more": (safe_offset + returned_count) < total_miners,
                "sort_criteria": sort_by,
                "summary": {
                    "active_miners": len(
                        [m for m in leaderboard if m["total_activity"] > 0]
                    ),
                    "total_network_activity": sum(
                        m["total_activity"] for m in leaderboard
                    ),
                    "average_final_score": (
                        round(
                            sum(m["final_score"] for m in leaderboard)
                            / len(leaderboard),
                            4,
                        )
                        if leaderboard
                        else 0
                    ),
                    "top_score": leaderboard[0]["final_score"] if leaderboard else 0,
                    "platforms_analyzed": list(platform_manager.get_platform_names()),
                },
                "leaderboard": returned_leaderboard,
            }

        except Exception as e:
            logger.error(f"Failed to generate leaderboard: {str(e)}")
            import traceback

            traceback.print_exc()
            return {
                "error": f"Failed to generate leaderboard: {str(e)}",
                "traceback": traceback.format_exc(),
                "leaderboard": [],
            }

    async def monitor_integrity_summary(self, hours: int = 8):
        """
        Return compact anomaly and integrity indicators for anti-cheat monitoring.
        """
        try:
            import time
            import numpy as np
            from collections import defaultdict
            from validator.weights import WeightsManager

            def gini(values):
                vals = np.array([v for v in values if v >= 0], dtype=float)
                if vals.size == 0 or np.all(vals == 0):
                    return 0.0
                vals = np.sort(vals)
                n = vals.size
                idx = np.arange(1, n + 1)
                return float(
                    (np.sum((2 * idx - n - 1) * vals) / (n * np.sum(vals)))
                )

            def top_share(values, n):
                if not values:
                    return 0.0
                ordered = sorted(values, reverse=True)
                total = sum(ordered)
                if total <= 0:
                    return 0.0
                return float(sum(ordered[: min(n, len(ordered))]) / total)

            current_time = int(time.time())
            cutoff_time = current_time - (max(1, int(hours)) * 3600)

            # Gather and time-filter telemetry records
            all_telemetry_data = self.validator.telemetry_storage.get_all_telemetry()
            telemetry_data = []
            for data in all_telemetry_data:
                data_time = data.timestamp
                if isinstance(data_time, str):
                    try:
                        from datetime import datetime

                        dt = datetime.fromisoformat(data_time.replace(" ", "T"))
                        data_time = int(dt.timestamp())
                    except Exception:
                        continue
                elif data_time == 0:
                    continue

                if data_time >= cutoff_time:
                    telemetry_data.append(data)

            # Derive delta data in the same way scoring does
            weights_manager = WeightsManager(self.validator)
            delta_data = weights_manager._get_delta_node_data(telemetry_data)

            # Pull leaderboard summary for score-share diagnostics
            leaderboard_data = await self.monitor_leaderboard(hours=hours, limit=0)
            leaderboard = leaderboard_data.get("leaderboard", [])
            score_values = [float(m.get("final_score", 0.0)) for m in leaderboard]
            activity_values = [float(m.get("total_activity", 0.0)) for m in leaderboard]

            # Identity integrity checks
            worker_regs = self.validator.routing_table.get_all_worker_registrations()
            routing_rows = self.validator.routing_table.get_all_addresses_with_hotkeys()

            worker_to_hotkeys_registry = defaultdict(set)
            hotkey_to_workers_registry = defaultdict(set)
            for worker_id, hotkey in worker_regs:
                worker_to_hotkeys_registry[worker_id].add(hotkey)
                hotkey_to_workers_registry[hotkey].add(worker_id)

            worker_to_hotkeys_routing = defaultdict(set)
            hotkey_to_workers_routing = defaultdict(set)
            address_to_hotkeys = defaultdict(set)
            for hotkey, address, worker_id in routing_rows:
                if worker_id:
                    worker_to_hotkeys_routing[worker_id].add(hotkey)
                    hotkey_to_workers_routing[hotkey].add(worker_id)
                if address:
                    address_to_hotkeys[address].add(hotkey)

            worker_id_to_hotkey_violations = sorted(
                [
                    {
                        "worker_id": worker_id,
                        "hotkeys": sorted(list(hotkeys)),
                    }
                    for worker_id, hotkeys in worker_to_hotkeys_routing.items()
                    if len(hotkeys) > 1
                ],
                key=lambda x: len(x["hotkeys"]),
                reverse=True,
            )
            address_to_hotkey_violations = sorted(
                [
                    {
                        "address": address,
                        "hotkeys": sorted(list(hotkeys)),
                    }
                    for address, hotkeys in address_to_hotkeys.items()
                    if len(hotkeys) > 1
                ],
                key=lambda x: len(x["hotkeys"]),
                reverse=True,
            )
            hotkeys_with_multiple_worker_ids = sorted(
                [
                    {
                        "hotkey": hotkey,
                        "worker_ids": sorted(list(worker_ids)),
                    }
                    for hotkey, worker_ids in hotkey_to_workers_registry.items()
                    if len(worker_ids) > 1
                ],
                key=lambda x: len(x["worker_ids"]),
                reverse=True,
            )

            # Compare active routing worker against registry set
            hotkey_to_active_worker_routing = {
                hotkey: worker_id
                for hotkey, _, worker_id in routing_rows
                if worker_id
            }
            routing_registry_mismatches = []
            for hotkey, active_worker in hotkey_to_active_worker_routing.items():
                known_workers = hotkey_to_workers_registry.get(hotkey, set())
                if known_workers and active_worker not in known_workers:
                    routing_registry_mismatches.append(
                        {
                            "hotkey": hotkey,
                            "active_routing_worker_id": active_worker,
                            "registry_worker_ids": sorted(list(known_workers)),
                        }
                    )

            # Delta integrity checks
            nonzero_deltas = []
            for node in delta_data:
                total = 0
                for value in (node.stats_json or {}).values():
                    if isinstance(value, (int, float)):
                        total += value
                nonzero_deltas.append((node.hotkey, float(total)))
            nonzero_deltas.sort(key=lambda x: x[1], reverse=True)

            zero_delta_hotkeys = [
                hotkey for hotkey, total in nonzero_deltas if total == 0.0
            ]

            active_count = len([v for v in activity_values if v > 0])
            total_activity = float(sum(activity_values))
            expected_per_active = (
                float(total_activity / active_count) if active_count > 0 else 0.0
            )
            chi_square_activity = 0.0
            if expected_per_active > 0:
                chi_square_activity = float(
                    sum(
                        ((v - expected_per_active) ** 2) / expected_per_active
                        for v in activity_values
                        if v > 0
                    )
                )

            return {
                "success": True,
                "window": {
                    "hours_analyzed": int(hours),
                    "cutoff_time": cutoff_time,
                    "analysis_time": current_time,
                    "records_used": len(telemetry_data),
                    "miners_scored": len(delta_data),
                },
                "distribution": {
                    "active_miners": int(active_count),
                    "total_network_activity": int(total_activity),
                    "expected_share_per_miner": (
                        float(1.0 / active_count) if active_count > 0 else 0.0
                    ),
                    "top1_score_share": round(top_share(score_values, 1), 6),
                    "top5_score_share": round(top_share(score_values, 5), 6),
                    "top10_score_share": round(top_share(score_values, 10), 6),
                    "top1_activity_share": round(top_share(activity_values, 1), 6),
                    "top5_activity_share": round(top_share(activity_values, 5), 6),
                    "top10_activity_share": round(top_share(activity_values, 10), 6),
                    "gini_score": round(gini(score_values), 6),
                    "gini_activity": round(gini(activity_values), 6),
                    "chi_square_activity": round(chi_square_activity, 4),
                },
                "identity_integrity": {
                    "worker_id_to_hotkey_violations": worker_id_to_hotkey_violations,
                    "address_to_hotkey_violations": address_to_hotkey_violations,
                    "hotkeys_with_multiple_worker_ids": hotkeys_with_multiple_worker_ids,
                    "routing_registry_mismatches": routing_registry_mismatches,
                },
                "delta_integrity": {
                    "zero_delta_hotkeys_count": len(zero_delta_hotkeys),
                    "zero_delta_hotkeys_sample": zero_delta_hotkeys[:25],
                    "largest_delta_hotkeys": [
                        {"hotkey": hotkey, "delta_sum": int(total)}
                        for hotkey, total in nonzero_deltas[:10]
                    ],
                },
            }
        except Exception as e:
            logger.error(f"Failed to generate integrity summary: {str(e)}")
            return {"success": False, "error": str(e)}

    async def healthcheck(self):
        # Implement the healthcheck logic for the validator
        return self.validator.healthcheck()

    async def monitor_worker_registry(self):
        """Return all worker registrations (worker_id to hotkey mappings)"""
        try:
            registrations = self.validator.routing_table.get_all_worker_registrations()
            worker_registrations = []

            for worker_id, hotkey in registrations:
                # Check if the worker is in the routing table (active)
                miner_addresses = self.validator.routing_table.get_miner_addresses(
                    hotkey
                )
                is_in_routing_table = len(miner_addresses) > 0

                worker_registrations.append(
                    {
                        "worker_id": worker_id,
                        "hotkey": hotkey,
                        "is_in_routing_table": is_in_routing_table,
                    }
                )

            return {
                "count": len(registrations),
                "worker_registrations": worker_registrations,
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_routing_table(self):
        """Return all miner addresses and their associated hotkeys"""
        try:
            addresses = self.validator.routing_table.get_all_addresses_with_hotkeys()
            nodes_count = len(self.validator.metagraph.nodes)

            return {
                "count": nodes_count,
                "miner_addresses": [
                    {
                        "hotkey": hotkey,
                        "address": address,
                        "worker_id": worker_id if worker_id else None,
                    }
                    for hotkey, address, worker_id in addresses
                ],
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_telemetry(self):
        """Return a list of hotkeys that have telemetry data"""
        try:
            hotkeys = self.validator.telemetry_storage.get_all_hotkeys_with_telemetry()
            return {"count": len(hotkeys), "hotkeys": hotkeys}
        except Exception as e:
            return {"error": str(e)}

    async def monitor_worker_hotkey(self, worker_id: str):
        """Return the hotkey associated with a worker_id"""

        try:
            hotkey = self.validator.routing_table.get_worker_hotkey(worker_id)
            if hotkey:
                return {"worker_id": worker_id, "hotkey": hotkey}
            else:
                return {
                    "worker_id": worker_id,
                    "hotkey": None,
                    "message": "Worker ID not found",
                }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_telemetry_by_hotkey(self, hotkey: str):
        """Return telemetry data for a specific hotkey"""
        try:
            telemetry_data = self.validator.telemetry_storage.get_telemetry_by_hotkey(
                hotkey
            )

            # Convert NodeData objects to dictionaries using dynamic approach
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict_list.append(self._nodedata_to_dict(data))

            return {
                "hotkey": hotkey,
                "count": len(telemetry_dict_list),
                "telemetry_data": telemetry_dict_list,
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_errors(self, limit: int = 100):
        """Return all errors logged in the system"""
        try:
            # Get errors storage from node manager since that's where it's initialized
            errors_storage = self.validator.node_manager.errors_storage
            errors = errors_storage.get_all_errors(limit)

            return {
                "count": len(errors),
                "errors": errors,
                "error_count_24h": errors_storage.get_error_count(hours=24),
                "error_count_1h": errors_storage.get_error_count(hours=1),
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_errors_by_hotkey(self, hotkey: str, limit: int = 100):
        """Return errors for a specific hotkey"""
        try:
            errors_storage = self.validator.node_manager.errors_storage
            errors = errors_storage.get_errors_by_hotkey(hotkey, limit)

            return {
                "hotkey": hotkey,
                "count": len(errors),
                "errors": errors,
            }
        except Exception as e:
            return {"error": str(e)}

    async def cleanup_old_errors(self):
        """Manually trigger cleanup of error logs based on retention period"""
        try:
            errors_storage = self.validator.node_manager.errors_storage
            retention_days = errors_storage.retention_days
            count = errors_storage.clean_errors_based_on_retention()

            return {
                "success": True,
                "retention_days": retention_days,
                "removed_count": count,
                "message": f"Removed {count} error logs older than {retention_days} days",
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def monitor_unregistered_tee_addresses(self):
        """Return all unregistered TEE addresses in the system"""
        try:
            addresses = (
                self.validator.routing_table.get_all_unregistered_tee_addresses()
            )
            return {
                "count": len(addresses),
                "unregistered_tee_addresses": addresses,
            }
        except Exception as e:
            return {"error": str(e)}

    async def add_unregistered_tee(
        self, address: str = Body(...), hotkey: str = Body(...)
    ):
        """
        Register a TEE worker with the MASA TEE API.

        :param address: The TEE address to register
        :param hotkey: The hotkey associated with the TEE address
        :return: Success or error message
        """
        # Get process monitor from background tasks
        process_monitor = getattr(self.validator, "background_tasks", None)
        if process_monitor:
            process_monitor = getattr(process_monitor, "process_monitor", None)

        execution_id = None

        try:
            # Start monitoring for this TEE registration
            if process_monitor:
                execution_id = process_monitor.start_process("add_unregistered_tee")

            # Validate input
            if not address or not hotkey:
                error_msg = "Both 'address' and 'hotkey' are required fields"

                # Update metrics for validation error
                if execution_id and process_monitor:
                    process_monitor.update_metrics(
                        execution_id,
                        nodes_processed=0,
                        successful_nodes=0,
                        failed_nodes=1,
                        errors=[error_msg],
                        additional_metrics={
                            "address": address,
                            "hotkey": hotkey,
                            "validation_error": True,
                        },
                    )
                    process_monitor.end_process(execution_id)

                raise HTTPException(
                    status_code=400,
                    detail=error_msg,
                )

            # Get the API URL from environment variables
            masa_tee_api = os.getenv("MASA_TEE_API", "")
            masa_tee_api_key = os.getenv("MASA_TEE_API_KEY", "")

            if not masa_tee_api:
                error_msg = "MASA_TEE_API environment variable not set"
                logger.error(error_msg)

                # Update metrics for environment variable error
                if execution_id and process_monitor:
                    process_monitor.update_metrics(
                        execution_id,
                        nodes_processed=0,
                        successful_nodes=0,
                        failed_nodes=1,
                        errors=[error_msg],
                        additional_metrics={
                            "address": address,
                            "hotkey": hotkey,
                            "env_var_error": "MASA_TEE_API",
                        },
                    )
                    process_monitor.end_process(execution_id)

                return {
                    "success": False,
                    "error": error_msg,
                }
            if not masa_tee_api_key:
                error_msg = "MASA_TEE_API_KEY environment variable not set"

                # Update metrics for environment variable error
                if execution_id and process_monitor:
                    process_monitor.update_metrics(
                        execution_id,
                        nodes_processed=0,
                        successful_nodes=0,
                        failed_nodes=1,
                        errors=[error_msg],
                        additional_metrics={
                            "address": address,
                            "hotkey": hotkey,
                            "env_var_error": "MASA_TEE_API_KEY",
                        },
                    )
                    process_monitor.end_process(execution_id)

                return {
                    "success": False,
                    "error": error_msg,
                }
            if not masa_tee_api_key:
                return {
                    "success": False,
                    "error": "MASA_TEE_API environment variable not set",
                }

            # Format the API endpoint (normalize URL and append endpoint)
            base_url = masa_tee_api.rstrip("/")
            api_endpoint = f"{base_url}/register-tee-worker"

            # Format the payload
            payload = {"address": address}

            # Make the API call
            logger.info(f"Calling MASA TEE API to register TEE worker: {address}")

            # Prepare headers with API key
            headers = {
                "X-API-Key": masa_tee_api_key,
                "Content-Type": "application/json",
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    api_endpoint, json=payload, headers=headers
                ) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        logger.info(
                            f"Successfully registered TEE worker with MASA API: {address}"
                        )

                        # Update metrics for successful registration
                        if execution_id and process_monitor:
                            process_monitor.update_metrics(
                                execution_id,
                                nodes_processed=1,
                                successful_nodes=1,
                                failed_nodes=0,
                                additional_metrics={
                                    "address": address,
                                    "hotkey": hotkey,
                                    "api_endpoint": api_endpoint,
                                    "response_status": response.status,
                                    "api_response": response_data,
                                },
                            )
                            process_monitor.end_process(execution_id)
                            execution_id = None

                        return {
                            "success": True,
                            "message": f"Successfully registered TEE worker: {address}",
                            "api_response": response_data,
                        }
                    else:
                        response_text = await response.text()
                        error_msg = (
                            f"API call failed with status {response.status}: "
                            f"{response_text}"
                        )
                        logger.error(
                            f"Failed to register TEE worker with MASA API: "
                            f"{response.status} - {response_text}"
                        )

                        # Update metrics for API failure
                        if execution_id and process_monitor:
                            process_monitor.update_metrics(
                                execution_id,
                                nodes_processed=1,
                                successful_nodes=0,
                                failed_nodes=1,
                                errors=[error_msg],
                                additional_metrics={
                                    "address": address,
                                    "hotkey": hotkey,
                                    "api_endpoint": api_endpoint,
                                    "response_status": response.status,
                                    "response_text": response_text,
                                },
                            )
                            process_monitor.end_process(execution_id)
                            execution_id = None

                        return {"success": False, "error": error_msg}

        except aiohttp.ClientError as e:
            error_msg = f"API connection error: {str(e)}"
            logger.error(error_msg)

            # Update metrics for connection error
            if execution_id and process_monitor:
                process_monitor.update_metrics(
                    execution_id,
                    nodes_processed=1,
                    successful_nodes=0,
                    failed_nodes=1,
                    errors=[error_msg],
                    additional_metrics={
                        "address": address,
                        "hotkey": hotkey,
                        "connection_error": True,
                        "error_type": "aiohttp.ClientError",
                    },
                )
                process_monitor.end_process(execution_id)
                execution_id = None

            return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"Failed to register TEE worker: {str(e)}"
            logger.error(error_msg)

            # Update metrics for unexpected error
            if execution_id and process_monitor:
                process_monitor.update_metrics(
                    execution_id,
                    nodes_processed=1,
                    successful_nodes=0,
                    failed_nodes=1,
                    errors=[error_msg],
                    additional_metrics={
                        "address": address,
                        "hotkey": hotkey,
                        "unexpected_error": True,
                        "error_type": type(e).__name__,
                    },
                )
                process_monitor.end_process(execution_id)
                execution_id = None

            return {"success": False, "error": str(e)}

    async def monitor_processes(self):
        """Return process monitoring statistics for background tasks"""
        try:
            # Get process monitoring data from the background tasks
            if hasattr(self.validator, "background_tasks") and hasattr(
                self.validator.background_tasks, "process_monitor"
            ):
                monitor = self.validator.background_tasks.process_monitor
                return monitor.get_all_processes_statistics()
            else:
                return {
                    "error": "Process monitoring not available",
                    "monitoring_status": {
                        "active_executions": 0,
                        "monitored_processes": [],
                        "max_records_per_process": 0,
                        "timestamp": datetime.now().isoformat(),
                    },
                    "processes": {},
                }
        except Exception as e:
            logger.error(f"Failed to get process monitoring data: {str(e)}")
            return {"error": str(e)}

    async def monitor_nats_publishing(self):
        """Return NATS monitoring statistics for publishing"""
        try:
            # Get process monitoring data from the background tasks
            if hasattr(self.validator, "background_tasks") and hasattr(
                self.validator.background_tasks, "process_monitor"
            ):
                monitor = self.validator.background_tasks.process_monitor
                # Get statistics specifically for send_connected_nodes process
                nats_stats = monitor.get_process_statistics("send_connected_nodes")
                return {
                    "monitoring_status": {
                        "active_executions": len(
                            [
                                exec_id
                                for exec_id, exec_data in monitor.current_executions.items()
                                if exec_data.get("process_name")
                                == "send_connected_nodes"
                            ]
                        ),
                        "process_name": "send_connected_nodes",
                        "timestamp": datetime.now().isoformat(),
                    },
                    "nats_publishing": nats_stats,
                }
            else:
                return {
                    "error": "Process monitoring not available",
                    "monitoring_status": {
                        "active_executions": 0,
                        "process_name": "send_connected_nodes",
                        "timestamp": datetime.now().isoformat(),
                    },
                }
        except Exception as e:
            logger.error(f"Failed to get NATS monitoring data: {str(e)}")
            return {"error": str(e)}

    async def trigger_send_connected_nodes(self):
        """Trigger NATS send_connected_nodes process"""
        try:
            # Check if the validator has a NATSPublisher
            if hasattr(self.validator, "NATSPublisher"):
                # Call the send_connected_nodes method directly
                await self.validator.NATSPublisher.send_connected_nodes()

                return {
                    "success": True,
                    "message": "NATS send_connected_nodes process triggered successfully",
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                return {
                    "success": False,
                    "error": "NATSPublisher not available",
                    "timestamp": datetime.now().isoformat(),
                }
        except Exception as e:
            logger.error(
                f"Failed to trigger NATS send_connected_nodes process: {str(e)}"
            )
            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    async def trigger_telemetry_fetch(self):
        """Trigger telemetry fetching process manually"""
        try:
            # Check if the validator has a scorer (NodeDataScorer)
            if not hasattr(self.validator, "scorer"):
                return {
                    "success": False,
                    "error": "NodeDataScorer not available",
                    "timestamp": datetime.now().isoformat(),
                }

            # Get process monitor for tracking if available
            process_monitor = None
            if hasattr(self.validator, "background_tasks") and hasattr(
                self.validator.background_tasks, "process_monitor"
            ):
                process_monitor = self.validator.background_tasks.process_monitor

            execution_id = None

            # Start monitoring for this manual telemetry fetch
            if process_monitor:
                execution_id = process_monitor.start_process("manual_telemetry_fetch")

            logger.info("Manual telemetry fetch triggered via API")

            # Call the telemetry fetching method
            await self.validator.scorer.get_node_data()

            # Update metrics for successful execution
            if execution_id and process_monitor:
                connected_nodes_count = len(self.validator.node_manager.connected_nodes)
                process_monitor.update_metrics(
                    execution_id,
                    nodes_processed=connected_nodes_count,
                    successful_nodes=connected_nodes_count,
                    failed_nodes=0,
                    additional_metrics={
                        "trigger_source": "api_manual",
                        "connected_nodes": connected_nodes_count,
                    },
                )
                process_monitor.end_process(execution_id)

            logger.info("Manual telemetry fetch completed successfully")

            return {
                "success": True,
                "message": "Telemetry fetching process triggered successfully",
                "timestamp": datetime.now().isoformat(),
                "nodes_processed": (
                    len(self.validator.node_manager.connected_nodes)
                    if hasattr(self.validator, "node_manager")
                    else 0
                ),
            }

        except Exception as e:
            logger.error(f"Failed to trigger telemetry fetch process: {str(e)}")

            # Update metrics for failed execution
            if execution_id and process_monitor:
                process_monitor.update_metrics(
                    execution_id,
                    nodes_processed=0,
                    successful_nodes=0,
                    failed_nodes=1,
                    errors=[str(e)],
                    additional_metrics={
                        "trigger_source": "api_manual",
                        "error_type": type(e).__name__,
                    },
                )
                process_monitor.end_process(execution_id)

            return {
                "success": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }

    async def get_telemetry_by_hotkey(self, hotkey: str):
        """Get telemetry data for a specific hotkey address"""
        try:
            # Get telemetry data from storage
            telemetry_data = self.validator.telemetry_storage.get_telemetry_by_hotkey(
                hotkey
            )

            if not telemetry_data:
                return {
                    "hotkey": hotkey,
                    "found": False,
                    "message": "No telemetry data found for this hotkey",
                    "count": 0,
                    "telemetry_data": [],
                }

            # Convert NodeData objects to dictionaries with structured format
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict_list.append(self._nodedata_to_dict(data, "structured"))

            # Sort by timestamp (most recent first)
            telemetry_dict_list.sort(key=lambda x: x["timestamp"], reverse=True)

            # Calculate delta summary (changes over time)
            delta_summary = self._calculate_delta_summary(telemetry_data)

            # Get latest record for metadata
            latest = telemetry_dict_list[0] if telemetry_dict_list else None

            return {
                "hotkey": hotkey,
                "found": True,
                "count": len(telemetry_dict_list),
                "latest_timestamp": latest["timestamp"] if latest else None,
                "worker_id": latest["worker_id"] if latest else None,
                "summary": delta_summary,
                "telemetry_data": telemetry_dict_list,
            }

        except Exception as e:
            logger.error(f"Failed to get telemetry for hotkey {hotkey}: {str(e)}")
            return {
                "hotkey": hotkey,
                "found": False,
                "error": str(e),
                "count": 0,
                "telemetry_data": [],
            }

    async def get_live_telemetry_by_hotkey(self, hotkey: str):
        """Get fresh telemetry data directly from miner worker by hotkey"""
        try:
            # Find the miner's address from routing table
            all_addresses = (
                self.validator.routing_table.get_all_addresses_with_hotkeys()
            )

            # Find matching hotkey
            miner_address = None
            worker_id = None
            for h, addr, w_id in all_addresses:
                if h == hotkey:
                    miner_address = addr
                    worker_id = w_id
                    break

            if not miner_address:
                return {
                    "hotkey": hotkey,
                    "found": False,
                    "error": "Miner not found in routing table",
                    "message": "No miner found with this hotkey address",
                }

            logger.info(
                f"Fetching live telemetry from {hotkey[:10]}... at {miner_address}"
            )

            # Import TEETelemetryClient for direct connection
            from validator.telemetry import TEETelemetryClient

            # Create telemetry client and fetch fresh data
            telemetry_client = TEETelemetryClient(miner_address)
            telemetry_result = await telemetry_client.execute_telemetry_sequence(
                routing_table=self.validator.routing_table
            )

            if not telemetry_result:
                return {
                    "hotkey": hotkey,
                    "found": True,
                    "error": "Failed to fetch telemetry from miner",
                    "message": (
                        f"Miner at {miner_address} did not return telemetry data"
                    ),
                    "miner_address": miner_address,
                    "worker_id": worker_id,
                }

            # Process the raw telemetry result into structured format
            return {
                "hotkey": hotkey,
                "found": True,
                "source": "live_from_miner",
                "miner_address": miner_address,
                "worker_id": telemetry_result.get("worker_id", worker_id),
                "timestamp": telemetry_result.get("current_time", "unknown"),
                "timing": {
                    "boot_time": telemetry_result.get("boot_time", 0),
                    "last_operation_time": telemetry_result.get(
                        "last_operation_time", 0
                    ),
                    "current_time": telemetry_result.get("current_time", 0),
                },
                "twitter_metrics": {
                    "auth_errors": telemetry_result.get("twitter_auth_errors", 0),
                    "errors": telemetry_result.get("twitter_errors", 0),
                    "ratelimit_errors": telemetry_result.get(
                        "twitter_ratelimit_errors", 0
                    ),
                    "returned_other": telemetry_result.get("twitter_returned_other", 0),
                    "returned_profiles": telemetry_result.get(
                        "twitter_returned_profiles", 0
                    ),
                    "returned_tweets": telemetry_result.get(
                        "twitter_returned_tweets", 0
                    ),
                    "scrapes": telemetry_result.get("twitter_scrapes", 0),
                },
                "web_metrics": {
                    "errors": telemetry_result.get("web_errors", 0),
                    "success": telemetry_result.get("web_success", 0),
                },
                "tiktok_metrics": {
                    "transcription_success": telemetry_result.get(
                        "tiktok_transcription_success", 0
                    ),
                    "transcription_errors": telemetry_result.get(
                        "tiktok_transcription_errors", 0
                    ),
                },
                "platform_metrics": telemetry_result.get("platform_metrics", {}),
                "raw_telemetry": telemetry_result,  # Include raw data
            }

        except Exception as e:
            logger.error(
                f"Failed to fetch live telemetry for hotkey {hotkey}: {str(e)}"
            )
            return {
                "hotkey": hotkey,
                "found": False,
                "error": str(e),
                "message": "Exception occurred while fetching live telemetry",
                "source": "live_from_miner",
            }

    async def monitor_weights_setting(self):
        """Return weights monitoring statistics for setting"""
        try:
            # Get process monitoring data from the background tasks
            if hasattr(self.validator, "background_tasks") and hasattr(
                self.validator.background_tasks, "process_monitor"
            ):
                monitor = self.validator.background_tasks.process_monitor
                # Get statistics specifically for set_weights process
                weights_stats = monitor.get_process_statistics("set_weights")
                return {
                    "monitoring_status": {
                        "active_executions": len(
                            [
                                exec_id
                                for exec_id, exec_data in monitor.current_executions.items()
                                if exec_data.get("process_name") == "set_weights"
                            ]
                        ),
                        "process_name": "set_weights",
                        "timestamp": datetime.now().isoformat(),
                    },
                    "weights_setting": weights_stats,
                }
            else:
                return {
                    "error": "Process monitoring not available",
                    "monitoring_status": {
                        "active_executions": 0,
                        "process_name": "set_weights",
                        "timestamp": datetime.now().isoformat(),
                    },
                }
        except Exception as e:
            logger.error(f"Failed to get weights monitoring data: {str(e)}")
            return {"error": str(e)}

    async def monitor_priority_miners_publishing(self):
        """Return priority miners monitoring statistics for publishing"""
        try:
            # Get process monitoring data from the background tasks
            if hasattr(self.validator, "background_tasks") and hasattr(
                self.validator.background_tasks, "process_monitor"
            ):
                monitor = self.validator.background_tasks.process_monitor
                # Get statistics specifically for priority miners process
                priority_miners_stats = monitor.get_process_statistics(
                    "send_priority_miners"
                )
                return {
                    "monitoring_status": {
                        "active_executions": len(
                            [
                                exec_id
                                for exec_id, exec_data in monitor.current_executions.items()
                                if exec_data.get("process_name")
                                == "send_priority_miners"
                            ]
                        ),
                        "process_name": "send_priority_miners",
                        "timestamp": datetime.now().isoformat(),
                    },
                    "priority_miners_publishing": priority_miners_stats,
                }
            else:
                return {
                    "error": "Process monitoring not available",
                    "monitoring_status": {
                        "active_executions": 0,
                        "process_name": "send_priority_miners",
                        "timestamp": datetime.now().isoformat(),
                    },
                }
        except Exception as e:
            logger.error(f"Failed to get priority miners monitoring data: {str(e)}")
            return {"error": str(e)}

    async def get_weighted_priority_miners_list(self, list_size: int = 100):
        """
        Return weighted priority miners list where better scoring miners
        appear more frequently in a deterministic way.

        :param list_size: Size of the weighted list to generate (default: 100)
        :return: Deterministic weighted list of miner IP addresses
        """
        try:
            # Get telemetry data
            telemetry = self.validator.telemetry_storage.get_all_telemetry()
            data_to_score = self.validator.weights_manager._get_delta_node_data(
                telemetry
            )

            # Get the scores from calculate_weights
            uids, weights = await self.validator.weights_manager.calculate_weights(
                data_to_score, simulation=False
            )

            if not uids or not weights:
                logger.warning("No scores available for priority miners")
                return {"error": "No scores available"}

            # Create UID to score mapping
            uid_to_score = dict(zip(uids, weights))

            # Get addresses with hotkeys for mapping
            addresses_with_hotkeys = (
                self.validator.routing_table.get_all_addresses_with_hotkeys()
            )

            # Create mapping from address to hotkey
            address_to_hotkey = {
                address: hotkey for hotkey, address, worker_id in addresses_with_hotkeys
            }

            # Create list of (address, score, hotkey) tuples for addresses that have scores
            address_scores = []
            for hotkey, address, worker_id in addresses_with_hotkeys:
                try:
                    # Get UID for this hotkey
                    node_uid = self.validator.metagraph.nodes[hotkey].node_id
                    if node_uid in uid_to_score:
                        score = uid_to_score[node_uid]
                        address_scores.append((address, score, hotkey))
                except KeyError:
                    logger.debug(f"Hotkey {hotkey} not found in metagraph, skipping")
                    continue

            if not address_scores:
                logger.warning("No addresses with valid scores found")
                return {"error": "No addresses with valid scores found"}

            # Sort by score (highest first) for deterministic ordering
            address_scores.sort(key=lambda x: x[1], reverse=True)

            # Create deterministic weighted distribution
            weighted_list = []
            total_score = sum(score for _, score, _ in address_scores)

            if total_score > 0:
                # Calculate how many times each address should appear
                for address, score, hotkey in address_scores:
                    # Calculate frequency based on score proportion
                    frequency = max(1, int((score / total_score) * list_size))
                    # Add this address 'frequency' times to the list
                    weighted_list.extend([address] * frequency)

                # If we have too few items, add top miners until we reach list_size
                while len(weighted_list) < list_size:
                    # Add the best miners in order until we reach desired size
                    for address, _, _ in address_scores:
                        if len(weighted_list) >= list_size:
                            break
                        weighted_list.append(address)

                # If we have too many items, trim to exact size
                weighted_list = weighted_list[:list_size]
            else:
                # Fallback: equal distribution if no positive scores
                for i in range(list_size):
                    idx = i % len(address_scores)
                    weighted_list.append(address_scores[idx][0])

            # Calculate statistics
            unique_addresses = list(set(weighted_list))

            from collections import Counter

            address_counts = Counter(weighted_list)
            top_addresses = [
                {
                    "address": addr,
                    "frequency": count,
                    "score": next(
                        (score for a, score, _ in address_scores if a == addr), 0
                    ),
                    "percentage": round((count / len(weighted_list)) * 100, 1),
                }
                for addr, count in address_counts.most_common(5)
            ]

            return {
                "weighted_list": weighted_list,
                "list_size": len(weighted_list),
                "unique_addresses": len(unique_addresses),
                "top_addresses": top_addresses,
                "total_addresses_available": len(address_scores),
                "address_to_hotkey": address_to_hotkey,
                "note": "List is deterministically weighted by score (top miners appear more frequently)",
            }
        except Exception as e:
            logger.error(f"Failed to get weighted priority miners list: {str(e)}")
            return {"error": str(e)}

    async def monitor_all_telemetry(self):
        """Return all telemetry data from the SQLite database"""
        try:
            telemetry_data = self.validator.telemetry_storage.get_all_telemetry()

            # Convert NodeData objects to dictionaries
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict_list.append(self._nodedata_to_dict(data))

            return {
                "count": len(telemetry_dict_list),
                "telemetry_data": telemetry_dict_list,
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_postgresql_telemetry(self, limit: int = 1000):
        """Return all PostgreSQL telemetry data"""
        try:
            telemetry_data = (
                self.validator.telemetry_storage.get_all_telemetry_postgresql(limit)
            )

            # Convert NodeData objects to dictionaries using dynamic approach
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict_list.append(self._nodedata_to_dict(data))

            return {
                "count": len(telemetry_dict_list),
                "telemetry_data": telemetry_dict_list,
                "source": "postgresql",
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_postgresql_telemetry_stats(self):
        """Return PostgreSQL telemetry statistics"""
        try:
            stats = self.validator.telemetry_storage.get_telemetry_stats_postgresql()
            status = self.validator.telemetry_storage.check_postgresql_status()

            return {
                "postgresql_status": "connected" if status else "disconnected",
                **stats,
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_postgresql_telemetry_by_hotkey(self, hotkey: str):
        """Return PostgreSQL telemetry data for a specific hotkey"""
        try:
            telemetry_data = (
                self.validator.telemetry_storage.get_telemetry_by_hotkey_postgresql(
                    hotkey
                )
            )

            # Convert NodeData objects to dictionaries using dynamic approach
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict_list.append(self._nodedata_to_dict(data))

            return {
                "hotkey": hotkey,
                "count": len(telemetry_dict_list),
                "telemetry_data": telemetry_dict_list,
                "source": "postgresql",
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_platforms(self):
        """Return platform configuration and status information"""
        try:
            from validator.platform_config import PlatformManager
            import time

            manager = PlatformManager()

            # Initialize platform manager

            platforms = {}

            for platform_name in manager.get_platform_names():
                config = manager.get_platform(platform_name)
                platforms[platform_name] = {
                    "name": config.name,
                    "emission_weight": config.emission_weight,
                    "emission_weight_percent": f"{config.emission_weight * 100:.1f}%",
                    "success_metrics": config.success_metrics,
                    "error_metrics": config.error_metrics,
                    "all_metrics": config.metrics,
                }

            return {
                "platforms": platforms,
                "total_platforms": len(platforms),
                "total_emission_weight": sum(
                    p["emission_weight"] for p in platforms.values()
                ),
                "timestamp": int(time.time()),
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_platform_scores(self, hours: int = 24):
        """Return platform-specific scoring data for all miners"""
        try:
            from validator.platform_config import PlatformManager
            from validator.weights import WeightsManager
            import time

            manager = PlatformManager()

            # Use the actual validator's WeightsManager (no mocking needed!)
            weights_manager = WeightsManager(self.validator)

            # Get telemetry data from the last N hours
            current_time = int(time.time())
            cutoff_time = current_time - (hours * 3600)

            all_telemetry = self.validator.telemetry_storage.get_all_telemetry()
            recent_telemetry = [
                data
                for data in all_telemetry
                if self._convert_timestamp_to_int(data.timestamp) >= cutoff_time
            ]

            # Calculate delta data using the real validator's WeightsManager
            delta_telemetry = weights_manager._get_delta_node_data(recent_telemetry)

            platform_scores = {}
            node_scores = {}

            for platform_name in manager.get_platform_names():
                platform_scores[platform_name] = {
                    "total_score": 0.0,
                    "nodes_with_data": 0,
                    "total_nodes": 0,
                    "average_score": 0.0,
                }

            # Calculate scores for each node using DELTA data
            for node_data in delta_telemetry:
                node_key = node_data.hotkey
                if node_key not in node_scores:
                    node_scores[node_key] = {}

                # Update platform metrics for scoring
                self._update_node_platform_metrics(node_data)

                for platform_name in manager.get_platform_names():
                    score = weights_manager.calculate_platform_score(
                        node_data, platform_name
                    )
                    node_scores[node_key][platform_name] = score

                    platform_scores[platform_name]["total_score"] += score
                    platform_scores[platform_name]["total_nodes"] += 1
                    if score > 0:
                        platform_scores[platform_name]["nodes_with_data"] += 1

            # Calculate averages
            for platform_name in platform_scores:
                total_nodes = platform_scores[platform_name]["total_nodes"]
                if total_nodes > 0:
                    avg_score = (
                        platform_scores[platform_name]["total_score"] / total_nodes
                    )
                    platform_scores[platform_name]["average_score"] = round(
                        avg_score, 4
                    )

            return {
                "platform_scores": platform_scores,
                "node_scores": {
                    k: v for k, v in list(node_scores.items())[:10]
                },  # Top 10 for brevity
                "time_period_hours": hours,
                "nodes_analyzed": len(node_scores),
                "timestamp": current_time,
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_emission_distribution(self, hours: int = 24):
        """Return emission distribution across platforms"""
        try:
            from validator.platform_config import PlatformManager
            from validator.weights import WeightsManager
            import time

            manager = PlatformManager()

            # Use the actual validator's WeightsManager (no mocking needed!)
            weights_manager = WeightsManager(self.validator)

            # Get recent telemetry data
            current_time = int(time.time())
            cutoff_time = current_time - (hours * 3600)

            all_telemetry = self.validator.telemetry_storage.get_all_telemetry()
            recent_telemetry = [
                data
                for data in all_telemetry
                if self._convert_timestamp_to_int(data.timestamp) >= cutoff_time
            ]

            # Calculate delta data using the real validator's WeightsManager
            delta_telemetry = weights_manager._get_delta_node_data(recent_telemetry)

            platform_totals = {}
            weighted_totals = {}

            # Calculate platform totals using DELTA data
            for platform_name in manager.get_platform_names():
                platform_config = manager.get_platform(platform_name)
                platform_total = 0.0

                for node_data in delta_telemetry:
                    self._update_node_platform_metrics(node_data)
                    score = weights_manager.calculate_platform_score(
                        node_data, platform_name
                    )
                    platform_total += score

                platform_totals[platform_name] = platform_total
                weighted_totals[platform_name] = (
                    platform_total * platform_config.emission_weight
                )

            total_weighted_score = sum(weighted_totals.values())

            # Calculate emission distribution
            emission_distribution = {}
            for platform_name in manager.get_platform_names():
                platform_config = manager.get_platform(platform_name)
                weighted_score = weighted_totals[platform_name]
                percentage = (
                    (weighted_score / total_weighted_score * 100)
                    if total_weighted_score > 0
                    else 0
                )

                emission_distribution[platform_name] = {
                    "unweighted_score": round(platform_totals[platform_name], 4),
                    "emission_weight": platform_config.emission_weight,
                    "emission_weight_percent": f"{platform_config.emission_weight * 100:.1f}%",
                    "weighted_score": round(weighted_score, 4),
                    "actual_emission_percentage": round(percentage, 2),
                    "nodes_contributing": sum(
                        1
                        for data in delta_telemetry
                        if self._get_platform_contribution(
                            data, platform_name, weights_manager
                        )
                        > 0
                    ),
                }

            return {
                "emission_distribution": emission_distribution,
                "total_weighted_score": round(total_weighted_score, 4),
                "time_period_hours": hours,
                "nodes_analyzed": len(delta_telemetry),
                "timestamp": current_time,
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_weights_distribution(self, hours: int = 24):
        """Return current weights distribution across platforms and miners"""
        try:
            from validator.platform_config import PlatformManager
            from validator.weights import WeightsManager
            import time

            manager = PlatformManager()

            # Use the actual validator's WeightsManager (no mocking needed!)
            weights_manager = WeightsManager(self.validator)

            # Get recent telemetry data
            current_time = int(time.time())
            cutoff_time = current_time - (hours * 3600)

            all_telemetry = self.validator.telemetry_storage.get_all_telemetry()
            recent_telemetry = [
                data
                for data in all_telemetry
                if self._convert_timestamp_to_int(data.timestamp) >= cutoff_time
            ]

            # Calculate actual weights using the current system
            delta_data = weights_manager._get_delta_node_data(recent_telemetry)
            uids, weights = await weights_manager.calculate_weights(
                delta_data, simulation=True
            )

            # Create UID to weight mapping
            uid_to_weight = dict(zip(uids, weights))

            # Calculate platform breakdown
            platform_breakdown = {}
            miner_details = []

            for platform_name in manager.get_platform_names():
                platform_config = manager.get_platform(platform_name)
                platform_breakdown[platform_name] = {
                    "emission_weight": platform_config.emission_weight,
                    "total_contributed_weight": 0.0,
                    "miners_contributing": 0,
                    "average_contribution": 0.0,
                }

            for node in delta_data:
                self._update_node_platform_metrics(node)
                node_weight = uid_to_weight.get(node.uid, 0.0)

                platform_contributions = {}
                total_platform_score = 0.0

                for platform_name in manager.get_platform_names():
                    platform_score = weights_manager.calculate_platform_score(
                        node, platform_name
                    )
                    platform_config = manager.get_platform(platform_name)
                    weighted_contribution = (
                        platform_score * platform_config.emission_weight
                    )

                    platform_contributions[platform_name] = {
                        "platform_score": round(platform_score, 4),
                        "weighted_contribution": round(weighted_contribution, 4),
                    }

                    total_platform_score += weighted_contribution

                    if platform_score > 0:
                        platform_breakdown[platform_name][
                            "total_contributed_weight"
                        ] += node_weight
                        platform_breakdown[platform_name]["miners_contributing"] += 1

                miner_details.append(
                    {
                        "hotkey": node.hotkey[:16] + "...",
                        "uid": node.uid,
                        "final_weight": round(node_weight, 4),
                        "total_platform_score": round(total_platform_score, 4),
                        "platform_contributions": platform_contributions,
                    }
                )

            # Calculate averages
            for platform_name in platform_breakdown:
                contributing_miners = platform_breakdown[platform_name][
                    "miners_contributing"
                ]
                if contributing_miners > 0:
                    avg_contribution = (
                        platform_breakdown[platform_name]["total_contributed_weight"]
                        / contributing_miners
                    )
                    platform_breakdown[platform_name]["average_contribution"] = round(
                        avg_contribution, 4
                    )

            # Sort miners by weight (descending)
            miner_details.sort(key=lambda x: x["final_weight"], reverse=True)

            return {
                "weights_distribution": {
                    "platform_breakdown": platform_breakdown,
                    "top_miners": miner_details[:20],  # Top 20 miners
                    "total_miners": len(miner_details),
                    "total_weight": round(sum(weights), 4),
                    "average_weight": (
                        round(sum(weights) / len(weights), 4) if weights else 0.0
                    ),
                },
                "calculation_metadata": {
                    "time_period_hours": hours,
                    "nodes_analyzed": len(recent_telemetry),
                    "delta_nodes_calculated": len(delta_data),
                    "timestamp": current_time,
                },
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_platform_performance(self, hours: int = 24):
        """Return comprehensive platform performance analysis"""
        try:
            from validator.platform_config import PlatformManager
            from validator.weights import WeightsManager
            import time

            manager = PlatformManager()

            # Use the actual validator's WeightsManager (no mocking needed!)
            weights_manager = WeightsManager(self.validator)

            # Get recent telemetry data
            current_time = int(time.time())
            cutoff_time = current_time - (hours * 3600)

            all_telemetry = self.validator.telemetry_storage.get_all_telemetry()
            recent_telemetry = [
                data
                for data in all_telemetry
                if self._convert_timestamp_to_int(data.timestamp) >= cutoff_time
            ]

            # Calculate delta data using the real validator's WeightsManager
            delta_telemetry = weights_manager._get_delta_node_data(recent_telemetry)

            performance_analysis = {}

            for platform_name in manager.get_platform_names():
                platform_config = manager.get_platform(platform_name)

                performance_analysis[platform_name] = {
                    "platform_config": {
                        "emission_weight": platform_config.emission_weight,
                        "success_metrics": platform_config.success_metrics,
                        "error_metrics": platform_config.error_metrics,
                    },
                    "performance_metrics": {
                        "total_nodes": len(delta_telemetry),
                        "active_nodes": 0,
                        "total_success_operations": 0,
                        "total_errors": 0,
                        "success_rate": 0.0,
                    },
                    "top_performers": [],
                    "health_status": "unknown",
                }

                node_performances = []

                # Use DELTA data for accurate performance analysis
                for node_data in delta_telemetry:
                    self._update_node_platform_metrics(node_data)

                    success_count = 0
                    error_count = 0

                    # Get platform-specific metrics
                    if (
                        hasattr(node_data, "platform_metrics")
                        and node_data.platform_metrics
                    ):
                        platform_metrics = node_data.platform_metrics.get(
                            platform_name, {}
                        )

                        for success_metric in platform_config.success_metrics:
                            success_count += platform_metrics.get(success_metric, 0)

                        for error_metric in platform_config.error_metrics:
                            error_count += platform_metrics.get(error_metric, 0)

                    if success_count > 0 or error_count > 0:
                        performance_analysis[platform_name]["performance_metrics"][
                            "active_nodes"
                        ] += 1

                    performance_analysis[platform_name]["performance_metrics"][
                        "total_success_operations"
                    ] += success_count
                    performance_analysis[platform_name]["performance_metrics"][
                        "total_errors"
                    ] += error_count

                    # Calculate error rate per hour for reference only (no punishment)
                    time_span = getattr(node_data, "time_span_seconds", 3600) / 3600
                    error_rate = (
                        error_count / time_span if time_span > 0 else error_count
                    )

                    if success_count > 0:
                        node_performances.append(
                            {
                                "hotkey": node_data.hotkey[:16] + "...",
                                "success_operations": success_count,
                                "errors": error_count,
                                "error_rate_per_hour": round(error_rate, 2),
                            }
                        )

                # Calculate success rate
                total_operations = (
                    performance_analysis[platform_name]["performance_metrics"][
                        "total_success_operations"
                    ]
                    + performance_analysis[platform_name]["performance_metrics"][
                        "total_errors"
                    ]
                )

                if total_operations > 0:
                    success_rate = (
                        performance_analysis[platform_name]["performance_metrics"][
                            "total_success_operations"
                        ]
                        / total_operations
                        * 100
                    )
                    performance_analysis[platform_name]["performance_metrics"][
                        "success_rate"
                    ] = round(success_rate, 2)

                # Sort and get top performers
                node_performances.sort(
                    key=lambda x: x["success_operations"], reverse=True
                )
                performance_analysis[platform_name]["top_performers"] = (
                    node_performances[:10]
                )

                # Determine health status
                active_nodes = performance_analysis[platform_name][
                    "performance_metrics"
                ]["active_nodes"]
                if active_nodes == 0:
                    health_status = "inactive"
                elif (
                    performance_analysis[platform_name]["performance_metrics"][
                        "success_rate"
                    ]
                    > 90
                ):
                    health_status = "excellent"
                elif (
                    performance_analysis[platform_name]["performance_metrics"][
                        "success_rate"
                    ]
                    > 75
                ):
                    health_status = "good"
                elif (
                    performance_analysis[platform_name]["performance_metrics"][
                        "success_rate"
                    ]
                    > 50
                ):
                    health_status = "fair"
                else:
                    health_status = "poor"

                performance_analysis[platform_name]["health_status"] = health_status

            return {
                "platform_performance": performance_analysis,
                "time_period_hours": hours,
                "total_nodes_analyzed": len(delta_telemetry),
                "timestamp": current_time,
                "summary": {
                    "total_platforms": len(performance_analysis),
                    "active_platforms": sum(
                        1
                        for p in performance_analysis.values()
                        if p["performance_metrics"]["active_nodes"] > 0
                    ),
                    "healthy_platforms": sum(
                        1
                        for p in performance_analysis.values()
                        if p["health_status"] in ["excellent", "good"]
                    ),
                },
            }
        except Exception as e:
            return {"error": str(e)}

    async def monitor_platform_analytics(self, hours: int = 24):
        """Return comprehensive platform analytics and trends"""
        try:
            from validator.platform_config import PlatformManager
            from validator.weights import WeightsManager
            import time
            from collections import defaultdict

            manager = PlatformManager()

            # Use the actual validator's WeightsManager (no mocking needed!)
            weights_manager = WeightsManager(self.validator)

            # Get recent telemetry data
            current_time = int(time.time())
            cutoff_time = current_time - (hours * 3600)

            all_telemetry = self.validator.telemetry_storage.get_all_telemetry()
            recent_telemetry = [
                data
                for data in all_telemetry
                if self._convert_timestamp_to_int(data.timestamp) >= cutoff_time
            ]

            # Calculate delta data using the real validator's WeightsManager
            delta_telemetry = weights_manager._get_delta_node_data(recent_telemetry)

            analytics = {}

            for platform_name in manager.get_platform_names():
                platform_config = manager.get_platform(platform_name)

                # Initialize analytics structure
                analytics[platform_name] = {
                    "overview": {
                        "total_miners": 0,
                        "active_miners": 0,
                        "total_operations": 0,
                        "total_errors": 0,
                        "success_rate": 0.0,
                        "average_operations_per_miner": 0.0,
                    },
                    "metrics_breakdown": {},
                    "top_miners": [],
                    "error_analysis": {
                        "total_errors": 0,
                        "error_types": {},
                        "high_error_miners": [],
                    },
                    "trends": {
                        "hourly_activity": [],
                        "growing_miners": [],
                        "declining_miners": [],
                    },
                }

                # Initialize metrics breakdown
                for metric in platform_config.metrics:
                    analytics[platform_name]["metrics_breakdown"][metric] = {
                        "total": 0,
                        "average_per_miner": 0.0,
                        "top_contributors": [],
                    }

                # Analyze miners using DELTA data (not raw telemetry)
                miner_data = defaultdict(
                    lambda: {
                        "success_operations": 0,
                        "total_errors": 0,
                        "metrics": defaultdict(int),
                        "timestamps": [],
                    }
                )

                for node_data in delta_telemetry:  # ← Using delta data now!
                    self._update_node_platform_metrics(node_data)

                    if (
                        not hasattr(node_data, "platform_metrics")
                        or not node_data.platform_metrics
                    ):
                        continue

                    platform_metrics = node_data.platform_metrics.get(platform_name, {})
                    if not platform_metrics:
                        continue

                    hotkey = node_data.hotkey
                    miner_data[hotkey]["timestamps"].append(node_data.timestamp)

                    # Aggregate DELTA metrics (not cumulative raw values)
                    for metric in platform_config.success_metrics:
                        value = platform_metrics.get(metric, 0)
                        miner_data[hotkey]["success_operations"] += value
                        miner_data[hotkey]["metrics"][metric] += value
                        analytics[platform_name]["metrics_breakdown"][metric][
                            "total"
                        ] += value

                    for metric in platform_config.error_metrics:
                        value = platform_metrics.get(metric, 0)
                        miner_data[hotkey]["total_errors"] += value
                        miner_data[hotkey]["metrics"][metric] += value
                        analytics[platform_name]["metrics_breakdown"][metric][
                            "total"
                        ] += value
                        analytics[platform_name]["error_analysis"]["error_types"][
                            metric
                        ] = (
                            analytics[platform_name]["error_analysis"][
                                "error_types"
                            ].get(metric, 0)
                            + value
                        )

                # Calculate overview metrics
                active_miners = len(
                    [m for m in miner_data.values() if m["success_operations"] > 0]
                )
                total_miners = len(miner_data)
                total_operations = sum(
                    m["success_operations"] for m in miner_data.values()
                )
                total_errors = sum(m["total_errors"] for m in miner_data.values())

                analytics[platform_name]["overview"].update(
                    {
                        "total_miners": total_miners,
                        "active_miners": active_miners,
                        "total_operations": total_operations,
                        "total_errors": total_errors,
                        "success_rate": round(
                            (
                                (
                                    total_operations
                                    / (total_operations + total_errors)
                                    * 100
                                )
                                if (total_operations + total_errors) > 0
                                else 0
                            ),
                            2,
                        ),
                        "average_operations_per_miner": round(
                            (
                                total_operations / active_miners
                                if active_miners > 0
                                else 0
                            ),
                            2,
                        ),
                    }
                )

                # Calculate metric averages
                for metric in analytics[platform_name]["metrics_breakdown"]:
                    total_value = analytics[platform_name]["metrics_breakdown"][metric][
                        "total"
                    ]
                    analytics[platform_name]["metrics_breakdown"][metric][
                        "average_per_miner"
                    ] = round(
                        total_value / active_miners if active_miners > 0 else 0, 2
                    )

                # Get top miners
                top_miners = sorted(
                    [
                        (
                            hotkey[:16] + "...",
                            data["success_operations"],
                            data["total_errors"],
                        )
                        for hotkey, data in miner_data.items()
                    ],
                    key=lambda x: x[1],
                    reverse=True,
                )[:10]

                analytics[platform_name]["top_miners"] = [
                    {"hotkey": hotkey, "operations": ops, "errors": errs}
                    for hotkey, ops, errs in top_miners
                ]

                # High error miners
                high_error_miners = sorted(
                    [
                        (hotkey[:16] + "...", data["total_errors"])
                        for hotkey, data in miner_data.items()
                        if data["total_errors"] > 10
                    ],
                    key=lambda x: x[1],
                    reverse=True,
                )[:5]

                analytics[platform_name]["error_analysis"]["high_error_miners"] = [
                    {"hotkey": hotkey, "errors": errs}
                    for hotkey, errs in high_error_miners
                ]
                analytics[platform_name]["error_analysis"][
                    "total_errors"
                ] = total_errors

            return {
                "platform_analytics": analytics,
                "analysis_period": {
                    "hours": hours,
                    "start_time": cutoff_time,
                    "end_time": current_time,
                    "total_data_points": len(recent_telemetry),
                },
                "timestamp": current_time,
            }
        except Exception as e:
            return {"error": str(e)}

    def _update_node_platform_metrics(self, node_data):
        """Helper method to update platform metrics for a single node"""
        if not hasattr(node_data, "platform_metrics") or not node_data.platform_metrics:
            node_data.platform_metrics = {}

        # Update Twitter platform metrics from legacy fields
        if (
            getattr(node_data, "twitter_returned_tweets", 0) > 0
            or getattr(node_data, "twitter_returned_profiles", 0) > 0
            or getattr(node_data, "twitter_auth_errors", 0) > 0
            or getattr(node_data, "twitter_errors", 0) > 0
            or getattr(node_data, "twitter_ratelimit_errors", 0) > 0
        ):

            node_data.platform_metrics["twitter"] = {
                "returned_tweets": getattr(node_data, "twitter_returned_tweets", 0),
                "returned_profiles": getattr(node_data, "twitter_returned_profiles", 0),
                "scrapes": getattr(node_data, "twitter_scrapes", 0),
                "auth_errors": getattr(node_data, "twitter_auth_errors", 0),
                "errors": getattr(node_data, "twitter_errors", 0),
                "ratelimit_errors": getattr(node_data, "twitter_ratelimit_errors", 0),
            }

        # Update TikTok platform metrics from new fields
        tiktok_success = getattr(node_data, "tiktok_transcription_success", 0)
        tiktok_errors = getattr(node_data, "tiktok_transcription_errors", 0)

        if tiktok_success > 0 or tiktok_errors > 0:
            node_data.platform_metrics["tiktok"] = {
                "transcription_success": tiktok_success,
                "transcription_errors": tiktok_errors,
            }

    def _get_platform_contribution(self, node_data, platform_name, weights_manager):
        """Helper method to get platform contribution for a node"""
        self._update_node_platform_metrics(node_data)
        return weights_manager.calculate_platform_score(node_data, platform_name)

    def _convert_timestamp_to_int(self, timestamp) -> int:
        """
        Convert timestamp to integer for comparison.

        :param timestamp: Timestamp value (string, int, or other)
        :return: Integer timestamp (unix seconds) or 0 if conversion fails
        """
        if isinstance(timestamp, int):
            return timestamp
        elif isinstance(timestamp, str):
            if timestamp == "" or timestamp is None:
                return 0
            try:
                # Try to parse as datetime string first
                from datetime import datetime

                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                return int(dt.timestamp())
            except ValueError:
                try:
                    # Try to parse as integer string
                    return int(timestamp)
                except ValueError:
                    return 0
        else:
            return 0
