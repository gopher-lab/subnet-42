import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from validator.weights import WeightsManager
from interfaces.types import NodeData


@pytest.fixture
def mock_validator():
    # Create a mock Validator instance
    mock_validator = MagicMock()
    mock_validator.metagraph = MagicMock()
    mock_validator.substrate = MagicMock()
    mock_validator.keypair = MagicMock()
    return mock_validator


@pytest.fixture
def weights_manager(mock_validator):
    # Create a WeightsManager instance with the mock validator
    return WeightsManager(validator=mock_validator)


def test_calculate_weights(weights_manager):
    # Test calculate_weights method
    node_data = [
        NodeData(
            hotkey="node1",
            worker_id="worker1",
            uid=1,
            boot_time=0,
            last_operation_time=0,
            current_time=0,
            timestamp=0,
            stats_json={
                "twitter_auth_errors": 0,
                "twitter_errors": 0,
                "twitter_ratelimit_errors": 0,
                "twitter_returned_other": 0,
                "twitter_returned_profiles": 0,
                "twitter_returned_tweets": 0,
                "twitter_scrapes": 0,
                "web_errors": 0,
                "web_processed_pages": 10,
            },
        ),
        NodeData(
            hotkey="node2",
            worker_id="worker2",
            uid=2,
            boot_time=0,
            last_operation_time=0,
            current_time=0,
            timestamp=0,
            stats_json={
                "twitter_auth_errors": 0,
                "twitter_errors": 0,
                "twitter_ratelimit_errors": 0,
                "twitter_returned_other": 0,
                "twitter_returned_profiles": 0,
                "twitter_returned_tweets": 20,
                "twitter_scrapes": 0,
                "web_errors": 0,
                "web_processed_pages": 20,
            },
        ),
    ]
    uids, weights = weights_manager.calculate_weights(node_data)
    assert len(uids) == len(weights) == 2
    assert weights[0] < weights[1]  # Assuming node2 has more activity


@pytest.mark.asyncio
async def test_set_weights(weights_manager, mock_validator):
    # Mock the required validator attributes
    mock_validator.substrate.url = "ws://localhost:9944"
    mock_validator.netuid = 42
    mock_validator.keypair.ss58_address = "validator1"
    mock_validator.metagraph.nodes = {
        "validator1": MagicMock(node_id=0),
        "node1": MagicMock(node_id=1, hotkey="node1"),
        "node2": MagicMock(node_id=2, hotkey="node2"),
    }

    # Mock telemetry storage to return empty data (simpler test path)
    mock_validator.telemetry_storage = MagicMock()
    mock_validator.telemetry_storage.get_all_telemetry = MagicMock(return_value={})

    with patch("validator.weights.interface.get_substrate") as mock_get_substrate, \
         patch("validator.weights.weights.blocks_since_last_update", return_value=1000), \
         patch("validator.weights.weights.min_interval_to_set_weights", return_value=100), \
         patch("validator.weights.weights.set_node_weights", return_value=True) as mock_set_node_weights:
        mock_get_substrate.return_value = mock_validator.substrate
        await weights_manager.set_weights()
        mock_set_node_weights.assert_called_once()


class TestRestartDetection:
    """Tests for worker restart detection in _get_delta_node_data."""

    def _make_node_data(self, hotkey, worker_id, uid, boot_time, timestamp, stats):
        """Helper to create NodeData with stats_json."""
        return NodeData(
            hotkey=hotkey,
            worker_id=worker_id,
            uid=uid,
            boot_time=boot_time,
            last_operation_time=0,
            current_time=0,
            timestamp=timestamp,
            stats_json=stats,
        )

    def test_no_restart_monotonically_increasing(self, weights_manager, mock_validator):
        """No restart when counters are monotonically increasing and boot_time stays same."""
        mock_validator.metagraph.nodes = {
            "node1": MagicMock(node_id=1, hotkey="node1"),
        }

        telemetry_data = [
            self._make_node_data(
                "node1", "worker1", 1, boot_time=1000, timestamp=100,
                stats={"twitter_returned_tweets": 10, "web_processed_pages": 5}
            ),
            self._make_node_data(
                "node1", "worker1", 1, boot_time=1000, timestamp=200,
                stats={"twitter_returned_tweets": 20, "web_processed_pages": 15}
            ),
        ]

        result = weights_manager._get_delta_node_data(telemetry_data)
        assert len(result) == 1
        # Delta should be 20-10=10 tweets, 15-5=10 web_processed_pages
        assert result[0].get_stat_value("twitter_returned_tweets") == 10
        assert result[0].get_stat_value("web_processed_pages") == 10

    def test_restart_detected_boot_time_change(self, weights_manager, mock_validator):
        """Restart detected when boot_time changes between records."""
        mock_validator.metagraph.nodes = {
            "node1": MagicMock(node_id=1, hotkey="node1"),
        }

        telemetry_data = [
            self._make_node_data(
                "node1", "worker1", 1, boot_time=1000, timestamp=100,
                stats={"twitter_returned_tweets": 100, "web_processed_pages": 50}
            ),
            self._make_node_data(
                "node1", "worker1", 1, boot_time=2000, timestamp=200,  # Restart
                stats={"twitter_returned_tweets": 5, "web_processed_pages": 10}
            ),
            self._make_node_data(
                "node1", "worker1", 1, boot_time=2000, timestamp=300,
                stats={"twitter_returned_tweets": 25, "web_processed_pages": 30}
            ),
        ]

        result = weights_manager._get_delta_node_data(telemetry_data)
        assert len(result) == 1
        # First chunk is single record (no delta), second chunk: 25-5=20 tweets, 30-10=20 pages
        assert result[0].get_stat_value("twitter_returned_tweets") == 20
        assert result[0].get_stat_value("web_processed_pages") == 20

    def test_multiple_restarts(self, weights_manager, mock_validator):
        """Multiple restarts create multiple chunks that sum correctly."""
        mock_validator.metagraph.nodes = {
            "node1": MagicMock(node_id=1, hotkey="node1"),
        }

        telemetry_data = [
            # Chunk 1: boot_time=1000
            self._make_node_data(
                "node1", "worker1", 1, boot_time=1000, timestamp=100,
                stats={"twitter_returned_tweets": 0, "web_processed_pages": 0}
            ),
            self._make_node_data(
                "node1", "worker1", 1, boot_time=1000, timestamp=200,
                stats={"twitter_returned_tweets": 10, "web_processed_pages": 10}
            ),
            # Chunk 2: boot_time=2000 (first restart)
            self._make_node_data(
                "node1", "worker1", 1, boot_time=2000, timestamp=300,
                stats={"twitter_returned_tweets": 0, "web_processed_pages": 0}
            ),
            self._make_node_data(
                "node1", "worker1", 1, boot_time=2000, timestamp=400,
                stats={"twitter_returned_tweets": 15, "web_processed_pages": 15}
            ),
            # Chunk 3: boot_time=3000 (second restart)
            self._make_node_data(
                "node1", "worker1", 1, boot_time=3000, timestamp=500,
                stats={"twitter_returned_tweets": 0, "web_processed_pages": 0}
            ),
            self._make_node_data(
                "node1", "worker1", 1, boot_time=3000, timestamp=600,
                stats={"twitter_returned_tweets": 5, "web_processed_pages": 5}
            ),
        ]

        result = weights_manager._get_delta_node_data(telemetry_data)
        assert len(result) == 1
        # Chunk 1: 10-0=10, Chunk 2: 15-0=15, Chunk 3: 5-0=5 = Total 30
        assert result[0].get_stat_value("twitter_returned_tweets") == 30
        assert result[0].get_stat_value("web_processed_pages") == 30
