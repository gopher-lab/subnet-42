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
            twitter_auth_errors=0,
            twitter_errors=0,
            twitter_ratelimit_errors=0,
            twitter_returned_other=0,
            twitter_returned_profiles=0,
            twitter_returned_tweets=0,
            twitter_scrapes=0,
            web_errors=0,
            web_success=10,
            timestamp=0,
        ),
        NodeData(
            hotkey="node2",
            worker_id="worker2",
            uid=2,
            boot_time=0,
            last_operation_time=0,
            current_time=0,
            twitter_auth_errors=0,
            twitter_errors=0,
            twitter_ratelimit_errors=0,
            twitter_returned_other=0,
            twitter_returned_profiles=0,
            twitter_returned_tweets=20,
            twitter_scrapes=0,
            web_errors=0,
            web_success=20,
            timestamp=0,
        ),
    ]
    uids, weights = weights_manager.calculate_weights(node_data)
    assert len(uids) == len(weights) == 2
    assert weights[0] < weights[1]  # Assuming node2 has more activity


@pytest.mark.asyncio
async def test_set_weights(weights_manager, mock_validator):
    # Mock the async method and dependencies
    mock_validator.substrate.query = MagicMock(return_value=MagicMock(value=1))
    mock_validator.metagraph.nodes = {
        "node1": MagicMock(node_id=1),
        "node2": MagicMock(node_id=2),
    }
    with patch(
        "validator.weights.weights.set_node_weights", return_value=True
    ) as mock_set_node_weights:
        await weights_manager.set_weights([])
        mock_set_node_weights.assert_called_once()


class TestRestartDetection:
    """Tests for worker restart detection in _get_delta_node_data."""

    def test_no_restart_monotonically_increasing(self, weights_manager, mock_validator):
        """No restart when counters are monotonically increasing and boot_time stays same."""
        mock_validator.metagraph.nodes = {
            "node1": MagicMock(node_id=1, hotkey="node1"),
        }

        telemetry_data = [
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=1000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=10,
                twitter_scrapes=0,
                web_errors=0,
                web_success=5,
                timestamp=100,
            ),
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=1000,  # Same boot_time = no restart
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=20,  # Increased
                twitter_scrapes=0,
                web_errors=0,
                web_success=15,  # Increased
                timestamp=200,
            ),
        ]

        result = weights_manager._get_delta_node_data(telemetry_data)
        assert len(result) == 1
        # Delta should be 20-10=10 tweets, 15-5=10 web_success
        assert result[0].twitter_returned_tweets == 10
        assert result[0].web_success == 10

    def test_restart_detected_boot_time_change(self, weights_manager, mock_validator):
        """Restart detected when boot_time changes between records."""
        mock_validator.metagraph.nodes = {
            "node1": MagicMock(node_id=1, hotkey="node1"),
        }

        telemetry_data = [
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=1000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=100,
                twitter_scrapes=0,
                web_errors=0,
                web_success=50,
                timestamp=100,
            ),
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=2000,  # Different boot_time = restart
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=5,  # Reset to lower value after restart
                twitter_scrapes=0,
                web_errors=0,
                web_success=10,
                timestamp=200,
            ),
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=2000,  # Same boot_time as previous
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=25,  # Increased from 5
                twitter_scrapes=0,
                web_errors=0,
                web_success=30,  # Increased from 10
                timestamp=300,
            ),
        ]

        result = weights_manager._get_delta_node_data(telemetry_data)
        assert len(result) == 1
        # Should handle restart: first chunk is single record (no delta)
        # Second chunk: 25-5=20 tweets, 30-10=20 web_success
        assert result[0].twitter_returned_tweets == 20
        assert result[0].web_success == 20

    def test_multiple_restarts(self, weights_manager, mock_validator):
        """Multiple restarts create multiple chunks that sum correctly."""
        mock_validator.metagraph.nodes = {
            "node1": MagicMock(node_id=1, hotkey="node1"),
        }

        telemetry_data = [
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=1000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=0,
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,
                timestamp=100,
            ),
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=1000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=10,
                twitter_scrapes=0,
                web_errors=0,
                web_success=10,
                timestamp=200,
            ),
            # First restart
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=2000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=0,
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,
                timestamp=300,
            ),
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=2000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=15,
                twitter_scrapes=0,
                web_errors=0,
                web_success=15,
                timestamp=400,
            ),
            # Second restart
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=3000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=0,
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,
                timestamp=500,
            ),
            NodeData(
                hotkey="node1",
                worker_id="worker1",
                uid=1,
                boot_time=3000,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=5,
                twitter_scrapes=0,
                web_errors=0,
                web_success=5,
                timestamp=600,
            ),
        ]

        result = weights_manager._get_delta_node_data(telemetry_data)
        assert len(result) == 1
        # Chunk 1: 10-0=10, Chunk 2: 15-0=15, Chunk 3: 5-0=5 = Total 30
        assert result[0].twitter_returned_tweets == 30
        assert result[0].web_success == 30
