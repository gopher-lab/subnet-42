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


@pytest.mark.asyncio
async def test_calculate_weights(weights_manager, mock_validator):
    # Test calculate_weights method
    mock_validator.node_manager.send_score_report = AsyncMock()
    
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
                "twitter_returned_tweets": 0,
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
                "twitter_returned_tweets": 20,
                "web_processed_pages": 20,
            },
        ),
    ]
    uids, weights = await weights_manager.calculate_weights(node_data, simulation=True)
    assert len(uids) == len(weights) == 2
    assert weights[0] < weights[1]  # Assuming node2 has more activity


@pytest.mark.asyncio
async def test_set_weights(weights_manager, mock_validator):
    # Mock the required validator attributes
    mock_validator.substrate.url = "ws://localhost:9944"
    mock_validator.netuid = 42
    mock_validator.keypair.ss58_address = "validator1"
    mock_validator.metagraph.nodes = {
        "validator1": MagicMock(node_id=0, hotkey="validator1"),
        "node1": MagicMock(node_id=1, hotkey="node1"),
        "node2": MagicMock(node_id=2, hotkey="node2"),
    }

    # Mock telemetry storage to return empty data (simpler test path)
    mock_validator.telemetry_storage = MagicMock()
    mock_validator.telemetry_storage.get_all_telemetry = MagicMock(return_value={})
    
    # Mock async methods
    mock_validator.scorer = MagicMock()
    mock_validator.scorer.fetch_active_worker_version = AsyncMock()
    mock_validator.node_manager = MagicMock()
    mock_validator.node_manager.send_score_report = AsyncMock()

    with patch("validator.weights.interface.get_substrate") as mock_get_substrate, \
         patch("validator.weights.weights.blocks_since_last_update", return_value=1000), \
         patch("validator.weights.weights.min_interval_to_set_weights", return_value=100), \
         patch("validator.weights.weights.set_node_weights", return_value=True) as mock_set_node_weights:
        mock_get_substrate.return_value = mock_validator.substrate
        await weights_manager.set_weights()
        mock_set_node_weights.assert_called_once()


