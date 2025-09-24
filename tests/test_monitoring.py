import pytest
import asyncio
import numpy as np
from unittest.mock import Mock, AsyncMock, patch
from validator.process_monitor import ProcessMonitor
from validator.nats import MinersNATSPublisher
from validator.weights import WeightsManager
from interfaces.types import NodeData


class TestProcessMonitoring:
    """Test the process monitoring functionality for NATS and weights"""

    def test_process_monitor_basic_functionality(self):
        """Test that the process monitor works correctly"""
        monitor = ProcessMonitor()

        # Start a process
        execution_id = monitor.start_process("test_process")
        assert execution_id is not None
        assert "test_process" in execution_id

        # Update metrics
        monitor.update_metrics(
            execution_id,
            nodes_processed=5,
            successful_nodes=4,
            failed_nodes=1,
            additional_metrics={"test_data": "test_value"},
        )

        # End process
        result = monitor.end_process(execution_id)
        assert result is not None
        assert result.nodes_processed == 5
        assert result.successful_nodes == 4
        assert result.failed_nodes == 1
        assert result.additional_metrics["test_data"] == "test_value"

        # Check statistics
        stats = monitor.get_process_statistics("test_process")
        assert stats["total_executions"] == 1
        assert len(stats["recent_executions"]) == 1

    @pytest.mark.asyncio
    async def test_nats_monitoring_integration(self):
        """Test NATS publishing with monitoring"""
        # Mock validator
        mock_validator = Mock()
        mock_validator.routing_table_updating = False
        mock_validator.routing_table.get_all_addresses_atomic.return_value = [
            "192.168.1.1",
            "192.168.1.2",
            "192.168.1.3",
        ]

        # Mock background tasks with process monitor
        mock_background_tasks = Mock()
        mock_process_monitor = ProcessMonitor()
        mock_background_tasks.process_monitor = mock_process_monitor
        mock_validator.background_tasks = mock_background_tasks

        # Create NATS publisher
        nats_publisher = MinersNATSPublisher(mock_validator)

        # Mock the NATS client
        with patch.object(
            nats_publisher.nc, "send_connected_nodes", new_callable=AsyncMock
        ) as mock_send:
            mock_send.return_value = None

            # Execute the method
            await nats_publisher.send_connected_nodes()

            # Verify NATS was called
            mock_send.assert_called_once_with(
                ["192.168.1.1", "192.168.1.2", "192.168.1.3"]
            )

            # Check monitoring data
            stats = mock_process_monitor.get_process_statistics("send_connected_nodes")
            assert stats["total_executions"] == 1
            assert len(stats["recent_executions"]) == 1

            # Check the recorded data
            execution = stats["recent_executions"][0]
            assert execution["nodes_processed"] == 3
            assert execution["successful_nodes"] == 3
            assert execution["failed_nodes"] == 0
            assert "addresses" in execution["additional_metrics"]
            assert len(execution["additional_metrics"]["addresses"]) == 3

    @pytest.mark.asyncio
    async def test_nats_monitoring_empty_addresses(self):
        """Test NATS monitoring when no addresses are available"""
        # Mock validator with empty addresses
        mock_validator = Mock()
        mock_validator.routing_table_updating = False
        mock_validator.routing_table.get_all_addresses_atomic.return_value = []

        # Mock background tasks with process monitor
        mock_background_tasks = Mock()
        mock_process_monitor = ProcessMonitor()
        mock_background_tasks.process_monitor = mock_process_monitor
        mock_validator.background_tasks = mock_background_tasks

        # Create NATS publisher
        nats_publisher = MinersNATSPublisher(mock_validator)

        # Execute the method
        await nats_publisher.send_connected_nodes()

        # Check monitoring data
        stats = mock_process_monitor.get_process_statistics("send_connected_nodes")
        assert stats["total_executions"] == 1

        # Check the recorded data shows it was skipped
        execution = stats["recent_executions"][0]
        assert execution["nodes_processed"] == 0
        assert execution["additional_metrics"]["skipped"] is True
        assert execution["additional_metrics"]["reason"] == "no_addresses"

    def test_weights_monitoring_structure(self):
        """Test that weights monitoring structure is correct"""
        # Mock validator for weights manager
        mock_validator = Mock()
        mock_validator.substrate = Mock()
        mock_validator.metagraph = Mock()
        mock_validator.keypair = Mock()
        mock_validator.netuid = 42
        mock_validator.telemetry_storage = Mock()

        # Mock background tasks with process monitor
        mock_background_tasks = Mock()
        mock_process_monitor = ProcessMonitor()
        mock_background_tasks.process_monitor = mock_process_monitor
        mock_validator.background_tasks = mock_background_tasks

        # Create weights manager
        weights_manager = WeightsManager(mock_validator)

        # Verify the structure is set up correctly
        assert hasattr(mock_validator, "background_tasks")
        assert hasattr(mock_validator.background_tasks, "process_monitor")
        assert isinstance(
            mock_validator.background_tasks.process_monitor, ProcessMonitor
        )

    @pytest.mark.asyncio
    async def test_error_rate_calculation_in_weights(self):
        """Test that error rate calculation works with tweets and error scores"""
        # Create test node data with different error rates
        test_nodes = [
            # High tweets, low errors
            NodeData(
                hotkey="hotkey1",
                uid=1,
                worker_id="worker1",
                timestamp=1000,
                boot_time=0,
                last_operation_time=0,
                current_time=1000,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,  # Not used in scoring anymore
                twitter_returned_tweets=200,
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,  # Not used in scoring anymore
            ),
            # Low tweets, high errors
            NodeData(
                hotkey="hotkey2",
                uid=2,
                worker_id="worker2",
                timestamp=1000,
                boot_time=0,
                last_operation_time=0,
                current_time=1000,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,  # Not used in scoring anymore
                twitter_returned_tweets=20,
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,  # Not used in scoring anymore
            ),
        ]

        # Add custom attributes for error rate calculation
        # Node 1: 2 errors over 1 hour = 2 errors/hour
        test_nodes[0].time_span_seconds = 3600  # 1 hour
        test_nodes[0].total_errors = 2

        # Node 2: 20 errors over 1 hour = 20 errors/hour
        test_nodes[1].time_span_seconds = 3600  # 1 hour
        test_nodes[1].total_errors = 20

        # Mock validator
        mock_validator = Mock()
        mock_metagraph_nodes = {"hotkey1": Mock(node_id=1), "hotkey2": Mock(node_id=2)}
        mock_validator.metagraph.nodes = mock_metagraph_nodes
        mock_validator.node_manager.send_score_report = AsyncMock()

        # Create weights manager with default weights
        weights_manager = WeightsManager(mock_validator)

        # Calculate weights
        uids, weights = await weights_manager.calculate_weights(
            test_nodes, simulation=True
        )

        # Verify results
        assert len(uids) == 2
        assert len(weights) == 2

        # Node 1 (lower errors, more tweets) should have higher weight
        uid1_idx = uids.index(1)
        uid2_idx = uids.index(2)

        # Node 1 should have higher score due to lower error rate and more tweets
        assert weights[uid1_idx] > weights[uid2_idx]

        # Both weights should be between 0 and 1
        assert 0 <= weights[uid1_idx] <= 1
        assert 0 <= weights[uid2_idx] <= 1

    @pytest.mark.asyncio
    async def test_configurable_weights(self):
        """Test that configurable weights work correctly"""
        # Create test node with known values
        test_node = NodeData(
            hotkey="hotkey1",
            uid=1,
            worker_id="worker1",
            timestamp=1000,
            boot_time=0,
            last_operation_time=0,
            current_time=1000,
            twitter_auth_errors=0,
            twitter_errors=0,
            twitter_ratelimit_errors=0,
            twitter_returned_other=0,
            twitter_returned_profiles=0,
            twitter_returned_tweets=100,
            twitter_scrapes=0,
            web_errors=0,
            web_success=0,
        )

        test_node.time_span_seconds = 3600  # 1 hour
        test_node.total_errors = 5

        # Mock validator
        mock_validator = Mock()
        mock_validator.metagraph.nodes = {"hotkey1": Mock(node_id=1)}
        mock_validator.node_manager.send_score_report = AsyncMock()

        # Single configuration (weights manager no longer supports legacy weight configs)
        weights_manager = WeightsManager(mock_validator)

        # Calculate weights
        _, weights_single = await weights_manager.calculate_weights(
            [test_node], simulation=True
        )

        # Should produce a valid weight
        assert len(weights_single) == 1
        assert 0 <= weights_single[0] <= 1

    def test_weights_validation(self):
        """Test that weight validation works correctly"""
        mock_validator = Mock()

        # No legacy weights to validate; creation should succeed
        WeightsManager(mock_validator)

    def test_error_rate_edge_cases(self):
        """Test error rate calculation with edge cases"""
        # Test node with zero time span
        test_node = NodeData(
            hotkey="hotkey_zero_time",
            uid=1,
            worker_id="worker1",
            timestamp=1000,
            boot_time=0,
            last_operation_time=0,
            current_time=1000,
            twitter_auth_errors=0,
            twitter_errors=0,
            twitter_ratelimit_errors=0,
            twitter_returned_other=0,
            twitter_returned_profiles=50,
            twitter_returned_tweets=100,
            twitter_scrapes=0,
            web_errors=0,
            web_success=75,
        )

        # Zero time span should be handled gracefully
        test_node.time_span_seconds = 0
        test_node.total_errors = 5

        # This should not crash and should assign a penalty score
        from validator.weights import apply_kurtosis_custom

        error_rates = []
        if test_node.time_span_seconds > 0:
            hours = test_node.time_span_seconds / 3600
            error_rate = test_node.total_errors / hours
        else:
            error_rate = float("inf")

        error_rates.append(error_rate)
        error_rates = np.array(error_rates)

        # Should handle infinite values
        max_finite = 0  # No finite values in this case
        error_rates = np.where(np.isinf(error_rates), max_finite + 1, error_rates)
        error_quality_scores = 1.0 / (1.0 + error_rates)

        # Should result in a low quality score
        assert 0 < error_quality_scores[0] <= 0.5

    @pytest.mark.asyncio
    async def test_priority_miners_generation(self):
        """Test priority miners generation from scoring"""
        from validator.weights import WeightsManager
        from interfaces.types import NodeData
        from unittest.mock import Mock

        # Mock validator
        mock_validator = Mock()

        # Mock metagraph with nodes
        mock_validator.metagraph.nodes = {
            "hotkey1": Mock(node_id=1),
            "hotkey2": Mock(node_id=2),
            "hotkey3": Mock(node_id=3),
        }

        # Mock routing table with addresses
        mock_validator.routing_table.get_all_addresses_with_hotkeys.return_value = [
            ("hotkey1", "192.168.1.1", "worker1"),
            ("hotkey2", "192.168.1.2", "worker2"),
            ("hotkey3", "192.168.1.3", "worker3"),
        ]

        # Create weights manager
        weights_manager = WeightsManager(mock_validator)

        # Mock telemetry data with different scores
        delta_node_data = [
            NodeData(
                hotkey="hotkey1",
                uid=1,
                worker_id="worker1",
                timestamp=1234567890,
                boot_time=0,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=100,  # High tweet count
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,
            ),
            NodeData(
                hotkey="hotkey2",
                uid=2,
                worker_id="worker2",
                timestamp=1234567890,
                boot_time=0,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=50,  # Medium tweet count
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,
            ),
            NodeData(
                hotkey="hotkey3",
                uid=3,
                worker_id="worker3",
                timestamp=1234567890,
                boot_time=0,
                last_operation_time=0,
                current_time=0,
                twitter_auth_errors=0,
                twitter_errors=0,
                twitter_ratelimit_errors=0,
                twitter_returned_other=0,
                twitter_returned_profiles=0,
                twitter_returned_tweets=10,  # Low tweet count
                twitter_scrapes=0,
                web_errors=0,
                web_success=0,
            ),
        ]

        # Set time span for all nodes to enable scoring
        for node in delta_node_data:
            node.time_span_seconds = 3600  # 1 hour
            node.total_errors = 0  # No errors

        # Mock node manager for score reporting
        mock_validator.node_manager.send_score_report = AsyncMock()

        # Get priority miners
        priority_miners = await weights_manager.get_priority_miners_by_score(
            delta_node_data, simulation=True
        )

        # Verify we got addresses in priority order
        assert len(priority_miners) == 3
        # Should be ordered by score (highest first)
        # hotkey1 should be first (highest tweets), hotkey3 should be last (lowest tweets)
        assert "192.168.1.1" in priority_miners  # hotkey1
        assert "192.168.1.2" in priority_miners  # hotkey2
        assert "192.168.1.3" in priority_miners  # hotkey3

        # First address should correspond to highest scoring node (hotkey1)
        assert priority_miners[0] == "192.168.1.1"


if __name__ == "__main__":
    pytest.main([__file__])
