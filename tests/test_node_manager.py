import unittest
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock
import asyncio
from unittest.mock import patch
import sys
import types
import logging

# `validator.telemetry` imports `httpx`. Provide a minimal stub so importing
# `validator.node_manager` doesn't fail in lightweight environments.
try:  # pragma: no cover
    import httpx  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    httpx = types.ModuleType("httpx")
    sys.modules["httpx"] = httpx

    class AsyncClient:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    httpx.AsyncClient = AsyncClient  # type: ignore

# `validator.node_manager` imports `cryptography.fernet.Fernet`. Stub it only if the
# dependency isn't available in the current environment (CI should have it).
try:  # pragma: no cover
    import cryptography  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    cryptography = types.ModuleType("cryptography")
    sys.modules["cryptography"] = cryptography
    cryptography.fernet = types.ModuleType("cryptography.fernet")
    sys.modules["cryptography.fernet"] = cryptography.fernet

    class Fernet:  # type: ignore
        def __init__(self, *_args, **_kwargs):
            pass

    cryptography.fernet.Fernet = Fernet  # type: ignore

# The validator code depends on `fiber`. In minimal local environments it may not be
# installed, but we still want unit tests to import and run. If `fiber` is present
# (e.g. in CI), this stub is skipped.
try:  # pragma: no cover
    import fiber  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    fiber = types.ModuleType("fiber")
    sys.modules["fiber"] = fiber

    fiber.logging_utils = types.ModuleType("fiber.logging_utils")
    sys.modules["fiber.logging_utils"] = fiber.logging_utils

    def get_logger(name):  # type: ignore
        return logging.getLogger(name)

    fiber.logging_utils.get_logger = get_logger  # type: ignore

    fiber.networking = types.ModuleType("fiber.networking")
    sys.modules["fiber.networking"] = fiber.networking
    fiber.networking.models = types.ModuleType("fiber.networking.models")
    sys.modules["fiber.networking.models"] = fiber.networking.models

    class NodeWithFernet:  # minimal stand-in used by NodeManager
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    fiber.networking.models.NodeWithFernet = NodeWithFernet  # type: ignore

    fiber.encrypted = types.ModuleType("fiber.encrypted")
    sys.modules["fiber.encrypted"] = fiber.encrypted
    fiber.encrypted.validator = types.ModuleType("fiber.encrypted.validator")
    sys.modules["fiber.encrypted.validator"] = fiber.encrypted.validator

    fiber.encrypted.validator.handshake = types.ModuleType(
        "fiber.encrypted.validator.handshake"
    )
    sys.modules["fiber.encrypted.validator.handshake"] = (
        fiber.encrypted.validator.handshake
    )

    async def perform_handshake(*_args, **_kwargs):  # type: ignore
        return (None, None)

    fiber.encrypted.validator.handshake.perform_handshake = perform_handshake  # type: ignore

    fiber.encrypted.validator.client = types.ModuleType("fiber.encrypted.validator.client")
    sys.modules["fiber.encrypted.validator.client"] = fiber.encrypted.validator.client

from validator.node_manager import NodeManager
from fiber.networking.models import NodeWithFernet as Node


class TestNodeManager(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Mock the Validator and Node
        self.mock_validator = MagicMock()
        # Ensure NodeManager background tasks exit quickly in tests
        self.mock_validator.shutdown_event = asyncio.Event()
        self.mock_validator.shutdown_event.set()

        self.mock_node = MagicMock(spec=Node)
        self.mock_node.node_id = "test_node_id"
        # Attributes accessed by `connect_with_miner()` when it stores the node.
        self.mock_node.incentive = 0
        self.mock_node.netuid = 0
        self.mock_node.stake = 0
        self.mock_node.trust = 0
        self.mock_node.vtrust = 0
        self.mock_node.last_updated = 0
        self.mock_node.ip = "127.0.0.1"
        self.mock_node.ip_type = 4
        self.mock_node.port = 1234
        self.mock_node.protocol = "http"
        self.node_manager = NodeManager(validator=self.mock_validator)
        self.node_manager.send_custom_message = AsyncMock()

    async def asyncTearDown(self):
        # Best-effort: cancel any background tasks we registered.
        with suppress(Exception):
            for call in getattr(
                self.mock_validator.add_background_task, "call_args_list", []
            ):
                task = call.args[0]
                if hasattr(task, "cancel"):
                    task.cancel()
                with suppress(Exception):
                    await task

    async def test_connect_with_miner_success(self):
        # Mock the handshake function to return a valid key and UUID
        self.mock_validator.http_client_manager.client = AsyncMock()
        self.mock_validator.keypair = MagicMock()
        self.mock_node.hotkey = "test_hotkey"

        # Mock the perform_handshake function
        with unittest.mock.patch(
            "fiber.encrypted.validator.handshake.perform_handshake",
            new=AsyncMock(return_value=("symmetric_key_str", "symmetric_key_uuid")),
        ):
            result = await self.node_manager.connect_with_miner(
                miner_address="test_address",
                miner_hotkey="test_hotkey",
                node=self.mock_node,
            )
            self.assertTrue(result)
            self.assertIn("test_hotkey", self.node_manager.connected_nodes)

    async def test_connect_with_miner_failure(self):
        # Mock the handshake function to return None
        self.mock_validator.http_client_manager.client = AsyncMock()
        self.mock_validator.keypair = MagicMock()
        self.mock_node.hotkey = "test_hotkey"

        # Mock the perform_handshake function
        with unittest.mock.patch(
            "fiber.encrypted.validator.handshake.perform_handshake",
            new=AsyncMock(return_value=(None, None)),
        ):
            result = await self.node_manager.connect_with_miner(
                miner_address="test_address",
                miner_hotkey="test_hotkey",
                node=self.mock_node,
            )
            self.assertFalse(result)
            self.assertNotIn("test_hotkey", self.node_manager.connected_nodes)

    async def test_register_tee_address_wipes_telemetry_on_rotation(self):
        hotkey = "hk1"
        old_address = "tee://old"
        new_address = "tee://new"

        routing_table = MagicMock()
        routing_table.get_miner_addresses.side_effect = [
            [(old_address, "w1")],
            [(new_address, "w1")],
        ]
        routing_table.register_worker = MagicMock()
        routing_table.add_miner_address = MagicMock()

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey = MagicMock(
            return_value=3
        )

        async def direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("validator.node_manager.asyncio.to_thread", new=AsyncMock(side_effect=direct)):
            verified = set()
            await self.node_manager._register_tee_address(
                routing_table=routing_table,
                hotkey=hotkey,
                node=self.mock_node,
                tee_address=new_address,
                worker_id="worker-1",
                worker_hotkey="existing-hotkey",
                verified_entries=verified,
            )

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey.assert_called_once_with(
            hotkey
        )

    async def test_register_tee_address_does_not_wipe_when_unchanged(self):
        hotkey = "hk2"
        address = "tee://same"

        routing_table = MagicMock()
        routing_table.get_miner_addresses.side_effect = [
            [(address, "w1")],
            [(address, "w1")],
        ]
        routing_table.register_worker = MagicMock()
        routing_table.add_miner_address = MagicMock()

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey = MagicMock()

        async def direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("validator.node_manager.asyncio.to_thread", new=AsyncMock(side_effect=direct)):
            verified = set()
            await self.node_manager._register_tee_address(
                routing_table=routing_table,
                hotkey=hotkey,
                node=self.mock_node,
                tee_address=address,
                worker_id="worker-1",
                worker_hotkey="existing-hotkey",
                verified_entries=verified,
            )

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey.assert_not_called()

    async def test_register_tee_address_does_not_wipe_when_add_skipped(self):
        hotkey = "hk3"
        existing_address = "tee://existing"
        conflicting_address = "tee://conflict"

        routing_table = MagicMock()
        # Post-update state remains unchanged (e.g. address conflict => add no-op)
        routing_table.get_miner_addresses.side_effect = [
            [(existing_address, "w1")],
            [(existing_address, "w1")],
        ]
        routing_table.register_worker = MagicMock()
        routing_table.add_miner_address = MagicMock()

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey = MagicMock()

        async def direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("validator.node_manager.asyncio.to_thread", new=AsyncMock(side_effect=direct)):
            verified = set()
            await self.node_manager._register_tee_address(
                routing_table=routing_table,
                hotkey=hotkey,
                node=self.mock_node,
                tee_address=conflicting_address,
                worker_id="worker-1",
                worker_hotkey="existing-hotkey",
                verified_entries=verified,
            )

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey.assert_not_called()

    async def test_register_tee_address_wipes_when_previous_cleared(self):
        hotkey = "hk4"
        new_address = "tee://new"

        routing_table = MagicMock()
        routing_table.get_miner_addresses.side_effect = [
            [],
            [(new_address, "w1")],
        ]
        routing_table.register_worker = MagicMock()
        routing_table.add_miner_address = MagicMock()

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey = MagicMock(
            return_value=7
        )

        async def direct(func, *args, **kwargs):
            return func(*args, **kwargs)

        with patch("validator.node_manager.asyncio.to_thread", new=AsyncMock(side_effect=direct)):
            verified = set()
            await self.node_manager._register_tee_address(
                routing_table=routing_table,
                hotkey=hotkey,
                node=self.mock_node,
                tee_address=new_address,
                worker_id="worker-1",
                worker_hotkey="existing-hotkey",
                verified_entries=verified,
            )

        self.mock_validator.telemetry_storage.delete_telemetry_by_hotkey.assert_called_once_with(
            hotkey
        )

if __name__ == "__main__":
    unittest.main()
