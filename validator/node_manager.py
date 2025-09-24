import random
from typing import Dict, Optional
from fiber.networking.models import NodeWithFernet as Node
from fiber.encrypted.validator import handshake, client as vali_client
from cryptography.fernet import Fernet
import os
from typing import TYPE_CHECKING
import sqlite3
from fiber.logging_utils import get_logger
from interfaces.types import NodeData
from validator.telemetry import TEETelemetryClient
from validator.errors_storage import ErrorsStorage
import asyncio
from datetime import datetime

if TYPE_CHECKING:
    from neurons.validator import Validator

logger = get_logger(__name__)


class NodeManager:
    def __init__(self, validator: "Validator"):
        """
        Initialize the NodeManager with a validator instance.

        :param validator: The validator instance to manage nodes.
        """
        self.validator = validator
        self.connected_nodes: Dict[str, Node] = {}
        self.errors_storage = ErrorsStorage()

        # Schedule error logs cleanup based on retention period
        cleanup_task = asyncio.create_task(self.run_periodic_error_cleanup())
        # Register the task with validator for graceful shutdown
        if hasattr(validator, "add_background_task"):
            validator.add_background_task(cleanup_task)

    async def run_periodic_error_cleanup(self):
        """Run periodic cleanup of error logs based on retention period."""
        cleanup_interval_hours = 6  # Run cleanup every 6 hours
        while True:
            try:
                # Check for shutdown signal
                if (
                    hasattr(self.validator, "shutdown_event")
                    and self.validator.shutdown_event.is_set()
                ):
                    logger.info(
                        "Error cleanup loop received shutdown signal, exiting..."
                    )
                    break

                # Wait for first interval
                await asyncio.sleep(cleanup_interval_hours * 3600)

                # Perform cleanup based on retention policy
                count = self.errors_storage.clean_errors_based_on_retention()
                logger.info(f"Scheduled error logs cleanup removed {count} old entries")

            except asyncio.CancelledError:
                logger.info("Error cleanup loop cancelled, exiting gracefully...")
                break
            except Exception as e:
                logger.error(f"Error during scheduled error logs cleanup: {str(e)}")
                await asyncio.sleep(3600)  # Wait one hour and try again

    async def connect_with_miner(
        self, miner_address: str, miner_hotkey: str, node: Node
    ) -> bool:
        """
        Perform a handshake with a miner and establish a secure connection.

        :param httpx_client: The HTTP client to use for the connection.
        :param miner_address: The address of the miner to connect to.
        :param miner_hotkey: The hotkey of the miner.
        :return: True if the connection was successful, False otherwise.
        """
        try:
            symmetric_key_str, symmetric_key_uuid = await handshake.perform_handshake(
                self.validator.http_client_manager.client,
                miner_address,
                self.validator.keypair,
                miner_hotkey,
            )

            if not symmetric_key_str or not symmetric_key_uuid:
                logger.error(
                    f"Failed to establish secure connection with miner {miner_hotkey}"
                )
                self.errors_storage.add_error(
                    hotkey=miner_hotkey,
                    tee_address="",
                    miner_address=miner_address,
                    message="Failed to establish secure connection",
                )
                return False

            logger.debug(
                f"************* Handshake node data address: {miner_address}, "
                f"symmetric_key_str: {symmetric_key_str}, "
                f"symmetric_key_uuid: {symmetric_key_uuid}, "
            )

            self.connected_nodes[miner_hotkey] = Node(
                hotkey=miner_hotkey,
                coldkey="",  # Not needed for validator's node tracking
                node_id=node.node_id,
                incentive=node.incentive,
                netuid=node.netuid,
                stake=node.stake,
                trust=node.trust,
                vtrust=node.vtrust,
                last_updated=node.last_updated,
                ip=node.ip,
                ip_type=node.ip_type,
                port=node.port,
                protocol=node.protocol,
                fernet=Fernet(symmetric_key_str),
                symmetric_key_uuid=symmetric_key_uuid,
            )
            logger.debug(f"Handshake successful with miner {miner_hotkey}")
            return True

        except Exception as e:
            logger.debug(
                f"Failed to connect to miner {miner_address} - {miner_hotkey}: {str(e)}"
            )
            self.errors_storage.add_error(
                hotkey=miner_hotkey,
                tee_address="",
                miner_address=miner_address,
                message=f"Connection error: {str(e)}",
            )
            return False

    async def get_tee_address(self, node: Node) -> Optional[str]:
        endpoint = "/tee"
        try:
            return await self.validator.make_non_streamed_get(node, endpoint)
        except Exception as e:
            logger.error(f"Failed to get tee address: {node.hotkey} {str(e)}")
            self.errors_storage.add_error(
                hotkey=node.hotkey,
                tee_address="",
                miner_address=f"{node.ip}:{node.port}",
                message=f"Failed to get TEE address: {str(e)}",
            )

    async def connect_new_nodes(self) -> None:
        """
        Verify node registration and attempt to connect to new nodes.

        :param httpx_client: The HTTP client to use for connections.
        """
        logger.info("Attempting nodes connection")
        try:
            nodes = dict(self.validator.metagraph.nodes)
            nodes_list = list(nodes.values())
            # Filter to specific miners if in dev environment
            if os.getenv("ENV", "prod").lower() == "dev":
                whitelist = os.getenv("MINER_WHITELIST", "").split(",")
                nodes_list = [node for node in nodes_list if node.hotkey in whitelist]

            # Filter out already connected nodes
            available_nodes = [
                node
                for node in nodes_list
                if node.hotkey not in self.connected_nodes
                and (node.ip != "0.0.0.0" or node.ip != "0.0.0.1")
            ]

            logger.info(f"Found {len(available_nodes)} miners")
            for node in available_nodes:

                if node.ip == "0":
                    if os.getenv("DEBUG", "false").lower() == "true":
                        logger.warn(f"Skipping node {node.hotkey}: ip is {node.ip}")
                    self.errors_storage.add_error(
                        hotkey=node.hotkey,
                        tee_address="",
                        miner_address=f"{node.ip}:{node.port}",
                        message="Skipped: IP is 0",
                    )
                    continue

                server_address = vali_client.construct_server_address(
                    node=node,
                    replace_with_docker_localhost=True,
                    replace_with_localhost=True,
                )
                success = await self.connect_with_miner(
                    miner_address=server_address, miner_hotkey=node.hotkey, node=node
                )

                if success:
                    logger.info(
                        f"Connected to miner: {node.hotkey}, IP: {node.ip}, Port: {node.port}"
                    )
                else:
                    logger.debug(
                        f"Failed to connect to miner {node.hotkey} with address {server_address}"
                    )

        except Exception as e:
            logger.error("Error in registration check: %s", str(e))

    async def remove_disconnected_nodes(self):
        keys_to_delete = []
        for hotkey, _ in self.connected_nodes.items():
            if hotkey not in self.validator.metagraph.nodes:
                logger.info(
                    f"Hotkey: {hotkey} has been deregistered from the metagraph"
                )
                self.errors_storage.add_error(
                    hotkey=hotkey,
                    tee_address="",
                    miner_address="",
                    message="Node deregistered from metagraph",
                )
                keys_to_delete.append(hotkey)

        logger.info(f"Deleteing keys from connected nodes: {keys_to_delete}")
        for hotkey in keys_to_delete:
            del self.connected_nodes[hotkey]
            self.validator.routing_table.clear_miner(hotkey)

    async def send_custom_message(self, node_hotkey: str, message: str) -> None:
        """
        Send a custom message to a specific miner.

        Args:
            node_hotkey (str): The miner's hotkey
            message (str): The message to send
        """
        try:
            if node_hotkey not in self.connected_nodes:
                logger.debug(
                    f"Warning: No connected node found for hotkey {node_hotkey}"
                )
                self.errors_storage.add_error(
                    hotkey=node_hotkey,
                    tee_address="",
                    miner_address="",
                    message="Failed to send message: Node not connected",
                )
                return

            node = self.connected_nodes[node_hotkey]
            uid = str(
                self.validator.metagraph.nodes[
                    self.validator.keypair.ss58_address
                ].node_id
            )
            payload = {
                "message": message,
                "sender": f"Validator {uid} ({self.validator.keypair.ss58_address})",
            }

            response = await self.validator.http_client_manager.client.post(
                f"http://{node.ip}:{node.port}/custom-message", json=payload
            )

            if response.status_code == 200:
                logger.debug(f"Successfully sent custom message to miner {node_hotkey}")
            else:
                logger.warning(
                    f"Failed to send custom message to miner {node_hotkey}. "
                    f"Status code: {response.status_code}"
                )
                self.errors_storage.add_error(
                    hotkey=node_hotkey,
                    tee_address="",
                    miner_address=f"{node.ip}:{node.port}",
                    message=f"Failed to send message: Status code {response.status_code}",
                )

        except Exception as e:
            logger.error(
                f"Error sending custom message to miner {node_hotkey}: {str(e)}"
            )
            self.errors_storage.add_error(
                hotkey=node_hotkey,
                tee_address="",
                miner_address="",
                message=f"Error sending message: {str(e)}",
            )

    async def update_tee_list(self):
        logger.info("Starting TEE list update")
        routing_table = self.validator.routing_table

        # Note: routing_table_updating flag is now managed by background_tasks
        # to ensure proper coordination with NATS publishing

        # Get all current entries and initialize tracking
        current_entries_set, verified_entries = self._get_current_entries_for_update(
            routing_table
        )

        # Process all connected nodes
        await self._process_connected_nodes(routing_table, verified_entries)

        # Clean up unverified entries
        await self._cleanup_unverified_entries(
            routing_table, current_entries_set, verified_entries
        )

        # Clean up unregistered TEEs
        await self._cleanup_unregistered_tees(routing_table)

        logger.info("Completed TEE list update ✅")

    def _get_current_entries_for_update(self, routing_table):
        """Get all current addresses before starting update to track what needs cleanup."""
        all_current_entries = (
            routing_table.get_all_addresses_with_hotkeys()
        )  # Insert order here
        current_entries_set = set()
        for hotkey, address, worker_id in all_current_entries:
            current_entries_set.add((hotkey, address))

        logger.debug(
            f"Starting update with {len(current_entries_set)} existing entries"
        )

        # Track entries that are successfully verified in this update cycle
        verified_entries = set()

        return current_entries_set, verified_entries

    async def _process_connected_nodes(self, routing_table, verified_entries):
        """Process all connected nodes for TEE registration."""
        # Shuffle connected nodes for fair processing order
        connected_nodes_items = list(self.connected_nodes.items())
        random.shuffle(connected_nodes_items)

        for hotkey, _ in connected_nodes_items:
            logger.debug(f"Processing hotkey: {hotkey}")
            if hotkey in self.validator.metagraph.nodes:
                node = self.validator.metagraph.nodes[hotkey]
                await self._process_single_node(
                    node, hotkey, routing_table, verified_entries
                )

    async def _process_single_node(self, node, hotkey, routing_table, verified_entries):
        """Process a single node's TEE addresses."""
        if node.ip == "0":
            self.errors_storage.add_error(
                hotkey=hotkey,
                tee_address="",
                miner_address=f"{node.ip}:{node.port}",
                message="Skipped updating TEE: IP is 0",
            )
            return

        logger.debug(f"Found node in metagraph for hotkey: {hotkey}")

        try:
            tee_addresses = await self.get_tee_address(node)
            logger.debug(
                f"Retrieved TEE addresses for hotkey {hotkey}: {tee_addresses}"
            )

            if tee_addresses:
                for tee_address in tee_addresses.split(","):
                    tee_address = tee_address.strip()
                    await self._process_tee_address(
                        tee_address, node, hotkey, routing_table, verified_entries
                    )
            else:
                logger.debug(f"No TEE addresses found for hotkey {hotkey}")
                # If a node has no TEE addresses, mark all its current entries for cleanup
                current_tees = routing_table.get_miner_addresses(hotkey=hotkey)
                for address, _ in current_tees if current_tees else []:
                    logger.info(
                        f"Marking {address} for cleanup (no TEE addresses provided)"
                    )
                    # IMPORTANT: This cleanup is not happening

        except Exception as e:
            logger.error(f"Error processing hotkey {hotkey}: {str(e)}")
            self.errors_storage.add_error(
                hotkey=hotkey,
                tee_address="",
                miner_address=f"{node.ip}:{node.port}",
                message=f"Error during TEE update: {str(e)}",
            )

    async def _process_tee_address(
        self,
        tee_address,
        node,
        hotkey,
        routing_table,
        verified_entries,
    ):
        """Process a single TEE address for registration."""
        # Skip if localhost
        if "localhost" in tee_address or "127.0.0.1" in tee_address:
            logger.debug(f"Skipping localhost TEE address {tee_address} - {hotkey}")
            self.errors_storage.add_error(
                hotkey=hotkey,
                tee_address=tee_address,
                miner_address=f"{node.ip}:{node.port}",
                message="Skipped: localhost TEE address",
            )
            return

        # Skip if not https
        if not tee_address.startswith("https://"):
            logger.debug(f"Skipping non-HTTPS TEE address {tee_address} - {hotkey}")
            self.errors_storage.add_error(
                hotkey=hotkey,
                tee_address=tee_address,
                miner_address=f"{node.ip}:{node.port}",
                message="Skipped: non-HTTPS TEE address",
            )
            return

        try:
            telemetry_client = TEETelemetryClient(tee_address)

            logger.info(f"Getting registration telemetry for {hotkey} at {tee_address}")

            telemetry_result = await telemetry_client.execute_telemetry_sequence(
                routing_table=routing_table
            )

            if not telemetry_result:
                await self._handle_telemetry_failure(
                    hotkey,
                    tee_address,
                    node,
                    routing_table,
                    "Telemetry failed to return results",
                )
                return

            logger.info(
                f"Telemetry successful for hotkey {hotkey} "
                f"at {tee_address} with worker_id "
                f"{telemetry_result.get('worker_id', 'N/A')}"
            )

            worker_id = telemetry_result.get("worker_id", None)

            if worker_id is None:
                await self._handle_telemetry_failure(
                    hotkey,
                    tee_address,
                    node,
                    routing_table,
                    "Skipped: No worker_id returned from telemetry",
                )
                return

            # Check worker ownership
            worker_hotkey = self.validator.routing_table.get_worker_hotkey(worker_id)

            logger.info(f"worker id: {worker_id}")
            logger.info(f"worker hotkey: {worker_hotkey}")
            logger.info(f"node hotkey: {hotkey}")

            is_worker_already_owned = (
                worker_hotkey is not None and worker_hotkey != hotkey
            )

            # This checks that a worker address is only owned by the first node that requests it
            # For removing this restriction please communicate on discord
            if is_worker_already_owned:
                logger.warning(
                    f"Worker ID {worker_id} is already registered to another hotkey. "
                    f"({worker_hotkey}) Skipping registration for {hotkey}."
                )
                self.errors_storage.add_error(
                    hotkey=hotkey,
                    tee_address=tee_address,
                    miner_address=f"{node.ip}:{node.port}",
                    message=f"Skipped: Worker ID {worker_id} already registered to hotkey {worker_hotkey}",
                )
                return

            # Register the worker and TEE address
            await self._register_tee_address(
                routing_table,
                hotkey,
                node,
                tee_address,
                worker_id,
                worker_hotkey,
                verified_entries,
            )

        except sqlite3.IntegrityError:
            logger.debug(f"Address {tee_address} already exists for another miner")
            self.errors_storage.add_error(
                hotkey=hotkey,
                tee_address=tee_address,
                miner_address=f"{node.ip}:{node.port}",
                message="Address already exists for another miner",
            )
        except Exception as e:
            logger.error(
                f"Error registering TEE address {tee_address} for hotkey {hotkey}: {str(e)}"
            )
            # Add to unregistered TEEs table for tracking
            await self.validator.routing_table.add_unregistered_tee(
                address=tee_address, hotkey=hotkey, validator=self.validator
            )
            self.errors_storage.add_error(
                hotkey=hotkey,
                tee_address=tee_address,
                miner_address=f"{node.ip}:{node.port}",
                message=f"Error during registration: {str(e)}",
            )

    async def _handle_telemetry_failure(
        self, hotkey, tee_address, node, routing_table, message
    ):
        """Handle cases where telemetry fails or returns invalid data."""
        logger.warn(
            f"Telemetry failed for hotkey {hotkey} - {tee_address} - {node.ip}:{node.port}"
        )
        # Add to unregistered TEEs table for tracking
        await self.validator.routing_table.add_unregistered_tee(
            address=tee_address, hotkey=hotkey, validator=self.validator
        )

        logger.info(f"Added to unregistered TEEs: {tee_address} for hotkey {hotkey}")
        self.errors_storage.add_error(
            hotkey=hotkey,
            tee_address=tee_address,
            miner_address=f"{node.ip}:{node.port}",
            message=message,
        )

    async def _register_tee_address(
        self,
        routing_table,
        hotkey,
        node,
        tee_address,
        worker_id,
        worker_hotkey,
        verified_entries,
    ):
        """Register a TEE address and send notifications."""
        routing_table.register_worker(hotkey=hotkey, worker_id=worker_id)
        routing_table.add_miner_address(hotkey, node.node_id, tee_address, worker_id)

        logger.debug(f"Added TEE address {tee_address} for hotkey {hotkey}")

        # Mark this entry as verified in this update cycle
        verified_entries.add((hotkey, tee_address))

        # Check if this is a new worker registration (worker_id was not set before)
        if worker_hotkey is None:
            logger.info(f"New worker registration: {worker_id} for hotkey {hotkey}")
            # Send notification about new worker registration
            await self.send_custom_message(
                hotkey,
                f"New worker registration: Your worker ID {worker_id} has been registered for the first time with hotkey {hotkey}",
            )

        # Send notification to miner about successful registration
        await self.send_custom_message(
            hotkey,
            f"Your TEE address {tee_address} has been successfully registered with worker_id {worker_id} for hotkey {hotkey}",
        )

    async def _cleanup_unverified_entries(
        self, routing_table, current_entries_set, verified_entries
    ):
        """Clean up entries that weren't verified in this cycle and are older than a reasonable threshold."""
        unverified_entries = current_entries_set - verified_entries
        if unverified_entries:
            logger.info(
                f"Performing graceful cleanup of {len(unverified_entries)} unverified entries"
            )
            for hotkey, address in unverified_entries:
                try:
                    # Get the entry details to check age
                    current_tees = routing_table.get_miner_addresses(hotkey=hotkey)
                    for addr, worker_id in current_tees if current_tees else []:
                        if addr == address:
                            # Check if the entry is at least 4 hours old
                            timestamp_str = routing_table.get_address_timestamp(address)
                            if timestamp_str:
                                try:
                                    # Parse SQLite timestamp format
                                    entry_time = datetime.fromisoformat(
                                        timestamp_str.replace(" ", "T")
                                    )
                                    current_time = datetime.now()
                                    age_hours = (
                                        current_time - entry_time
                                    ).total_seconds() / 3600

                                    if age_hours >= 4:
                                        routing_table.remove_miner_address_by_address(
                                            address
                                        )
                                        logger.info(
                                            f"Cleaned up unverified entry (age: {age_hours:.1f}h): "
                                            f"{hotkey} - {address}"
                                        )
                                    else:
                                        logger.debug(
                                            f"Skipping cleanup of recent entry (age: {age_hours:.1f}h): "
                                            f"{hotkey} - {address}"
                                        )
                                except Exception as parse_error:
                                    logger.error(
                                        f"Error parsing timestamp for {address}: {parse_error}"
                                    )
                            break
                except Exception as e:
                    logger.error(
                        f"Error during cleanup of {hotkey} - {address}: {str(e)}"
                    )

    async def _cleanup_unregistered_tees(self, routing_table):
        """Clean up any unregistered TEEs that are now in the routing table."""
        try:
            # Get all registered addresses
            registered_addrs = routing_table.get_all_addresses()

            # Get current list of unregistered TEE addresses
            unregistered_addrs = routing_table.get_all_unregistered_tee_addresses()

            # Check which addresses should be removed from unregistered list
            cleaned_count = 0

            for address in registered_addrs:
                if address in unregistered_addrs:
                    # This address was previously unregistered but is now registered
                    routing_table.remove_unregistered_tee(address)
                    cleaned_count += 1

            if cleaned_count > 0:
                logger.info(
                    f"Cleaned {cleaned_count} addresses from unregistered TEEs that are now registered"
                )
        except Exception as e:
            logger.error(f"Error cleaning up unregistered TEEs: {str(e)}")

    async def send_score_report(
        self, node_hotkey: str, score: float, telemetry: NodeData
    ) -> None:
        """
        Send a score report to a specific miner.

        Args:
            hotkey (str): The miner's hotkey
            score (float): The calculated score for the miner
            telemetry (dict): The telemetry data for the miner
        """
        try:
            if node_hotkey not in self.connected_nodes:
                logger.warning(f"No connected node found for hotkey {node_hotkey}")
                self.errors_storage.add_error(
                    hotkey=node_hotkey,
                    tee_address="",
                    miner_address="",
                    message="Failed to send score report: Node not connected",
                )
                return

            node = self.connected_nodes[node_hotkey]
            validator_node_id = self.validator.metagraph.nodes[
                self.validator.keypair.ss58_address
            ].node_id

            # Calculate platform scores for detailed feedback
            platform_scores = {}
            weights_manager = self.validator.weights_manager

            for platform_name in weights_manager.platform_manager.get_platform_names():
                platform_config = weights_manager.platform_manager.get_platform(
                    platform_name
                )

                # Prefer normalized score computed during the latest scoring run
                normalized_score = None
                try:
                    normalized_score = weights_manager.platform_normalized_scores.get(
                        node_hotkey, {}
                    ).get(platform_name, None)
                except Exception:
                    normalized_score = None

                # Fallback to raw success score if normalized not available
                if normalized_score is None:
                    raw_success = weights_manager.calculate_platform_score(
                        telemetry, platform_name
                    )
                    # Without cohort, cannot normalize here; report raw success as 0.0..1.0=0
                    # to avoid huge numbers in miner report. Keep 0.0 to be conservative.
                    normalized_score = 0.0 if raw_success == 0 else 1.0

                platform_scores[platform_name] = {
                    "score": float(normalized_score),
                    "weight": platform_config.emission_weight,
                    "weighted_score": (
                        float(normalized_score) * platform_config.emission_weight
                    ),
                }

            payload = {
                "telemetry": (
                    telemetry.stats_json
                    if telemetry.stats_json
                    else {
                        # Fallback to legacy fields if stats_json is empty
                        "web_success": telemetry.web_success,
                        "twitter_returned_tweets": telemetry.twitter_returned_tweets,
                        "twitter_returned_profiles": telemetry.twitter_returned_profiles,
                        "twitter_errors": telemetry.twitter_errors,
                        "twitter_auth_errors": telemetry.twitter_auth_errors,
                        "twitter_ratelimit_errors": telemetry.twitter_ratelimit_errors,
                        "web_errors": telemetry.web_errors,
                        "boot_time": telemetry.boot_time,
                        "last_operation_time": telemetry.last_operation_time,
                        "current_time": telemetry.current_time,
                    }
                ),
                "platform_metrics": (
                    telemetry.platform_metrics if telemetry.platform_metrics else {}
                ),
                "platform_scores": platform_scores,
                "score": score,
                "hotkey": self.validator.keypair.ss58_address,
                "uid": validator_node_id,
            }

            response = await self.validator.http_client_manager.client.post(
                f"http://{node.ip}:{node.port}/score-report", json=payload
            )

            if response.status_code == 200:
                logger.debug(f"Successfully sent score report to miner {node_hotkey}")
            else:
                logger.warning(
                    f"Failed to send score report to miner {node_hotkey}. "
                    f"Status code: {response.status_code}"
                )
                self.errors_storage.add_error(
                    hotkey=node_hotkey,
                    tee_address="",
                    miner_address=f"{node.ip}:{node.port}",
                    message=f"Failed to send score report: Status code {response.status_code}",
                )

        except Exception as e:
            logger.error(f"Error sending score report to miner {node_hotkey}: {str(e)}")
            self.errors_storage.add_error(
                hotkey=node_hotkey,
                tee_address="",
                miner_address="",
                message=f"Error sending score report: {str(e)}",
            )
