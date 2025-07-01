from fastapi import FastAPI, Depends, HTTPException, Header, Body
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
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

    def get_api_key_dependency(self) -> Callable:
        """Get a dependency function that checks the API key against config."""

        def check_api_key():
            return require_api_key(config=self.validator.config)

        return check_api_key

    def register_routes(self) -> None:
        # Mount static files directory
        try:
            self.app.mount("/static", StaticFiles(directory="static"), name="static")
        except Exception as e:
            logger.error(f"Failed to mount static files: {str(e)}, cwd: {os.getcwd()}")

        self.app.add_api_route(
            "/healthcheck",
            self.healthcheck,
            methods=["GET"],
            tags=["healthcheck"],
        )

        # Create API key dependency with config
        api_key_dependency = self.get_api_key_dependency()

        # Add monitoring endpoints with API key protection
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
            "/monitor/unregistered-tee-addresses",
            self.monitor_unregistered_tee_addresses,
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

        # Add error monitoring endpoints
        self.app.add_api_route(
            "/monitor/errors",
            self.monitor_errors,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/monitor/errors/{hotkey}",
            self.monitor_errors_by_hotkey,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/monitor/errors/cleanup",
            self.cleanup_old_errors,
            methods=["POST"],
            tags=["maintenance"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add process monitoring endpoint
        self.app.add_api_route(
            "/monitoring/processes",
            self.monitor_processes,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add NATS monitoring endpoint
        self.app.add_api_route(
            "/monitoring/nats",
            self.monitor_nats_publishing,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add NATS trigger endpoint
        self.app.add_api_route(
            "/trigger/nats/send-connected-nodes",
            self.trigger_send_connected_nodes,
            methods=["POST"],
            tags=["trigger"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add weights monitoring endpoint
        self.app.add_api_route(
            "/monitoring/weights",
            self.monitor_weights_setting,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add priority miners monitoring endpoint
        self.app.add_api_route(
            "/monitoring/priority-miners",
            self.monitor_priority_miners_publishing,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add weighted priority miners list endpoint
        self.app.add_api_route(
            "/monitor/priority-miners-list",
            self.get_weighted_priority_miners_list,
            methods=["GET"],
            tags=["monitoring"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add HTML page routes
        self.app.add_api_route(
            "/errors",
            self.serve_error_logs_page,
            methods=["GET"],
            tags=["pages"],
            response_class=HTMLResponse,
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/workers",
            self.serve_worker_registry_page,
            methods=["GET"],
            tags=["pages"],
            response_class=HTMLResponse,
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/routing",
            self.serve_routing_table_page,
            methods=["GET"],
            tags=["pages"],
            response_class=HTMLResponse,
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/unregistered-nodes",
            self.serve_unregistered_nodes_page,
            methods=["GET"],
            tags=["pages"],
            response_class=HTMLResponse,
            dependencies=[Depends(api_key_dependency)],
        )

        # Add dashboard endpoint
        self.app.add_api_route(
            "/dashboard",
            self.dashboard,
            methods=["GET"],
            tags=["dashboard"],
            response_class=HTMLResponse,
            dependencies=[Depends(api_key_dependency)],
        )

        # Add JSON API endpoint for dashboard data
        self.app.add_api_route(
            "/dashboard/data",
            self.dashboard_data,
            methods=["GET"],
            tags=["dashboard"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add JSON API endpoint for score simulation data
        self.app.add_api_route(
            "/score-simulation/data",
            self.score_simulation_data,
            methods=["GET"],
            tags=["simulation"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add Score Simulation HTML Page Route
        self.app.add_api_route(
            "/score-simulation",
            self.serve_score_simulation_page,
            methods=["GET"],
            tags=["pages"],
            response_class=HTMLResponse,
            dependencies=[Depends(api_key_dependency)],
        )

        # Add unregistered TEE management endpoint
        self.app.add_api_route(
            "/add-unregistered-tee",
            self.add_unregistered_tee,
            methods=["POST"],
            tags=["management"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add PostgreSQL telemetry endpoints
        self.app.add_api_route(
            "/telemetry/postgresql/all",
            self.monitor_postgresql_telemetry,
            methods=["GET"],
            tags=["telemetry"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/telemetry/postgresql/stats",
            self.monitor_postgresql_telemetry_stats,
            methods=["GET"],
            tags=["telemetry"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/telemetry/postgresql/{hotkey}",
            self.monitor_postgresql_telemetry_by_hotkey,
            methods=["GET"],
            tags=["telemetry"],
            dependencies=[Depends(api_key_dependency)],
        )

        # Add multi-platform scoring monitoring endpoints
        self.app.add_api_route(
            "/monitor/platforms",
            self.monitor_platforms,
            methods=["GET"],
            tags=["multi-platform"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/monitor/platforms/scores",
            self.monitor_platform_scores,
            methods=["GET"],
            tags=["multi-platform"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/monitor/platforms/emissions",
            self.monitor_emission_distribution,
            methods=["GET"],
            tags=["multi-platform"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/monitor/platforms/weights",
            self.monitor_weights_distribution,
            methods=["GET"],
            tags=["multi-platform"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/monitor/platforms/performance",
            self.monitor_platform_performance,
            methods=["GET"],
            tags=["multi-platform"],
            dependencies=[Depends(api_key_dependency)],
        )

        self.app.add_api_route(
            "/monitor/platforms/analytics",
            self.monitor_platform_analytics,
            methods=["GET"],
            tags=["multi-platform"],
            dependencies=[Depends(api_key_dependency)],
        )

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

            # Convert NodeData objects to dictionaries
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict = {
                    "hotkey": data.hotkey,
                    "uid": data.uid,
                    "timestamp": data.timestamp,
                    "boot_time": data.boot_time,
                    "last_operation_time": data.last_operation_time,
                    "current_time": data.current_time,
                    "twitter_auth_errors": data.twitter_auth_errors,
                    "twitter_errors": data.twitter_errors,
                    "twitter_ratelimit_errors": data.twitter_ratelimit_errors,
                    "twitter_returned_other": data.twitter_returned_other,
                    "twitter_returned_profiles": data.twitter_returned_profiles,
                    "twitter_returned_tweets": data.twitter_returned_tweets,
                    "twitter_scrapes": data.twitter_scrapes,
                    "web_errors": data.web_errors,
                    "web_success": data.web_success,
                    "tiktok_transcription_success": getattr(
                        data, "tiktok_transcription_success", 0
                    ),
                    "tiktok_transcription_errors": getattr(
                        data, "tiktok_transcription_errors", 0
                    ),
                    "worker_id": data.worker_id if hasattr(data, "worker_id") else None,
                }
                telemetry_dict_list.append(telemetry_dict)

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
        try:
            # Validate input
            if not address or not hotkey:
                raise HTTPException(
                    status_code=400,
                    detail="Both 'address' and 'hotkey' are required fields",
                )

            # Get the API URL from environment variables
            masa_tee_api = os.getenv("MASA_TEE_API", "")
            if not masa_tee_api:
                logger.error("MASA_TEE_API environment variable not set")
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

            async with aiohttp.ClientSession() as session:
                async with session.post(api_endpoint, json=payload) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        logger.info(
                            f"Successfully registered TEE worker with MASA API: {address}"
                        )
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
                        return {"success": False, "error": error_msg}

        except aiohttp.ClientError as e:
            logger.error(f"API connection error: {str(e)}")
            return {"success": False, "error": f"API connection error: {str(e)}"}
        except Exception as e:
            logger.error(f"Failed to register TEE worker: {str(e)}")
            return {"success": False, "error": str(e)}

    async def dashboard(self):
        # Implement the dashboard logic for the validator
        return self.validator.dashboard()

    async def dashboard_data(self):
        # Implement the dashboard data logic for the validator
        return self.validator.dashboard_data()

    async def serve_error_logs_page(self):
        """Serve the error logs HTML page"""
        try:
            with open("static/error-logs.html", "r") as f:
                content = f.read()

            # Replace placeholders with actual values
            network = self.validator.config.SUBTENSOR_NETWORK.upper()
            content = content.replace("{{network}}", network)
            content = content.replace(
                "{{current_year}}", str(datetime.datetime.now().year)
            )

            return HTMLResponse(content=content)
        except Exception as e:
            logger.error(f"Failed to serve error logs page: {str(e)}")
            return HTMLResponse(content=f"<html><body>Error: {str(e)}</body></html>")

    async def serve_worker_registry_page(self):
        """Serve the worker registry HTML page"""
        try:
            with open("static/worker-registry.html", "r") as f:
                content = f.read()

            # Replace placeholders with actual values
            network = self.validator.config.SUBTENSOR_NETWORK.upper()
            content = content.replace("{{network}}", network)
            content = content.replace(
                "{{current_year}}", str(datetime.datetime.now().year)
            )

            return HTMLResponse(content=content)
        except Exception as e:
            logger.error(f"Failed to serve worker registry page: {str(e)}")
            return HTMLResponse(content=f"<html><body>Error: {str(e)}</body></html>")

    async def serve_routing_table_page(self):
        """Serve the routing table HTML page"""
        try:
            with open("static/routing-table.html", "r") as f:
                content = f.read()

            # Replace placeholders with actual values
            network = self.validator.config.SUBTENSOR_NETWORK.upper()
            content = content.replace("{{network}}", network)
            content = content.replace(
                "{{current_year}}", str(datetime.datetime.now().year)
            )

            return HTMLResponse(content=content)
        except Exception as e:
            logger.error(f"Failed to serve routing table page: {str(e)}")
            return HTMLResponse(content=f"<html><body>Error: {str(e)}</body></html>")

    async def serve_unregistered_nodes_page(self):
        """Serve the unregistered nodes HTML page"""
        try:
            with open("static/unregistered-nodes.html", "r") as f:
                content = f.read()

            # Replace placeholders with actual values
            network = self.validator.config.SUBTENSOR_NETWORK.upper()
            content = content.replace("{{network}}", network)
            content = content.replace(
                "{{current_year}}", str(datetime.datetime.now().year)
            )

            return HTMLResponse(content=content)
        except Exception as e:
            logger.error(f"Failed to serve unregistered nodes page: {str(e)}")
            return HTMLResponse(content=f"<html><body>Error: {str(e)}</body></html>")

    async def serve_score_simulation_page(self):
        """Serve the score simulation HTML page"""
        try:
            with open("static/score-simulation.html", "r") as f:
                content = f.read()

            # Replace placeholders with actual values
            network = self.validator.config.SUBTENSOR_NETWORK.upper()
            content = content.replace("{{network}}", network)
            content = content.replace(
                "{{current_year}}", str(datetime.datetime.now().year)
            )

            return HTMLResponse(content=content)
        except Exception as e:
            logger.error(f"Failed to serve score simulation page: {str(e)}")
            return HTMLResponse(content=f"<html><body>Error: {str(e)}</body></html>")

    async def score_simulation_data(self):
        """Return JSON data for score simulation based on telemetry"""
        try:
            data = await self.validator.get_score_simulation_data()
            return data
        except Exception as e:
            logger.error(f"Failed to get score simulation data: {str(e)}")
            return {"error": str(e)}

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
                telemetry_dict = {
                    "hotkey": data.hotkey,
                    "uid": data.uid,
                    "timestamp": data.timestamp,
                    "boot_time": data.boot_time,
                    "last_operation_time": data.last_operation_time,
                    "current_time": data.current_time,
                    "twitter_auth_errors": data.twitter_auth_errors,
                    "twitter_errors": data.twitter_errors,
                    "twitter_ratelimit_errors": data.twitter_ratelimit_errors,
                    "twitter_returned_other": data.twitter_returned_other,
                    "twitter_returned_profiles": data.twitter_returned_profiles,
                    "twitter_returned_tweets": data.twitter_returned_tweets,
                    "twitter_scrapes": data.twitter_scrapes,
                    "web_errors": data.web_errors,
                    "web_success": data.web_success,
                    "tiktok_transcription_success": getattr(
                        data, "tiktok_transcription_success", 0
                    ),
                    "tiktok_transcription_errors": getattr(
                        data, "tiktok_transcription_errors", 0
                    ),
                    "worker_id": data.worker_id if hasattr(data, "worker_id") else None,
                }
                telemetry_dict_list.append(telemetry_dict)

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

            # Convert NodeData objects to dictionaries
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict = {
                    "hotkey": data.hotkey,
                    "uid": data.uid,
                    "timestamp": data.timestamp,
                    "boot_time": data.boot_time,
                    "last_operation_time": data.last_operation_time,
                    "current_time": data.current_time,
                    "twitter_auth_errors": data.twitter_auth_errors,
                    "twitter_errors": data.twitter_errors,
                    "twitter_ratelimit_errors": data.twitter_ratelimit_errors,
                    "twitter_returned_other": data.twitter_returned_other,
                    "twitter_returned_profiles": data.twitter_returned_profiles,
                    "twitter_returned_tweets": data.twitter_returned_tweets,
                    "twitter_scrapes": data.twitter_scrapes,
                    "web_errors": data.web_errors,
                    "web_success": data.web_success,
                    "tiktok_transcription_success": getattr(
                        data, "tiktok_transcription_success", 0
                    ),
                    "tiktok_transcription_errors": getattr(
                        data, "tiktok_transcription_errors", 0
                    ),
                    "worker_id": data.worker_id if hasattr(data, "worker_id") else None,
                }
                telemetry_dict_list.append(telemetry_dict)

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

            # Convert NodeData objects to dictionaries
            telemetry_dict_list = []
            for data in telemetry_data:
                telemetry_dict = {
                    "hotkey": data.hotkey,
                    "uid": data.uid,
                    "timestamp": data.timestamp,
                    "boot_time": data.boot_time,
                    "last_operation_time": data.last_operation_time,
                    "current_time": data.current_time,
                    "twitter_auth_errors": data.twitter_auth_errors,
                    "twitter_errors": data.twitter_errors,
                    "twitter_ratelimit_errors": data.twitter_ratelimit_errors,
                    "twitter_returned_other": data.twitter_returned_other,
                    "twitter_returned_profiles": data.twitter_returned_profiles,
                    "twitter_returned_tweets": data.twitter_returned_tweets,
                    "twitter_scrapes": data.twitter_scrapes,
                    "web_errors": data.web_errors,
                    "web_success": data.web_success,
                    "tiktok_transcription_success": getattr(
                        data, "tiktok_transcription_success", 0
                    ),
                    "tiktok_transcription_errors": getattr(
                        data, "tiktok_transcription_errors", 0
                    ),
                    "worker_id": data.worker_id if hasattr(data, "worker_id") else None,
                }
                telemetry_dict_list.append(telemetry_dict)

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
            from validator.weights import WeightsManager
            import time

            manager = PlatformManager()

            # Use the actual validator's WeightsManager (no mocking needed!)
            weights_manager = WeightsManager(self.validator)
            global_error_threshold = weights_manager.error_rate_threshold

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
                    "error_threshold": global_error_threshold,
                }

            return {
                "platforms": platforms,
                "total_platforms": len(platforms),
                "total_emission_weight": sum(
                    p["emission_weight"] for p in platforms.values()
                ),
                "global_error_threshold": global_error_threshold,
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
                        "error_threshold": weights_manager.error_rate_threshold,
                    },
                    "performance_metrics": {
                        "total_nodes": len(delta_telemetry),
                        "active_nodes": 0,
                        "total_success_operations": 0,
                        "total_errors": 0,
                        "success_rate": 0.0,
                        "nodes_above_threshold": 0,
                        "nodes_below_threshold": 0,
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

                    # Calculate error rate per hour for threshold check
                    time_span = getattr(node_data, "time_span_seconds", 3600) / 3600
                    error_rate = (
                        error_count / time_span if time_span > 0 else error_count
                    )

                    if error_rate <= weights_manager.error_rate_threshold:
                        performance_analysis[platform_name]["performance_metrics"][
                            "nodes_above_threshold"
                        ] += 1
                    else:
                        performance_analysis[platform_name]["performance_metrics"][
                            "nodes_below_threshold"
                        ] += 1

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
