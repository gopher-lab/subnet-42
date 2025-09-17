from fastapi import FastAPI
from miner.utils import (
    healthcheck,
    get_public_key,
    exchange_symmetric_key,
)
from fiber.logging_utils import get_logger

import os
from starlette.requests import Request
from starlette.responses import StreamingResponse
from starlette.background import BackgroundTask

import httpx

tee_address = os.getenv("MINER_TEE_ADDRESS")
client = httpx.AsyncClient(base_url=tee_address)

logger = get_logger(__name__)


class MinerAPI:
    def __init__(self, miner):
        self.miner = miner
        self.app = FastAPI()
        self.register_routes()

    def register_routes(self) -> None:
        self.app.add_api_route(
            "/healthcheck",
            self.healthcheck,
            methods=["GET"],
            tags=["healthcheck"],
        )

        self.app.add_api_route(
            "/public-encryption-key",
            get_public_key,
            methods=["GET"],
            tags=["encryption"],
        )

        self.app.add_api_route(
            "/exchange-symmetric-key",
            exchange_symmetric_key,
            methods=["POST"],
            tags=["encryption"],
        )

        self.app.add_api_route(
            "/tee",
            self.tee,
            methods=["GET"],
            tags=["tee address"],
        )

        self.app.add_api_route(
            "/get_information",
            self.information_handler,
            methods=["GET"],
            tags=["setup"],
        )

        self.app.add_api_route(
            "/job/{path:path}",
            self._reverse_proxy,
            methods=["GET", "POST", "PUT", "DELETE"],
            tags=["proxy"],
        )

        self.app.add_api_route(
            "/score-report",
            self.score_report_handler,
            methods=["POST"],
            tags=["scoring"],
        )

        self.app.add_api_route(
            "/custom-message",
            self.custom_message_handler,
            methods=["POST"],
            tags=["monitor"],
        )

    async def score_report_handler(self, request: Request):
        try:
            payload = await request.json()
            logger.info(
                f"\n\033[32m"
                f"====================================\n"
                f"       RECEIVED SCORE REPORT        \n"
                f"====================================\033[0m\n\n"
                f"  Validator UID: {payload['uid']}\n"
                f"  Validator Hotkey: {payload['hotkey']}\n"
                f"  Score: \033[33m{payload['score']:.4f}\033[0m\n"
            )

            # Display platform scores if available
            platform_scores_display = ""
            if "platform_scores" in payload:
                platform_scores_display = (
                    "\n\033[36m"
                    "====================================\n"
                    "        PLATFORM SCORE BREAKDOWN    \n"
                    "====================================\033[0m\n\n"
                )

                total_weighted_score = 0
                for platform, score_data in payload["platform_scores"].items():
                    score = score_data.get("score", 0)
                    weight = score_data.get("weight", 0)
                    weighted_score = score_data.get("weighted_score", 0)
                    total_weighted_score += weighted_score

                    platform_scores_display += (
                        f"  \033[33m{platform.upper()}\033[0m:\n"
                        f"    - Score: {score:.4f}\n"
                        f"    - Weight: {weight:.1%}\n"
                        f"    - Weighted Score: {weighted_score:.4f}\n\n"
                    )

                platform_scores_display += (
                    f"  \033[32mTotal Weighted Score: "
                    f"{total_weighted_score:.4f}\033[0m\n"
                )

            # Display platform metrics if available
            platform_metrics_display = ""
            if "platform_metrics" in payload:
                platform_metrics = payload["platform_metrics"]
                if platform_metrics:
                    platform_metrics_display = (
                        "\n\033[32m"
                        "====================================\n"
                        "       PLATFORM METRICS BREAKDOWN  \n"
                        "====================================\033[0m\n\n"
                    )

                    for platform, metrics in platform_metrics.items():
                        platform_metrics_display += (
                            f"  \033[33m{platform.upper()}\033[0m:\n"
                        )
                        for metric_name, metric_value in metrics.items():
                            platform_metrics_display += (
                                f"    - {metric_name}: {metric_value}\n"
                            )
                        platform_metrics_display += "\n"

            # Legacy telemetry display (for backward compatibility and timing info)
            telemetry = payload["telemetry"]
            formatted_telemetry = (
                f"  Timing:\n"
                f"    - Boot Time: {telemetry.get('boot_time', 'N/A')}\n"
                f"    - Last Operation: {telemetry.get('last_operation_time', 'N/A')}\n"
                f"    - Current Time: {telemetry.get('current_time', 'N/A')}\n"
            )

            # Display all information
            logger.info(platform_scores_display)
            logger.info(platform_metrics_display)

            if formatted_telemetry.strip():
                logger.info(
                    f"\n\033[32m"
                    f"====================================\n"
                    f"         TIMING INFORMATION         \n"
                    f"====================================\033[0m\n\n"
                    f"{formatted_telemetry}"
                )
            return {"status": "success"}
        except Exception as e:
            logger.error(f"\n\033[31mError processing score report: {str(e)}\033[0m")
            return {"status": "error", "message": str(e)}

    async def custom_message_handler(self, request: Request):
        try:
            payload = await request.json()
            message = payload.get("message", "No message provided")
            sender = payload.get("sender", "Unknown")

            logger.info(
                f"\n\033[36m"
                f"====================================\n"
                f"       CUSTOM MESSAGE RECEIVED      \n"
                f"====================================\033[0m\n\n"
                f"  From: {sender}\n"
                f"  Message: {message}\n"
            )

            return {"status": "success", "received": True}
        except Exception as e:
            logger.error(f"\n\033[31mError processing custom message: {str(e)}\033[0m")
            return {"status": "error", "message": str(e)}

    async def healthcheck(self, request: Request):
        return healthcheck(self.miner)

    async def information_handler(self, request: Request):
        return self.miner.information_handler()

    async def tee(self, request: Request):
        return tee_address

    async def _reverse_proxy(self, request: Request):
        url = httpx.URL(path=request.url.path, query=request.url.query.encode("utf-8"))
        rp_req = client.build_request(
            request.method,
            url,
            headers=request.headers.raw,
            content=await request.body(),
        )
        rp_resp = await client.send(rp_req, stream=True)
        return StreamingResponse(
            rp_resp.aiter_raw(),
            status_code=rp_resp.status_code,
            headers=rp_resp.headers,
            background=BackgroundTask(rp_resp.aclose),
        )
