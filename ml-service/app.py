"""
FastAPI adapter around CreditRiskScoringService.

Exposes internal-only scoring and health endpoints. The public React client
must never call this service directly; Spring Boot is the sole gateway.
"""
from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Project root must be on sys.path so `src.*` imports resolve.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.service.scoring_service import CreditRiskScoringService  # noqa: E402

logger = logging.getLogger("ml-service")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

MAX_BODY_BYTES = int(os.getenv("ML_MAX_BODY_BYTES", str(256 * 1024)))
INTERNAL_TOKEN = os.getenv("ML_SERVICE_TOKEN", "")
MODEL_PATH = os.getenv(
    "MODEL_PATH",
    str(ROOT / "outputs" / "models" / "credit_risk_model.joblib"),
)

_scoring_service: Optional[CreditRiskScoringService] = None


class ScoreRequest(BaseModel):
    applicant: Dict[str, Any] = Field(..., description="Raw Home Credit applicant fields")


def get_scoring_service() -> CreditRiskScoringService:
    if _scoring_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Scoring service is not ready",
        )
    return _scoring_service


def require_internal_token(
    x_internal_token: Optional[str] = Header(default=None, alias="X-Internal-Token"),
) -> None:
    if not INTERNAL_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service token is not configured",
        )
    if not x_internal_token or x_internal_token != INTERNAL_TOKEN:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scoring_service
    # Preserve an already-injected service (used by unit tests).
    if _scoring_service is None:
        logger.info("Loading CreditRiskScoringService")
        try:
            _scoring_service = CreditRiskScoringService(artifact_path=MODEL_PATH)
            logger.info("Model loaded successfully")
        except Exception:
            logger.exception("Failed to load model artifact")
            _scoring_service = None
    yield



app = FastAPI(
    title="Credit Risk ML Service",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > MAX_BODY_BYTES:
                return JSONResponse(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    content={"detail": "Request body too large"},
                )
        except ValueError:
            return JSONResponse(
                status_code=status.HTTP_400_BAD_REQUEST,
                content={"detail": "Invalid Content-Length"},
            )
    return await call_next(request)


@app.get("/health")
async def health():
    ready = _scoring_service is not None
    payload = {
        "status": "ok" if ready else "unavailable",
        "model_loaded": ready,
    }
    if not ready:
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=payload)
    return payload


@app.post("/score", dependencies=[Depends(require_internal_token)])
async def score(
    body: ScoreRequest,
    service: CreditRiskScoringService = Depends(get_scoring_service),
):
    if not isinstance(body.applicant, dict) or not body.applicant:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="applicant must be a non-empty object",
        )
    try:
        # Do not log applicant payloads — financial data is sensitive.
        result = service.score_applicant(body.applicant, include_explanations=True)
        return result
    except Exception:
        logger.exception("Scoring failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Scoring failed",
        )


def create_app_for_tests(
    service: Optional[CreditRiskScoringService] = None,
    token: str = "test-token",
) -> FastAPI:
    """Helper used by unit tests to inject a mock scoring service."""
    global _scoring_service, INTERNAL_TOKEN
    INTERNAL_TOKEN = token
    _scoring_service = service
    return app
