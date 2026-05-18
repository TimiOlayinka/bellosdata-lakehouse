"""
AWS Session Helper — Cloud-Compatible Authentication

Provides a unified boto3 session factory that works both:
  1. Locally (SSO profile: playEngineer)
  2. Cloud (IAM user env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

Usage:
    from aws_session import get_aws_session
    session = get_aws_session()
    s3 = session.client("s3", region_name="eu-west-2")

Author: Antigravity (DATA-29)
"""

import os
import boto3
import logging

logger = logging.getLogger(__name__)

AWS_REGION = "eu-west-2"


def get_aws_session() -> boto3.Session:
    """
    Create a boto3 session using the best available credentials.

    Priority:
    1. Environment variables (AWS_ACCESS_KEY_ID) — used on cloud instances
    2. SSO profile (playEngineer) — used on local dev machine
    3. Default credential chain — fallback
    """
    # Cloud: env vars are set by docker-compose
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        logger.debug("Using environment variable credentials (cloud mode)")
        return boto3.Session(region_name=AWS_REGION)

    # Local: try SSO profile
    try:
        session = boto3.Session(profile_name="playEngineer", region_name=AWS_REGION)
        # Validate the session works
        session.client("sts").get_caller_identity()
        logger.debug("Using SSO profile 'playEngineer' (local mode)")
        return session
    except Exception:
        pass

    # Fallback: default chain
    logger.debug("Using default credential chain")
    return boto3.Session(region_name=AWS_REGION)


def is_cloud() -> bool:
    """Check if running in cloud mode (env vars present, no local drives)."""
    return bool(os.environ.get("AWS_ACCESS_KEY_ID")) or not os.path.exists("D:\\")
