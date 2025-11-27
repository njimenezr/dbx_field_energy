import os
from typing import Optional

import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import User


def get_workspace_client() -> WorkspaceClient:
    w = WorkspaceClient()
    return w


def get_user() -> User:
    w = get_workspace_client()

    # Get the current user's details
    return w.current_user.me()


def get_targeted_env(key: str, default=None) -> Optional[str]:
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    targeted_key = f"{key}_{client_id}"

    result = os.getenv(targeted_key, os.getenv(key, default))
    # Return default if not found instead of asserting
    if result is None:
        return default
    return result


def get_context_username() -> str:
    try:
        return st.context.headers.get("X-Forwarded-Preferred-Username")
    except KeyError:
        return get_user().user_name
