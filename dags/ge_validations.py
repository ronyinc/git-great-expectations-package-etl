"""
ge_validations.py

Great Expectations (GE) data quality validations for the GitHub Great Expectations ETL project.

This module is designed to run inside Airflow (PythonOperator) and is compatible with
GE "Fluent" style contexts (FileDataContext + context.sources) as well as older contexts
that may expose context.data_sources.

What it does:
- Connects to Postgres via Airflow Connection (pg_warehouse)
- Ensures a GE datasource exists (create or update)
- Ensures expectation suites exist (dim_user_suite, fact_issue_suite)
- Validates:
    - dim_user table expectations
    - fact_issue table expectations
    - FK orphan check: fact_issue.user_id must exist in dim_user.user_id
- Writes full validation results to timestamped JSON files
- Fails the Airflow task if any validations fail
"""

import json
import logging
import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

import great_expectations as gx
from airflow.hooks.base import BaseHook

# ----------------------------
# Config
# ----------------------------
GE_ROOT = "/opt/airflow/great_expectations"
CONN_ID = "pg_warehouse"

TARGET_SCHEMA = "staging"
DIM_USER = "dim_user_github_great_exp_package"
FACT_ISSUE = "fact_issue_github_great_exp_package"

RESULTS_DIR = "/opt/airflow/ge_validation_results"

logger = logging.getLogger("airflow.task")


# ----------------------------
# Helpers
# ----------------------------
def _pg_uri() -> str:
    """Build SQLAlchemy URI from the Airflow connection."""
    c = BaseHook.get_connection(CONN_ID)
    return c.get_uri()


def _failed_expectations(validation_result: dict) -> list:
    """
    Extract only failed expectations from a GE validation result dict
    and return a compact list of strings for logging/errors.
    """
    failed = []
    for r in validation_result.get("results", []):
        if r.get("success") is False:
            exp_cfg = r.get("expectation_config", {})
            exp_type = exp_cfg.get("expectation_type", "unknown_expectation")
            kwargs = exp_cfg.get("kwargs", {})
            failed.append(f"{exp_type} {kwargs}")
    return failed


def _write_validation_json(name: str, result: dict) -> str:
    """
    Persist the full GE validation result to a timestamped JSON file.

    Returns the path so it can be logged and included in exceptions.
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(RESULTS_DIR, f"{name}_{ts}.json")

    with open(path, "w") as f:
        json.dump(result, f, indent=2, default=str)

    return path


def _get_sources(context):
    """
    GE compatibility helper:
    - Some GE versions expose context.data_sources
    - Fluent versions expose context.sources
    """
    sources = getattr(context, "data_sources", None)
    if sources is None:
        sources = context.sources
    return sources


def _get_or_create_datasource(context, ds_name: str):
    """
    Ensure datasource exists WITHOUT changing its type.

    If a datasource named ds_name already exists (e.g., type 'sql' from fluent config),
    we reuse it. Otherwise we create it using add_or_update_sql (most compatible).

    This prevents GE errors like:
    "Expected PostgresDatasource but got SQLDatasource"
    """
    sources = _get_sources(context)

    # 1) Reuse existing datasource if present
    # Some GE versions support sources.get()
    if hasattr(sources, "get"):
        try:
            return sources.get(ds_name)
        except Exception:
            pass

    # Some versions expose a dict-like container
    for attr in ("datasources", "data_sources", "all"):
        if hasattr(sources, attr):
            try:
                container = getattr(sources, attr)
                if isinstance(container, dict) and ds_name in container:
                    return container[ds_name]
            except Exception:
                pass

    # 2) Create/update as SQLDatasource (matches your fluent_datasources type: sql)
    if hasattr(sources, "add_or_update_sql"):
        return sources.add_or_update_sql(
            name=ds_name,
            connection_string=_pg_uri(),
        )

    # 3) Legacy fallback
    try:
        return sources.get(ds_name)
    except Exception:
        return sources.add_sql(
            name=ds_name,
            connection_string=_pg_uri(),
        )


def _get_or_add_table_asset(ds, asset_name: str, schema_name: str, table_name: str):
    """
    GE Fluent compatibility helper for table assets.

    Depending on GE version:
    - ds.get_asset(name) may exist
    - ds.assets may be dict-like
    - ds.add_table_asset(name=..., table_name=..., schema_name=...) creates it
    """
    # Try get_asset() if available
    if hasattr(ds, "get_asset"):
        try:
            return ds.get_asset(asset_name)
        except Exception:
            pass

    # Try ds.assets dict-like store
    if hasattr(ds, "assets"):
        try:
            if asset_name in ds.assets:
                return ds.assets[asset_name]
        except Exception:
            pass

    # Create asset
    return ds.add_table_asset(
        name=asset_name,
        table_name=table_name,
        schema_name=schema_name,
    )


def _ensure_suite(context, suite_name: str):
    """Create expectation suite if it doesn't exist."""
    try:
        context.get_expectation_suite(suite_name)
    except Exception:
        context.add_expectation_suite(suite_name)


# ----------------------------
# Main entrypoint for Airflow task
# ----------------------------
def run_ge_validations():
    """
    Main function to run GE validations.
    Use this as python_callable in an Airflow PythonOperator.
    """
    context = gx.get_context(context_root_dir=GE_ROOT)

    # Datasource (create/update)
    ds_name = "pg_warehouse_ds"
    ds = _get_or_create_datasource(context, ds_name)

    # ----------------------------
    # DIM USER VALIDATION
    # ----------------------------
    suite_user = "dim_user_suite"
    _ensure_suite(context, suite_user)

    asset_user_name = f"{TARGET_SCHEMA}.{DIM_USER}"
    asset_user = _get_or_add_table_asset(
        ds=ds,
        asset_name=asset_user_name,
        schema_name=TARGET_SCHEMA,
        table_name=DIM_USER,
    )

    v_user = context.get_validator(
        batch_request=asset_user.build_batch_request(),
        expectation_suite_name=suite_user,
    )

    # Expectations (dim_user)
    v_user.expect_table_row_count_to_be_between(min_value=1)
    v_user.expect_column_values_to_not_be_null("user_id", result_format={"result_format": "BASIC"})
    v_user.expect_column_values_to_be_unique("user_id", result_format={"result_format": "BASIC"})
    v_user.expect_column_values_to_be_in_set("type", ["User", "Organization"])
    v_user.expect_column_values_to_not_be_null("extracted_at_utc")

    v_user.save_expectation_suite(discard_failed_expectations=False)
    res_user = v_user.validate()

    # ----------------------------
    # FACT ISSUE VALIDATION
    # ----------------------------
    suite_issue = "fact_issue_suite"
    _ensure_suite(context, suite_issue)

    asset_issue_name = f"{TARGET_SCHEMA}.{FACT_ISSUE}"
    asset_issue = _get_or_add_table_asset(
        ds=ds,
        asset_name=asset_issue_name,
        schema_name=TARGET_SCHEMA,
        table_name=FACT_ISSUE,
    )

    v_issue = context.get_validator(
        batch_request=asset_issue.build_batch_request(),
        expectation_suite_name=suite_issue,
    )

    # Expectations (fact_issue)
    v_issue.expect_table_row_count_to_be_between(min_value=1)
    v_issue.expect_column_values_to_not_be_null("issue_id")
    v_issue.expect_column_values_to_be_unique("issue_id")
    v_issue.expect_column_values_to_be_between("issue_number", min_value=1)
    v_issue.expect_column_values_to_be_in_set("state", ["open", "closed"])
    v_issue.expect_column_values_to_be_between("assignee_count", min_value=0)
    v_issue.expect_column_values_to_be_between("label_count", min_value=0)
    v_issue.expect_column_values_to_not_be_null("extracted_at_utc")
    v_issue.save_expectation_suite(discard_failed_expectations=False)
    res_issue = v_issue.validate()

    # ----------------------------
    # FK orphan check (facts.user_id must exist in dim_user)
    # ----------------------------
    engine = create_engine(_pg_uri())
    fk_sql = f"""
    SELECT COUNT(*) AS cnt
    FROM {TARGET_SCHEMA}.{FACT_ISSUE} f
    LEFT JOIN {TARGET_SCHEMA}.{DIM_USER} u
      ON f.user_id = u.user_id
    WHERE f.user_id IS NOT NULL
      AND u.user_id IS NULL;
    """
    with engine.begin() as conn:
        orphan_cnt = conn.execute(text(fk_sql)).scalar()

    # ----------------------------
    # Persist results (debug artifacts)
    # ----------------------------
    user_path = _write_validation_json("dim_user", res_user)
    issue_path = _write_validation_json("fact_issue", res_issue)

    logger.info("GE results saved: dim_user=%s fact_issue=%s", user_path, issue_path)
    logger.info("FK orphan users count = %s", orphan_cnt)

    # ----------------------------
    # Fail task if any issues
    # ----------------------------
    failed_user = _failed_expectations(res_user)
    failed_issue = _failed_expectations(res_issue)

    if (not res_user.get("success", False)) or (not res_issue.get("success", False)) or (orphan_cnt > 0):

        logger.warning(
            "GE validation failed:\n"
            f"- dim_user success={res_user.get('success')} failed={failed_user}\n"
            f"- fact_issue success={res_issue.get('success')} failed={failed_issue}\n"
            f"- orphan_users={orphan_cnt}\n"
            f"- results_json: dim_user={user_path} fact_issue={issue_path}"
        )