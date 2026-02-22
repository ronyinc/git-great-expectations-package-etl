from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from pathlib import Path

import os
import pandas as pd
from sqlalchemy import create_engine, text, types as sqltypes

LANDING_DIR = Path("/opt/spark-apps/git-great-expectations-package-etl/landing-input")

def get_latest_file(prefix: str):
    files = sorted(LANDING_DIR.glob(f"{prefix}_*.csv"))

    if not files:
        raise FileNotFoundError(f"No files found for the prefix: {prefix} in the Landing area {LANDING_DIR}")
    return files[-1]
    

CSV_PATH_DIM_USER = get_latest_file("github_dim_user")
CSV_PATH_DIM_REPO = get_latest_file("github_dim_repo")
CSV_PATH_DIM_LABEL = get_latest_file("github_issue_label_dim")
CSV_PATH_BRIDGE_LABEL_ISSUE = get_latest_file("github_issue_label_bridge")
CSV_PATH_ISSUE_FACT = get_latest_file("github_issue_fact")

TARGET_SCHEMA = "staging"
TARGET_TABLE_USER = "dim_user_github_great_exp_package"
TARGET_TABLE_LABEL = "dim_label_github_great_exp_package"
TARGET_TABLE_REPO = "dim_repo_github_great_exp_package"
TARGET_TABLE_ISSUE_FACT = "fact_issue_github_great_exp_package"
TMP_TABLE = "github_great_exp_package_tmp"
CONN_ID = "pg_warehouse"

def get_engine():
    c = BaseHook.get_connection(CONN_ID)
    return create_engine(c.get_uri())

def load_dim_user():
    """Loading dim user table"""

    engine = get_engine()
    df = pd.read_csv(CSV_PATH_DIM_USER)

    dtype_map = {
        "user_id" : sqltypes.TEXT(),
        "type" : sqltypes.TEXT(),
        "login" : sqltypes.TEXT(),
        "node_id" : sqltypes.TEXT(),
        "site_admin" : sqltypes.BOOLEAN(),
        "avatar_url" : sqltypes.TEXT(),
        "url" : sqltypes.TEXT(),
        "html_url" : sqltypes.TEXT(),
        "followers_url" : sqltypes.TEXT(),
        "following_url" : sqltypes.TEXT(),
        "gists_url" : sqltypes.TEXT(),
        "starred_url" : sqltypes.TEXT(),
        "subscriptions_url" : sqltypes.TEXT(),
        "organizations_url" : sqltypes.TEXT(),
        "repos_url" : sqltypes.TEXT(),
        "events_url" : sqltypes.TEXT(),
        "received_events_url" : sqltypes.TEXT(),
        "user_view_type" : sqltypes.TEXT(),
        "extracted_at_utc" : sqltypes.TIMESTAMP(timezone=True)
    }

    df.to_sql(
        name=TMP_TABLE,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
        dtype=dtype_map,
    )

    """Step 1 : Insert all records, if records already exist then insert new records """

    insert_sql = f"""

    INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE_USER} (
        user_id, "type", login, node_id, site_admin, avatar_url, url, html_url, followers_url,
        following_url, gists_url, starred_url, subscriptions_url, organizations_url, repos_url,
        events_url, received_events_url, user_view_type, extracted_at_utc
    )

    SELECT 
          tmp.user_id, tmp."type", tmp.login, tmp.node_id, tmp.site_admin, tmp.avatar_url, tmp.url, tmp.html_url, tmp.followers_url,
          tmp.following_url, tmp.gists_url, tmp.starred_url, tmp.subscriptions_url, tmp.organizations_url, tmp.repos_url,
          tmp.events_url, tmp.received_events_url, tmp.user_view_type, tmp.extracted_at_utc     
    FROM 
          {TARGET_SCHEMA}.{TMP_TABLE} tmp 
    LEFT JOIN 
              {TARGET_SCHEMA}.{TARGET_TABLE_USER} trg
              on trg.user_id = tmp.user_id
    WHERE 
          trg.user_id is null
          ON CONFLICT (user_id) DO NOTHING;              

    """

    update_sql = f"""
    
    UPDATE {TARGET_SCHEMA}.{TARGET_TABLE_USER} trg 
    SET 
        
        "type" = tmp."type",
        login = tmp.login,
        node_id = tmp.node_id,
        site_admin = tmp.site_admin,
        avatar_url = tmp.avatar_url,
        url = tmp.url,
        html_url = tmp.html_url,
        followers_url = tmp.followers_url,
        following_url = tmp.following_url,
        gists_url = tmp.gists_url,
        starred_url = tmp.starred_url,
        subscriptions_url = tmp.subscriptions_url,
        organizations_url = tmp.organizations_url,
        repos_url = tmp.repos_url,
        events_url = tmp.events_url,
        received_events_url = tmp.received_events_url,
        user_view_type = tmp.user_view_type,
        extracted_at_utc = tmp.extracted_at_utc

    FROM {TARGET_SCHEMA}.{TMP_TABLE} tmp
    WHERE 
          trg.user_id = tmp.user_id
    AND 
         (
            trg."type" is distinct from tmp."type" or
            trg.login is distinct from tmp.login or
            trg.node_id is distinct from tmp.node_id or
            trg.site_admin is distinct from tmp.site_admin or
            trg.avatar_url is distinct from tmp.avatar_url or
            trg.url is distinct from tmp.url or
            trg.html_url is distinct from tmp.html_url or
            trg.followers_url is distinct from tmp.followers_url or
            trg.following_url is distinct from tmp.following_url or
            trg.gists_url is distinct from tmp.gists_url or
            trg.starred_url is distinct from tmp.starred_url or
            trg.subscriptions_url is distinct from tmp.subscriptions_url or
            trg.organizations_url is distinct from tmp.organizations_url or
            trg.repos_url is distinct from tmp.repos_url or
            trg.events_url is distinct from tmp.events_url or
            trg.received_events_url is distinct from tmp.received_events_url or
            trg.user_view_type is distinct from tmp.user_view_type or
            trg.extracted_at_utc is distinct from tmp.extracted_at_utc
                
         )         
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(update_sql))


def load_dim_label():
    """Loading dim user table"""

    engine = get_engine()
    df = pd.read_csv(CSV_PATH_DIM_LABEL)

    dtype_map = {
        "label_id" : sqltypes.TEXT(),
        "label_name" : sqltypes.TEXT(),
        "label_color" : sqltypes.TEXT(),
        "is_default" : sqltypes.BOOLEAN(),
        "label_description" : sqltypes.TEXT(),
        "extracted_at_utc" : sqltypes.TIMESTAMP(timezone=True)
    }

    df.to_sql(
        name=TMP_TABLE,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
        dtype=dtype_map,
    )

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE_LABEL} (
        label_id          TEXT PRIMARY KEY,
        label_name        TEXT,
        label_color       TEXT,
        is_default        BOOLEAN,
        label_description TEXT,
        extracted_at_utc  TIMESTAMPTZ NOT NULL

    );
    """

    truncate_sql = f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE_LABEL}; "

    """Insert all records for dim_label"""

    insert_sql_dim_label = f"""

    INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE_LABEL} (
        label_id, label_name, label_color, is_default, label_description, extracted_at_utc
    )

    SELECT 
          * 
    FROM 
          {TARGET_SCHEMA}.{TMP_TABLE} tmp ;

    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(truncate_sql))
        conn.execute(text(insert_sql_dim_label))


def load_dim_repo():
    """Loading dim repo table"""

    engine = get_engine()
    df = pd.read_csv(CSV_PATH_DIM_REPO)

    dtype_map = {
        "repo_id" : sqltypes.TEXT(),
        "repo_node_id" : sqltypes.TEXT(),
        "name" : sqltypes.TEXT(),
        "owner_user_id" : sqltypes.TEXT(),
        "private" : sqltypes.BOOLEAN(),
        "fork" : sqltypes.BOOLEAN(),
        "archived" : sqltypes.BOOLEAN(),
        "disabled" : sqltypes.BOOLEAN(),
        "created_at" : sqltypes.TIMESTAMP(timezone=True),
        "updated_at" : sqltypes.TIMESTAMP(timezone=True),
        "pushed_at" : sqltypes.TIMESTAMP(timezone=True),
        "default_branch" : sqltypes.TEXT(),
        "language" : sqltypes.TEXT(),
        "stargazers_count" : sqltypes.INTEGER(),
        "watchers_count" : sqltypes.INTEGER(),
        "forks_count" : sqltypes.INTEGER(),
        "open_issues_count" : sqltypes.INTEGER(),
        "extracted_at_utc" : sqltypes.TIMESTAMP(timezone=True)
    }

    df.to_sql(
        name=TMP_TABLE,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
        dtype=dtype_map,
    )
    """Step 1 & 2: Create table is not exists and also delete it """

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE_REPO} (
        repo_id            TEXT PRIMARY KEY,
        repo_node_id       TEXT,
        "name"             TEXT,
        owner_user_id      TEXT,
        private            BOOLEAN,
        fork               BOOLEAN,
        archived           BOOLEAN,
        disabled           BOOLEAN,
        created_at         TIMESTAMPTZ,
        updated_at         TIMESTAMPTZ,
        pushed_at          TIMESTAMPTZ,
        default_branch     TEXT,
        "language"         TEXT,
        stargazers_count   INTEGER,
        watchers_count     INTEGER,
        forks_count        INTEGER,
        open_issues_count  INTEGER,
        extracted_at_utc   TIMESTAMPTZ NOT NULL
    );
    """

    truncate_sql = f"TRUNCATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE_REPO};"


    """Step 3 : Insert all records, if records already exist then insert new records """

    insert_sql = f"""

    INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE_REPO} (
        repo_id, repo_node_id, "name", owner_user_id, private, fork, archived, disabled, created_at,
        updated_at, pushed_at, default_branch, "language", stargazers_count, watchers_count,
        forks_count, open_issues_count, extracted_at_utc
    )

    SELECT 
          tmp.*    
    FROM 
          {TARGET_SCHEMA}.{TMP_TABLE} tmp 
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(truncate_sql))
        conn.execute(text(insert_sql))      

def load_fact_issues():
    """Loading dim user table"""

    engine = get_engine()
    df = pd.read_csv(CSV_PATH_ISSUE_FACT)

    dtype_map = {
        "issue_id" : sqltypes.TEXT(),
        "issue_number" : sqltypes.INTEGER(),
        "repo_full_name" : sqltypes.TEXT(),
        "repository_url" : sqltypes.TEXT(),
        "title" : sqltypes.TEXT(),
        "user_id" : sqltypes.TEXT(),
        "state" : sqltypes.TEXT(),
        "locked" : sqltypes.BOOLEAN(),
        "assignee_count" : sqltypes.INTEGER(),
        "label_count" : sqltypes.INTEGER(),
        "milestone" : sqltypes.TEXT(),
        "comments" : sqltypes.INTEGER(),
        "created_at" : sqltypes.TIMESTAMP(timezone=True),
        "updated_at" : sqltypes.TIMESTAMP(timezone=True),
        "closed_at" : sqltypes.TIMESTAMP(timezone=True),
        "events_url" : sqltypes.TEXT(),
        "api_url" : sqltypes.TEXT(),
        "state_reason" : sqltypes.TEXT(),
        "extracted_at_utc" : sqltypes.TIMESTAMP(timezone=True)
    }

    df.to_sql(
        name=TMP_TABLE,
        schema=TARGET_SCHEMA,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
        dtype=dtype_map,
    )

    """Step 1 : Insert all records, if records already exist then insert new records """

    insert_sql = f"""

    INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE_ISSUE_FACT} (
        issue_id, issue_number, repo_full_name, repository_url, title, user_id, state, locked, assignee_count, label_count, milestone, comments, created_at, updated_at,
        closed_at, events_url, api_url, state_reason, extracted_at_utc
        )

    SELECT 
          tmp.issue_id, tmp.issue_number, tmp.repo_full_name, tmp.repository_url, tmp.title, tmp.user_id, tmp.state, tmp.locked, tmp.assignee_count, tmp.label_count,
          tmp.milestone, tmp.comments, tmp.created_at, tmp.updated_at, tmp.closed_at, tmp.events_url, tmp.api_url, tmp.state_reason, tmp.extracted_at_utc
    FROM 
          {TARGET_SCHEMA}.{TMP_TABLE} tmp 
    LEFT JOIN 
              {TARGET_SCHEMA}.{TARGET_TABLE_ISSUE_FACT} trg
              on trg.issue_id = tmp.issue_id
    WHERE 
          trg.issue_id is null
          ON CONFLICT (issue_id) DO NOTHING;

    """

    update_sql = f"""
    
    UPDATE {TARGET_SCHEMA}.{TARGET_TABLE_ISSUE_FACT} trg 
    SET 
        
   
        issue_number = tmp.issue_number,
        repo_full_name = tmp.repo_full_name,
        repository_url = tmp.repository_url,
        title = tmp.title,
        user_id = tmp.user_id,
        state = tmp.state,
        locked = tmp.locked,
        assignee_count = tmp.assignee_count,
        label_count = tmp.label_count,
        milestone = tmp.milestone,
        comments = tmp.comments,
        created_at = tmp.created_at,
        updated_at = tmp.updated_at,
        closed_at = tmp.closed_at,
        events_url = tmp.events_url,
        api_url = tmp.api_url,
        state_reason = tmp.state_reason,
        extracted_at_utc = tmp.extracted_at_utc

    FROM {TARGET_SCHEMA}.{TMP_TABLE} tmp
    WHERE 
          trg.issue_id = tmp.issue_id
    AND 
         (
            trg.issue_number is distinct from tmp.issue_number or
            trg.repo_full_name is distinct from tmp.repo_full_name or
            trg.repository_url is distinct from tmp.repository_url or
            trg.title is distinct from tmp.title or
            trg.user_id is distinct from tmp.user_id or
            trg.state is distinct from tmp.state or
            trg.locked is distinct from tmp.locked or
            trg.assignee_count is distinct from tmp.assignee_count or
            trg.label_count is distinct from tmp.label_count or
            trg.milestone is distinct from tmp.milestone or
            trg.comments is distinct from tmp.comments or
            trg.created_at is distinct from tmp.created_at or
            trg.updated_at is distinct from tmp.updated_at or
            trg.closed_at is distinct from tmp.closed_at or
            trg.events_url is distinct from tmp.events_url or
            trg.api_url is distinct from tmp.api_url or
            trg.state_reason is distinct from tmp.state_reason or
            trg.extracted_at_utc is distinct from tmp.extracted_at_utc          
         )         
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(update_sql))


def drop_tmp_table():

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_SCHEMA}.{TMP_TABLE};"))

with DAG(
    dag_id="github-great-expectations-package-api-etl-02",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["github-great-expectations-package","staging", "postgres", "differential", "two-step"],
) as dag:
    t1 = PythonOperator(task_id="create_dim_user", python_callable=load_dim_user)
    t2 = PythonOperator(task_id="create_dim_label", python_callable=load_dim_label)
    t3 = PythonOperator(task_id="create_dim_repo", python_callable=load_dim_repo)
    t4 = PythonOperator(task_id="create_fact_issues", python_callable=load_fact_issues)
    t5 = PythonOperator(task_id="delete_tmp_table", python_callable=drop_tmp_table)        

    t1 >> t2 >> t3 >> t4 >> t5




