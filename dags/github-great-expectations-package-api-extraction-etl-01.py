from __future__ import annotations

import requests
import json
import os
from pathlib import Path
import pandas as pd

from datetime import date, timedelta, datetime, timezone
from airflow.decorators import dag, task
from pathlib import Path

# --- Config -----

OWNER = "great-expectations"
REPO = "great_expectations"
REPO_FULL_NAME = f"{OWNER}/{REPO}"
PER_PAGE = 100
BASE_URL = "https://api.github.com"

API_KEY_ENV_NAME = "GITHUB_API_KEY"
API_KEY = os.getenv(API_KEY_ENV_NAME)


if not API_KEY:
    raise ValueError(f"Missing {API_KEY_ENV_NAME} env var")


LANDING_DIR = Path("/opt/spark-apps/git-great-expectations-package-etl/landing-input")
LANDING_DIR.mkdir(parents=True, exist_ok=True)



headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {API_KEY}",
    "X-GitHub-Api-Version": "2022-11-28"
}

def get_next_link(link_header: str | None) -> str | None:

    if not link_header:
        return None

    for part in link_header.split(','):
        if 'rel="next"' in part:
            url = part.split(';')[0].strip()
            return url.strip("<>")
        
    return None     


# Link header example : <https://api.github.com/repositories/103071520/issues?per_page=50&after=Y3Vyc29yOnYyOpLPAAABlY-K-wjOrd6C4Q%3D%3D&page=2>; rel="next"

def fetch_all_issues(repo, owner):

    try:
        url = f"{BASE_URL}/repos/{owner}/{repo}/issues"
        params = {
                   "per_page" : PER_PAGE,
                   "state" : "all",
                   "since" : "2024-01-01T00:00:00Z"
        }

        all_issues = []

        while url:
            response = requests.get(
                 url, 
                 params=params, 
                 headers=headers,
                 timeout=30
            )
            response.raise_for_status()
            data = [ x for x in response.json() if "pull_request" not in x]
            all_issues.extend(data)

            print(f"Fetched {len(data)} records. Total we have {len(all_issues)}")

            url = get_next_link(response.headers.get("Link"))

            params = None

        return all_issues    

    except requests.exceptions.RequestException as e:
        raise e    


def fetch_repo_info(repo, owner):

    try:
        url = f"{BASE_URL}/repos/{owner}/{repo}"
        repo_info = []


        response = requests.get(
                 url,
                 headers=headers,
                 timeout=30
            )

        response.raise_for_status()
        data = response.json()

        dim_repo = {
            "repo_id" : data["id"],
            "repo_node_id" : data["node_id"],
            "name" : data["full_name"],
            "owner_user_id" : data["owner"]["id"],
            "private" : data["private"],
            "fork" : data["fork"],
            "archived" : data["archived"],
            "disabled" : data["disabled"],
            "created_at" : data["created_at"],
            "updated_at" : data["updated_at"],
            "pushed_at" : data["pushed_at"],
            "default_branch" : data["default_branch"],
            "language" : data["language"],
            "stargazers_count" : data["stargazers_count"],
            "watchers_count" : data["watchers_count"],
            "forks_count" : data["forks_count"],
            "open_issues_count" : data["open_issues_count"]
        }

        repo_info.append(dim_repo)

        df_repo = pd.DataFrame(repo_info)
        df_repo["extracted_at_utc"] = datetime.now(timezone.utc).isoformat()

        return df_repo

    except requests.exceptions.RequestException as e:
        raise e 

def parse_issue_data_to_csv(data: list[dict]):

    dim_user_list = []
    label_info = []
    fact_issue = []
    bridge_table_lable_issue = []

    try: 
        for issue in data:                             # user dimension
            user_info = issue["user"]
            
            dim_user = {
                "user_id" : user_info["id"],
                "type" : user_info["type"],
                "login" : user_info["login"],
                "node_id" : user_info["node_id"],
                "type" : user_info["type"],
                "site_admin" : user_info["site_admin"],
                "avatar_url" : user_info["avatar_url"],
                "url" : user_info["url"],
                "html_url" : user_info["html_url"],
                "followers_url" : user_info["followers_url"],
                "following_url" : user_info["following_url"],
                "gists_url" : user_info["gists_url"],
                "starred_url" : user_info["starred_url"],
                "subscriptions_url" : user_info["subscriptions_url"],
                "organizations_url" : user_info["organizations_url"],
                "repos_url" : user_info["repos_url"],
                "events_url" : user_info["events_url"],
                "received_events_url" : user_info["received_events_url"],
                "type" : user_info["type"],
                "user_view_type" : user_info["user_view_type"],
                "site_admin" : user_info["site_admin"]}
            
            dim_user_list.append(dim_user)

        df_user = pd.DataFrame(dim_user_list).drop_duplicates(subset=["user_id"])
        df_user["extracted_at_utc"] = datetime.now(timezone.utc).isoformat()

        for issue in data:                           # fact issue 

            issue_fact = {
                "issue_id" : issue["id"],
                "issue_number" : issue["number"],
                "repo_full_name" : REPO_FULL_NAME,
                "repository_url" : issue["repository_url"],
                "title" : issue["title"],
                "user_id" : issue["user"]["id"],
                "state" : issue["state"],
                "locked" : issue["locked"],
                "assignee_count" : len(issue["assignees"]),
                "label_count" : len(issue["labels"]),
                "milestone" : issue["milestone"],
                "comments" : issue["comments"],
                "created_at" : issue["created_at"],
                "updated_at" : issue["updated_at"],
                "closed_at" : issue["closed_at"],
                "events_url" : issue["events_url"],
                "api_url" : issue["url"],
                "state_reason" : issue["state_reason"]

            }

            fact_issue.append(issue_fact)

        df_issue_fact = pd.DataFrame(fact_issue)
        df_issue_fact["extracted_at_utc"] = datetime.now(timezone.utc).isoformat()


        for issue in data:                         # label dimension
            issue_label = issue["labels"]
            

            for item in issue_label:
                dim_label = {
                "label_id" : item["id"],
                "label_name" : item["name"],
                "label_color" : item["color"],
                "is_default" : item["default"],
                "label_description" : item["description"]
                }
                label_info.append(dim_label)

        df_issue_label = pd.DataFrame(label_info).drop_duplicates(subset=["label_id"])
        df_issue_label["extracted_at_utc"] = datetime.now(timezone.utc).isoformat()


        for issue in data:                        # bridge table - issue label   

            label_issue_id = issue["id"]
            issue_label = issue.get("labels")     ## safe to do get() for external APIs

            for item in issue_label:
                bridge_table = {
                    "issue_id" : label_issue_id,
                    "label_id" : item["id"]
                }
                bridge_table_lable_issue.append(bridge_table)

        df_bridge_issue_label = pd.DataFrame(bridge_table_lable_issue).drop_duplicates(subset=["issue_id","label_id"])
        df_bridge_issue_label["extracted_at_utc"] =  datetime.now(timezone.utc).isoformat()        


        output_dir = "/opt/spark-apps/git-great-expectations-package-etl/landing-input"

        return df_user, df_issue_fact, df_issue_label, df_bridge_issue_label

    except requests.exceptions.RequestException as e:
        raise e 

@dag(
    dag_id="github-great-expectations-package-api-etl-01",
    description="Run github-great-expectations-package pipeline using existing functions, manual trigger.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["github-great-expectations-package", "api", "csv"]
)

def github_great_expectations_api_etl():

    @task
    def t_fetch_repo_info():
        df_repo = fetch_repo_info(REPO, OWNER)
        out = LANDING_DIR / f"github_dim_repo_{date.today().isoformat()}.csv"
        df_repo.to_csv(out, index=False, encoding="utf-8")
 
    @task
    def t_fetch_all_issues_to_json() -> str:
        issues = fetch_all_issues(REPO, OWNER)
        out = LANDING_DIR / f"github_issues_raw_{date.today().isoformat()}.json"
        out.write_text(json.dumps(issues), encoding="utf-8")
        return str(out)    # the path is storeded in XCom variable 

    @task
    def t_parse_issue_data_to_csv(raw_json_path: str) -> dict:
        data = json.loads(Path(raw_json_path).read_text(encoding="utf-8"))
        df_user, df_issue_fact, df_label, df_bridge = parse_issue_data_to_csv(data)

        paths = {
            "dim_user": str(LANDING_DIR / f"github_dim_user_{date.today().isoformat()}.csv"),
            "fact_issue": str(LANDING_DIR / f"github_issue_fact_{date.today().isoformat()}.csv"),
            "dim_label": str(LANDING_DIR / f"github_issue_label_dim_{date.today().isoformat()}.csv"),
            "bridge_issue_label": str(LANDING_DIR / f"github_issue_label_bridge_{date.today().isoformat()}.csv"),
        }

        df_user.to_csv(paths["dim_user"], index=False, encoding="utf-8")
        df_issue_fact.to_csv(paths["fact_issue"], index=False, encoding="utf-8")
        df_label.to_csv(paths["dim_label"], index=False, encoding="utf-8")
        df_bridge.to_csv(paths["bridge_issue_label"], index=False, encoding="utf-8")

        return paths

    repo_path = t_fetch_repo_info()
    raw_path = t_fetch_all_issues_to_json()
    extracted_paths = t_parse_issue_data_to_csv(raw_path)


github_great_expectations_api_etl()





# In this Airflow DAG, large API data is not passed directly between tasks; instead, it is written to disk and only the file path is shared using XCom.
# In the task t_fetch_all_issues_to_json, the GitHub issues are fetched as a Python list[dict], converted into a JSON string using json.dumps(), 
# and written to a file using Path.write_text().#  The task then returns the file path as a string, which Airflow automatically stores in XCom.
# 
# json.dumps()
# Python object → JSON string
# json.dump() Python object → write directly to file

# Python object → JSON string → write_text()


# The downstream task t_parse_issue_data_to_csv receives this path as an input parameter, reads the file back into memory using
# Path(raw_json_path).read_text(), and converts the JSON string back into Python objects using json.loads(). 

# JSON file on disk
#         ↓ read_text()
# JSON string
#         ↓ json.loads()
# Python list[dict]


# This pattern separates the data plane (files on disk containing large payloads) from the control plane 
# (small metadata like paths passed via XCom), which is a best practice in Airflow to avoid storing large datasets in the metadata database. 
# Conceptually, the flow becomes: API → raw JSON file → parsed DataFrames → structured CSV outputs, making the pipeline scalable, retry-safe,
# and easy to debug.



