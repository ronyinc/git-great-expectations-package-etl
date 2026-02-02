import requests
import json
import pandas as pd
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
OWNER = "great-expectations"
REPO = "great_expectations"
REPO_FULL_NAME = f"{OWNER}/{REPO}"
PER_PAGE = 100

BASE_URL = "https://api.github.com"

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
            # data = response.json()
            data = [ x for x in response.json() if "pull_request" not in x]
            all_issues.extend(data)

            print(f"Fetched {len(data)} records. Total we have {len(all_issues)}")

            url = get_next_link(response.headers.get("Link"))
            # print(f"url link for the next page {url}")

            # url = response.links.get("next", {}).get("url") -- built in link parsing in the request module

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

        output_dir = "./landing-input"
        os.makedirs(output_dir, exist_ok=True)

        file_path_dim_repo = os.path.join(output_dir, f"github_dim_repo_{date.today()}.csv")
        df_repo.to_csv(file_path_dim_repo, index=False, encoding="utf-8")


    except requests.exceptions.RequestException as e:
        raise e 



def parse_issue_data_to_csv(data):

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
                # "assignee" : issue["assignees"],
                "assignee_count" : len(issue["assignees"]),
                # "labels" : issue["labels"],
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
                # "created_at" : item["created_at"],
                # "updated_at" : item["updated_at"],
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


        output_dir = "./landing-input"

        os.makedirs(output_dir, exist_ok=True)
        
        file_path_dim_user = os.path.join(output_dir, f"github_dim_user_{date.today()}.csv")
        file_path_issue_fact = os.path.join(output_dir, f"github_issue_fact_{date.today()}.csv")
        file_path_issue_label_dim = os.path.join(output_dir, f"github_issue_label_dim_{date.today()}.csv")
        file_path_issue_label_bridge = os.path.join(output_dir, f"github_issue_label_bridge_{date.today()}.csv")

        df_user.to_csv(file_path_dim_user, index=False, encoding="utf-8")
        df_issue_fact.to_csv(file_path_issue_fact, index=False, encoding="utf-8")
        df_issue_label.to_csv(file_path_issue_label_dim, index=False, encoding="utf-8")
        df_bridge_issue_label.to_csv(file_path_issue_label_bridge, index=False, encoding="utf-8")
        # print(df)

    except requests.exceptions.RequestException as e:
        raise e 


if __name__ == "__main__":

    # issues_list = fetch_all_issues(REPO, OWNER)

    # parse_issue_data_to_csv(issues_list)

    fetch_repo_info(REPO, OWNER)



    # print(issues_list[0]["labels"][0])
    
    # output_dir = Path(r"C:\pyspark\spark-apps\git-great-expectations-package-etl\landing-input")
    # output_dir.mkdir(parents=True, exist_ok=True)
    # output_file = output_dir / "github_issues.json"

    # with open(output_file, "w", encoding="utf-8") as f:
    #     json.dump(issues_list, f, indent=2, ensure_ascii=False)

    # print(f"JSON written to: {output_file}")    




