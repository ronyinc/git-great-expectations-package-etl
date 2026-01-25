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
                   "since" : "2025-01-01T00:00:00Z"
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

def parse_issue_data_to_csv(data):

    dim_user_list = []

    try: 
        for issue in data:
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

        df = pd.DataFrame(dim_user_list).drop_duplicates(subset=["user_id"])
        df["extracted_at_utc"] = datetime.now(timezone.utc).isoformat()

        output_dir = "./landing-input"

        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, f"github_dim_user_{date.today()}.csv")
        df.to_csv(file_path, index=False, encoding="utf-8")
        # print(df)

    except requests.exceptions.RequestException as e:
        raise e 


if __name__ == "__main__":
    issues_list = fetch_all_issues(REPO, OWNER)

    parse_issue_data_to_csv(issues_list)



    # print(issues_list[0]["labels"][0])
    
    # output_dir = Path(r"C:\pyspark\spark-apps\git-great-expectations-package-etl\landing-input")
    # output_dir.mkdir(parents=True, exist_ok=True)
    # output_file = output_dir / "github_issues.json"

    # with open(output_file, "w", encoding="utf-8") as f:
    #     json.dump(issues_list, f, indent=2, ensure_ascii=False)

    print(f"JSON written to: {output_file}")    




