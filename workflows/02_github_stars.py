from prefect import flow, task
import httpx


@task(log_prints=True)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")

@task(name="get_repo_stars")
def github_stars(repos: list[str]):
    for repo in repos:
        get_stars(repo)

@flow(name="call-gh-stars")
def main():
    github_stars(["PrefectHQ/prefect"])

# run the flow!
if __name__ == "__main__":
    main.deploy(
        name="github_stars_server",
        cron="0/20 * * * *",
        work_pool_name="standard_local_dev_work_pool",
        concurrency_limit=1
        )
