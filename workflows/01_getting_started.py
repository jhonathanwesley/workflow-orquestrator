from prefect import flow, task
import random


@task(retries=3, retry_delay_seconds=5)
def get_customer_ids() -> list[str]:
    """Fetch customer ids from a database or API"""
    return [f"customer{n}" for n in random.choices(range(100), k=10)]

@task(retries=3, retry_delay_seconds=3)
def process_customer(customer_id: str) -> str:
    """Process a single customer"""
    return f"Processed {customer_id}"

@flow(name='Get-Started', retries=2, retry_delay_seconds=10)
def main(): # -> list[str]
    customer_ids = get_customer_ids()
    """Map the process_customer task across all customer IDs"""
    results = process_customer.map(customer_ids)
    return results


if __name__ == "__main__":
    main.deploy(
        name="random-numbers",
        cron="0/15 * * * *", # Run Every 15 minutes
        work_pool_name="standard_local_dev_work_pool",
        concurrency_limit=1
    )
