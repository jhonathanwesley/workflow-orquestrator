from prefect import flow

@flow(name="flow_de_explicacao", log_prints=True)
def explain_flows():
    print("run any python code here!")
    print("encapsulate that business logic!")

if __name__ == "__main__":
    explain_flows.deploy(
        name="begginer_docs_flow",
        cron="0/3 * * * *",
        work_pool_name="standard_local_dev_work_pool",
        concurrency_limit=1
        )
