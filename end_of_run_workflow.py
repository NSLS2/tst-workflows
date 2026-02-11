from prefect import task, flow, get_run_logger
from data_validation import data_validation
from test_extra_client import get_other_docs
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
# from long_flow import long_flow


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


def slack(func):
    def end_of_run_workflow(stop_doc):
        logger = get_run_logger()
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")
        slack_webhook = SlackWebhook.load("mon-prefect")

        try:
            logger.info(f"Flow run info: {FlowRunContext.get().flow_run.dict()}")
            result = func(stop_doc)
            slack_webhook.notify(
                f":white_check_mark: Flow-run successful. (*{flow_run_name}*)"
            )
            return result
        except Exception:
            slack_webhook.notify(f":bangbang: Flow-run failed. (*{flow_run_name}*)")
            raise

    return end_of_run_workflow


@flow
@slack
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    # hello_world()
    data_validation(uid, return_state=True)
    get_other_docs(uid)
    # long_flow(iterations=100, sleep_length=10)
    log_completion()
