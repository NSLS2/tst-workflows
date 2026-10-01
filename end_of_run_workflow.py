import traceback

from data_validation import data_validation, get_run
from prefect import flow, get_run_logger, task
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
from test_extra_client import get_other_docs

CATEGORY_NAME = "tst"
SLACK_GENERAL = "mon-prefect"
SLACK_BLUESKY = "mon-bluesky"
SLACK_STATUS = "mon-prefect-tst"


def slack(func):
    def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
        logger = get_run_logger()
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")

        def _load_block(name):
            try:
                return SlackWebhook.load(name)
            except Exception:
                logger.exception(f"Failed to load Slack webhook block '{name}'")
                return None

        def _notify(webhook, message, description):
            if webhook is None:
                logger.warning(
                    f"Skipping {description} notification: webhook not available"
                )
                return
            try:
                webhook.notify(message)
            except Exception:
                logger.exception(f"Failed to send {description} notification")

        mon_prefect = _load_block(SLACK_GENERAL)
        mon_prefect_tst = _load_block(SLACK_STATUS)
        mon_bluesky = _load_block(SLACK_BLUESKY)

        uid = stop_doc.get("run_start", "unknown")
        scan_id = "unknown"
        try:
            run = get_run(uid, api_key=api_key)
            scan_id = run.start["scan_id"]
            if stop_doc.get("exit_status") == "fail":
                _notify(
                    mon_bluesky,
                    f":bangbang: {CATEGORY_NAME} bluesky-run failed. "
                    f"(*{flow_run_name}*)\n ```run_start: {uid}\n"
                    f"scan_id: {scan_id}``` ```reason: "
                    f"{stop_doc.get('reason', 'none')}```",
                    SLACK_BLUESKY,
                )
        except Exception:
            logger.exception(
                f"Exception while checking {uid}/{scan_id} for scan exit status"
            )

        try:
            result = func(stop_doc, api_key=api_key, dry_run=dry_run)
            _notify(
                mon_prefect_tst,
                f":white_check_mark: {CATEGORY_NAME} flow-run successful. "
                f"(*{flow_run_name}*)\n ```run_start: {uid}\n"
                f"scan_id: {scan_id}```",
                SLACK_STATUS,
            )
            return result
        except Exception as error:
            tb = traceback.format_exception_only(type(error), error)
            message = (
                f":bangbang: {CATEGORY_NAME} flow-run failed. "
                f"(*{flow_run_name}*)\n ```run_start: {uid}\n"
                f"scan_id: {scan_id}``` ```{tb[-1]}```"
            )
            _notify(mon_prefect_tst, message, SLACK_STATUS)
            _notify(mon_prefect, message, SLACK_GENERAL)
            raise

    return end_of_run_workflow


# from long_flow import long_flow


@task
def log_completion(dry_run=False):
    logger = get_run_logger()
    logger.info(f"Complete! dry_run: {dry_run}")


@flow
@slack
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    data_validation(uid, api_key=api_key)
    get_other_docs(uid, api_key=api_key)
    # long_flow(iterations=100, sleep_length=10)  # keep in to potentially run as a test in the future
    log_completion(dry_run=dry_run)
