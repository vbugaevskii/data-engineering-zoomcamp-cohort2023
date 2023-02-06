from pathlib import Path

from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook
from prefect.context import get_run_context, FlowRunContext, TaskRunContext
from prefect.filesystems import GitHub

import re
import shutil

from tempfile import TemporaryDirectory


@task(log_prints=True)
def clone_repository() -> None:
    github = GitHub.load("github-connector")
    subdir = "cohorts/2023/week_2_workflow_orchestration/prefect-solution/flows"

    package_dir = Path("etl_flows")

    with TemporaryDirectory() as repo_dir:
        github.get_directory(subdir, repo_dir)
        if package_dir.exists():
            shutil.rmtree(package_dir, ignore_errors=True)
        shutil.move(Path(repo_dir) / subdir, package_dir)

    return package_dir


# NOTE: This shouldn't be task
def notify_slack(flow_ctx: FlowRunContext, status: str) -> None:
    message = f"""
    Flow status: {status}
    Flow run URL: http://127.0.0.1:4200/flow-runs/flow-run/{flow_ctx.flow_run.id}
    """
    message = re.sub(r'\n\s+', '\n', message.strip())

    slack_webhook_block = SlackWebhook.load("slack-alerts")
    slack_webhook_block.notify(message)


@flow(log_prints=True)
def etl_gcs_to_bq_github(month=11, year=2020, color='green') -> None:
    clone_repository()

    # FIXME: Imports looks aweful, don't do like this
    # See "etl_web_to_gcs_github_deploy.py"
    from etl_flows.etl_web_to_gcs import etl_web_to_gcs
    etl_web_to_gcs(month=month, year=year, color=color)

    notify_slack(get_run_context(), 'OK')


if __name__ == "__main__":
    etl_gcs_to_bq_github()