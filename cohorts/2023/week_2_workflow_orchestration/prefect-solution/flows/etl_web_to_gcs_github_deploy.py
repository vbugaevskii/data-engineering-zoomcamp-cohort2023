import re
from pathlib import Path
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook
from prefect.context import get_run_context, FlowRunContext, TaskRunContext
from prefect.filesystems import GitHub
from prefect.deployments import Deployment, run_deployment
from etl_web_to_gcs import etl_web_to_gcs


deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     parameters={'month': 4, 'year': 2019, 'color': 'green'},
     name="etl_web_to_gcs_github_deploy",
     storage=GitHub.load("github-connector"),
     entrypoint="cohorts/2023/week_2_workflow_orchestration/prefect-solution/flows/etl_web_to_gcs.py:etl_web_to_gcs",
)


if __name__ == "__main__":
    deployment.apply()
    flow_run = run_deployment("etl-web-to-gcs/etl_web_to_gcs_github_deploy")
