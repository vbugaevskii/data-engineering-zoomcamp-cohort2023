from pathlib import Path
from prefect.filesystems import GitHub
from prefect import flow, task
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


@flow()
def main() -> None:
    clone_repository()

    # FIXME: Imports looks aweful
    from etl_flows.etl_web_to_gcs import etl_web_to_gcs
    etl_web_to_gcs(month=11, year=2020, color='green')


if __name__ == "__main__":
    main()