# Lab 8 – CI/CD DEV -> PROD 

## Flow

`bundle validate` runs twice on purpose: in the pull request, so a broken bundle never reaches `main`, and again in `deploy-prod`, so the exact commit that is deployed is validated once more.

## Bundle

Jobs, the pipeline, the dashboard (`src/dashboards/petroleum.lvdash.json`), schemas, the volume, the schedule and permissions are described in `databricks.yml` and `resources/*.yml`. The Python code is packaged as a wheel (`petroleum_transformations`, built with `uv build --wheel`) and installed by the job and the pipeline environment. There are no `sys.path` tricks.

## CI/CD

Two workflows in `.github/workflows/`:

- `pull_request.yml`: runs on every PR to `main`: `bundle validate -t prod`, `bundle plan -t prod`, unit tests with the coverage gate. The check `validate-and-test` is required by branch protection on `main`, so a red check blocks the merge.
- `deploy_prod.yml`: runs on push to `main`, and manually (optional `start_date` input for a backfill). Steps: set the wheel version, `bundle validate`, `bundle deploy -t prod`, run the ETL job.

### uv in CI

`uv` is used for the Python side of CI: it is faster than pip, resolves dependencies in one step, and builds the wheel (`uv build --wheel` is also the `build` command of the bundle artifact). In the pull request workflow the environment is created explicitly (`uv venv`, then `uv pip install ...`) and the tests are started with `.venv/bin/python`, so the step uses exactly the installed packages and nothing from the runner.

`uv run` is not used in CI. For local work the same checks are `uv sync --group dev` and `uv run python tests/run_tests.py . --threshold=95` (with `USE_LOCAL_SPARK=1`). Development here is done in the Databricks workspace, so this is optional.

### Tests

- **Unit tests** run on the GitHub runner with a local Spark (`USE_LOCAL_SPARK=1`), not on a Databricks cluster. The gate needs no workspace compute and no extra permissions. Coverage threshold: 95%.
- **Integration tests.** There are no automated integration tests against a separate environment, because there is no staging workspace. The integration level is covered by two things: `bundle plan -t prod` in every pull request (shows exactly what would change), and the real ETL job that `deploy-prod` runs right after the deploy, as a smoke test. If the job fails, the workflow is red. A dev job `petroleum-unit-tests` can run the same tests on Databricks serverless by hand.

## Versions: where they are pinned and why

| What | Where | Why |
|---|---|---|
| Databricks CLI `1.19.0` | `databricks/setup-cli` in both workflows | The behaviour of `bundle` changes between CLI versions. A pinned version makes CI repeatable. `latest` also failed to resolve once. |
| Wheel version `0.0.1+<sha7>` | `deploy_prod.yml` sets it in `pyproject.toml` with `sed` before the build | Serverless environments install the wheel with pip. If the version stays `0.0.1`, pip can consider it already installed and keep old code. A new local version per commit forces a fresh install and shows which commit runs in PROD. The version in the repository stays `0.0.1`, so there are no version-bump commits. |
| `pyspark>=4.0,<4.1`, Java 17 (Temurin) | `pull_request.yml` | The local Spark in the tests matches the Spark generation of the serverless environment, so the tests behave like the real run. |
| Serverless `environment_version: "3"` | job `environments` and tests job | Fixes the Python and library set the code runs on, so a platform update does not change it silently. |
| `actions/checkout@v4`, `setup-uv@v5`, `setup-java@v4` | workflows | Pinned to major versions to avoid breaking changes. `databricks/setup-cli` is referenced by branch; only the CLI it installs is pinned. Pinning the action itself to a tag or commit is a possible next step. |

## Rollback

Rollback is `git revert` of the bad commit on `main`, followed by a normal pull request and merge. `deploy-prod` then deploys the previous state exactly like any other change.

Why it is done in git and not in the workspace: `main` is the only source of truth for PROD. A manual fix in the workspace would be overwritten by the next deploy and would leave no trace in the history. A revert is reviewed by the same checks, is recorded in the history, and is reproducible. `bundle destroy` is never used against PROD.

## Authentication

GitHub Actions authenticates to Databricks with OAuth M2M: the GitHub environment `prod` holds `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` of the service principal `sp-databricks-adls`. In PROD every resource runs as this service principal.

Why not workload identity federation: it needs an account admin, which we do not have. Consequence: the OAuth secret has a lifetime and must be rotated (about every 180 days). Moving to federation later removes the secret.

## Why Databricks Asset Bundles and not Terraform

- **One tool for code and resources.** `bundle deploy` ships the wheel, notebooks and the dashboard file together with the job, pipeline, schemas, volume and permissions. With Terraform the code and the dashboard file would still need a second tool.
- **Environments built in.** `dev` and `prod` targets override only what differs; dev mode prefixes names, so a developer cannot overwrite PROD.
- **No state backend to run.** The deployment state lives in the workspace, there is no storage account to own and protect.
- **Where Terraform fits.** Platform setup (workspaces, catalogs, identities) is infrastructure. Terraform is a good next step for the manual PROD preparation (catalog privileges, secret scope access).
