# Lab 9 – Databricks automation with the Python SDK

## Files

- `cli.py` - command line entry point: `provision-and-run`, `run-job`, `status`, `cleanup`
- `check_gold_tables.py` - notebook that fails if a gold table is empty
- `automation/clusters.py` - create, wait for and delete single-node clusters
- `automation/jobs.py` - submit a notebook run, find and start a job
- `automation/runs.py` - poll a run, return its result, collect error details
- `automation/commands.py` - the four commands built from the modules above
- `automation/ci.py` - GitHub Actions values: run id, cluster tags, idempotency token
- `automation/reporting.py` - logs and the GitHub step summary

## Technical notes

- **SDK instead of raw REST or CLI.** Built-in auth, typed responses, fewer
  lines. The version is pinned (`0.147.0`) in the workflow so CI is reproducible.
- **`permanent_delete` instead of `delete`.** `delete` only terminates the
  cluster, and it stays in the list for 30 days.
- **Cleanup also on Ctrl+C / SIGTERM** (`BaseException`, signal handler).
  GitHub cancels a job with SIGTERM, and a cluster must not be left running.
- **Idempotency token** built from the CI run, attempt, step and parameters.
  A retried step gets the existing run back instead of starting a second one.
- **Permanent and transient errors are separated.** Not found or no access
  stops at once. Network and API errors are retried up to 3 times.
- **Local timeout = run timeout + 300 s.** Databricks stops the run first,
  so the real error is reported instead of a local timeout.
- **Run status is read from the task, not the run.** Failed tasks give the
  real error text, and INTERNAL_ERROR / SKIPPED are handled too.
- **All-purpose cluster for `provision-and-run`.** Easy to reuse and debug.
  Job compute (`new_cluster`) would be cheaper in production.
- **`cleanup` runs once, in a separate job** after all others, and only
  deletes clusters with tags `ManagedBy=lab9-cli` and the current `CiRun`.

## Known limits

- No automated tests, the code was checked by real runs in dev and prod.
- A signal during cluster deletion is not retried, autotermination (15 min)
  is the safety net.
