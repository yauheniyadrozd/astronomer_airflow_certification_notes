# Apache Airflow Fundamentals — Complete Study Guide

A deep-dive companion to the official *Apache Airflow Fundamentals Certification Exam Guide*. This document expands each of the 15 official exam topics with explanations, code examples, and common "gotchas" that tend to show up on the exam.

> **Exam quick facts:** 75 multiple-choice questions · 60 minutes · 70% to pass (53/75) · question pool of 90+ items.

---

## Table of Contents
1. Airflow Use Cases
2. Dependencies
3. Operators
4. DAG Runs
5. DAG Scheduling
6. Debugging
7. XComs
8. Airflow CLI
9. Airflow Concepts (Architecture)
10. Best Practices
11. Connections
12. Tasks
13. Sensors
14. Variables
15. Airflow UI
16. Cron Cheat Sheet
17. Self-Test Practice Questions

---

## Topic 1: Airflow Use Cases

Airflow is a **workflow orchestrator**, not a data processing engine. It schedules, sequences, and monitors work — it does not move large volumes of data itself.

**Good fits for Airflow:**
- Batch ETL/ELT pipelines (extract → transform → load)
- Scheduled, repeatable jobs with dependencies between steps
- Coordinating work across multiple systems (databases, APIs, cloud storage, ML pipelines)
- Pipelines that need retries, alerting, and historical run tracking

**Poor fits for Airflow:**
- Real-time / streaming, sub-second latency processing (use Kafka, Flink, Spark Streaming instead)
- Pure data transformation at scale (Airflow should *trigger* Spark/dbt jobs, not do the heavy lifting in a task itself)
- Simple cron jobs with no dependency logic (often overkill)

**Exam tip:** When given a scenario, ask: *"Is this about scheduling/orchestrating discrete steps with dependencies?"* If yes → Airflow is suitable. If it's about continuous/streaming data → Airflow is not the right tool.

---

## Topic 2: Dependencies

Dependencies define the **order tasks execute in** within a DAG.

### Setting dependencies (bitshift operators)
```python
task_a >> task_b        # task_a runs before task_b
task_b << task_a        # same as above, written in reverse
task_a >> [task_b, task_c] >> task_d   # fan-out then fan-in
```

### Other ways to set dependencies
```python
task_a.set_downstream(task_b)
task_b.set_upstream(task_a)

from airflow.models.baseoperator import chain
chain(task_a, task_b, task_c)          # a >> b >> c

from airflow.models.baseoperator import cross_downstream
cross_downstream([task_a, task_b], [task_c, task_d])  # each of a,b -> each of c,d
```

### Key facts
- Dependencies are set **outside** the operator instantiation, typically at the bottom of the DAG file (or inline using `>>`/`<<`).
- Task-level dependencies control the **order and conditions** under which a downstream task runs (by default, a task only runs if all upstream tasks succeed — this is governed by `trigger_rule`).
- A DAG with a **cycle** (e.g., `a >> b` and `b >> a`) is invalid — DAG stands for *Directed **Acyclic** Graph*.

**Exam tip:** Expect "graph → code" matching questions. Diamond shapes (`a >> [b, c] >> d`) are a favorite pattern.

---

## Topic 3: Operators

An **Operator** is a template for a single, idempotent task. Once instantiated, an operator becomes a **task**.

|
 Operator Type 
|
 Purpose 
|
|
---
|
---
|
|
**
Action Operators
**
|
 Execute something (run code, call an API, run a SQL query). E.g., 
`PythonOperator`
, 
`BashOperator`
. 
|
|
**
Transfer Operators
**
|
 Move data from a source system to a destination system (e.g., 
`S3ToRedshiftOperator`
, 
`GCSToBigQueryOperator`
). 
|
|
**
Sensor Operators
**
|
 Wait for a condition to become true before letting downstream tasks run (e.g., a file to land, a partition to appear). They are a subclass of operators that "poke" or "wait." 
|

### PythonOperator
```python
from airflow.operators.python import PythonOperator

def my_function():
    print("Hello from Airflow")

run_python = PythonOperator(
    task_id="run_python",
    python_callable=my_function,
)
```
Its purpose is to execute **any arbitrary Python callable** as a task — the most flexible of the core operators.

**Exam tip:** Know the *category* a named operator belongs to (Transfer vs Sensor vs Action) even if you don't know the exact import path.

---

## Topic 4: DAG Runs

A **DAG Run** is a single execution instance of a DAG, tied to a specific **logical date / data interval**.

Key things that determine *how many* DAG runs occur:
- `start_date` — earliest date Airflow will consider scheduling from
- `end_date` — (optional) stops scheduling after this date
- `schedule_interval` (or `schedule` in newer versions) — the cadence
- `catchup` — whether Airflow creates runs for every interval missed between `start_date` and now, or only the most recent one

**Important nuance:** A scheduled run for interval `[start, end)` actually fires *after* `end` has passed — Airflow schedules at the **end** of the data interval, not the beginning. So a daily DAG with `start_date=2023-01-01` doesn't run on Jan 1; its first run fires on Jan 2 covering the Jan 1 interval.

**Exam tip:** These questions usually give you a `start_date`, a `schedule_interval`, today's date, and a `catchup` value, then ask "how many runs have happened?" Always count interval boundaries carefully, and remember the scheduler triggers at the *end* of an interval.

---

## Topic 5: DAG Scheduling

### Core scheduling parameters

|
 Parameter 
|
 Purpose 
|
 Default 
|
|
---
|
---
|
---
|
|
`start_date`
|
 The earliest logical date the DAG can be scheduled for 
|
 No default — must be provided (best practice: static, not 
`datetime.now()`
) 
|
|
`end_date`
|
 Optional date after which no new runs are scheduled 
|
`None`
|
|
`schedule_interval`
 / 
`schedule`
|
 How often the DAG runs (cron expression, preset, or 
`timedelta`
) 
|
`None`
 (manually triggered only, unless using presets) 
|
|
`catchup`
|
 Whether to backfill all missed intervals since 
`start_date`
|
`True`
|

### `schedule_interval` valid values
- **Cron expressions:** `"0 2 * * *"` (2 AM daily)
- **Presets:** `@once`, `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly`
- **`timedelta` objects:** `timedelta(days=1)`
- **`None`:** DAG is only triggered manually or externally

### catchup
- If `catchup=True` (the default), Airflow creates a DAG run for **every interval** between `start_date` and now that hasn't run yet.
- If `catchup=False`, Airflow only schedules the **most recent** interval going forward — it ignores the backlog.
- To intentionally backfill DAGs even when `catchup=False`, use the **CLI**: `airflow dags backfill -s <start_date> -e <end_date> <dag_id>`.

### Example: "Schedule a DAG every day at 2 PM"
```python
schedule_interval="0 14 * * *"
```

**Exam tip:** Be ready to translate plain-English goals ("every weekday at 9am," "every 15 minutes," "first day of the month") into cron syntax — see the Cron Cheat Sheet at the end of this guide.

---

## Topic 6: Debugging

Common issues the exam likes to test, using snippets of broken DAG code:

|
 Symptom 
|
 Likely Cause 
|
|
---
|
---
|
|
 DAG doesn't appear in UI 
|
 Syntax error in the file, DAG not in 
`DAGS_FOLDER`
, or scheduler hasn't parsed it yet (parsing runs on an interval) 
|
|
 Task stuck in 
`none`
/
`scheduled`
 state 
|
 No available worker slots, executor/queue misconfiguration, or upstream task not yet finished 
|
|
`DuplicateTaskIdFound`
|
 Two tasks in the same DAG sharing a 
`task_id`
|
|
 Tasks run in wrong / unexpected order 
|
 Dependency operators (
`>>`
, 
`<<`
) conflict or are set inconsistently (e.g., 
`task_1 >> task_2`
*
and
*
`task_2 << task_1`
 is redundant but not contradictory — watch for genuinely conflicting statements, which create a cycle) 
|
|
 Task never starts 
|
 Missing 
`start_date`
, or 
`start_date`
 is in the future 
|
|
`ImportError`
 on DAG parse 
|
 Tasks not properly assigned to a 
`dag=`
 object, or instantiated outside the 
`with DAG(...) as dag:`
 context manager 
|
|
 Operator instantiated but never runs 
|
 Operator created but never connected to the DAG (no 
`dag=`
 param and not inside the 
`with`
 block) 
|

**Exam tip:** When shown a code snippet, scan for: (1) missing `start_date`, (2) missing/duplicate `task_id`, (3) tasks not bound to the `dag` object, (4) contradictory or cyclic dependency statements.

---

## Topic 7: XComs

**XCom** ("cross-communication") lets tasks pass small pieces of data to one another.

### Methods
- **`xcom_push`** — explicitly stores a key/value pair from a task so other tasks can retrieve it. (Note: if a Python callable simply `return`s a value, Airflow automatically pushes it to XCom under the key `return_value`.)
- **`xcom_pull`** — retrieves a value pushed by another task, typically by specifying the source `task_ids` (and optionally a `key`).

```python
def push_value(**context):
    context["ti"].xcom_push(key="my_key", value=42)

def pull_value(**context):
    value = context["ti"].xcom_pull(task_ids="push_task", key="my_key")
    print(value)
```

### Limitations
- XComs are meant for **small** amounts of metadata (e.g., a file path, a row count, a status flag) — **not** for passing large datasets between tasks.
- They are stored in the **metadata database**, so large XComs bloat the DB and hurt performance.
- The default backend has a size limit (varies by backing database, but the guidance is always "keep XComs small").
- XComs are tied to a specific DAG run, task, and key — they aren't a general-purpose shared key-value store across DAGs (use **Variables** for that).

**Exam tip:** If a question describes passing megabytes of data between tasks, the "best practice" answer is almost always *"don't use XCom for this — use intermediate storage (e.g., S3, a database) and pass a reference/path via XCom instead."*

---

## Topic 8: Airflow CLI

|
 Command 
|
 Purpose 
|
|
---
|
---
|
|
`airflow tasks test <dag_id> <task_id> <date>`
|
 Run a single task 
**
without
**
 recording state in the metadata DB — great for isolated debugging (no dependency checks, no XCom persistence side effects logged the normal way). 
|
|
`airflow db init`
|
 Initialize the metadata database (create tables) for a fresh Airflow installation. 
|
|
`airflow info`
|
 Print diagnostic info about the Airflow environment (version, executor, backend, paths, etc.). 
|
|
`airflow config list`
|
 Display the current effective configuration (from 
`airflow.cfg`
, env vars, and defaults). 
|
|
`airflow cheat-sheet`
|
 Print a list of available CLI commands grouped by category. 
|
|
`airflow variables`
|
 Subcommands to list, get, set, import, export, or delete Airflow 
**
Variables
**
. 
|
|
`airflow users`
|
 Subcommands to create, list, delete, or manage Airflow UI user accounts/roles. 
|
|
`airflow standalone`
|
 Spin up a quick, all-in-one local Airflow instance (webserver + scheduler + triggerer + a default user) — handy for local testing. 
|
|
`airflow version`
|
 Print the installed Airflow version. 
|

**Exam tip:** Distinguish `airflow tasks test` (no state persisted, good for quick debugging) from running a task normally through the scheduler (full state tracking, retries, XCom persistence, etc.).

---

## Topic 9: Airflow Concepts (Architecture)

### Core architectural components
1. **Scheduler** — parses DAGs, decides when DAG runs should be created, and submits tasks that are ready to run to the **executor**. It does *not* execute the task code itself.
2. **Executor** — defines **how and where** tasks actually run (e.g., `LocalExecutor`, `CeleryExecutor`, `KubernetesExecutor`). It's a mechanism inside (or alongside) the scheduler.
3. **Worker(s)** — the process(es) that actually **execute task code**. With `CeleryExecutor`/`KubernetesExecutor`, workers can be distributed across machines/pods.
4. **Webserver** — serves the Airflow UI.
5. **Metadata Database** — stores DAG state, task instance state, connections, variables, XComs, users, etc. (e.g., Postgres, MySQL).
6. **Triggerer** *(newer versions)* — runs async "deferrable" operators/sensors efficiently.

### Other key facts
- **`DAGS_FOLDER`** — the folder the **scheduler parses** to discover new/updated DAG files (set via `airflow.cfg` or the `AIRFLOW__CORE__DAGS_FOLDER` env var).
- **Provider** — a separately installable Python package that bundles operators, hooks, sensors, and connections for a specific external system (e.g., `apache-airflow-providers-amazon`, `apache-airflow-providers-google`).
- **DAG Run** — one execution instance of a DAG for a specific logical/data interval.
- **DAG** — a *Directed Acyclic Graph*: the definition of a workflow as a set of tasks and their dependencies. A DAG describes *what* should run and in *what order*, not the specific data being processed.
- **XCom** — mechanism for small cross-task data exchange (see Topic 7).
- **`dag_id`** — a DAG's unique identifier. **If two DAGs share the same `dag_id`,** the scheduler treats the most recently parsed definition as authoritative — this typically leads to one DAG silently overwriting/conflicting with the other in the metadata DB, which is why `dag_id`s must be unique across the entire Airflow instance.
- **`default_args`** — a dictionary of arguments (e.g., `owner`, `retries`, `retry_delay`, `email_on_failure`) applied to **every task** in the DAG unless overridden at the task level. This avoids repeating the same kwargs on every operator.
- **Default time zone** — Airflow's internal scheduling and metadata database timestamps default to **UTC**, regardless of the host machine's local time zone.
- **Primary language** — Airflow is written in and configured via **Python** (DAGs are Python scripts).
- **Optional vs. required DAG parameters** — `start_date`/`dag_id` are essentially required to schedule meaningfully; things like `end_date`, `tags`, `description`, `catchup`, `max_active_runs` are optional with sensible defaults.
- **Valid ways to define a DAG:**
  ```python
  # 1. Context manager
  with DAG("my_dag", ...) as dag:
      ...

  # 2. Standard constructor
  dag = DAG("my_dag", ...)

  # 3. Decorator (TaskFlow API)
  from airflow.decorators import dag

  @dag(schedule="@daily", start_date=...)
  def my_dag():
      ...

  my_dag()
  ```

### The Task Lifecycle (typical journey of a task)
`no_status` → `scheduled` → `queued` → `running` → terminal state (`success`, `failed`, `up_for_retry`, `upstream_failed`, `skipped`)

- **`no_status`/`none`** — task instance created, not yet evaluated for scheduling.
- **`scheduled`** — the scheduler has determined the task is ready to be queued.
- **`queued`** — sent to the executor, waiting for a worker slot.
- **`running`** — actively executing on a worker.
- **`success` / `failed`** — terminal state once execution completes.
- **`up_for_retry`** — failed but retries remain, will be rescheduled.
- **`upstream_failed`** — an upstream dependency failed, so this task is skipped without running.
- **`skipped`** — deliberately bypassed (e.g., via a branching operator or trigger rule).

**Exam tip:** This topic is the broadest — expect several questions here. Make sure you can clearly separate the **roles** of scheduler vs. executor vs. worker; this distinction is one of the most commonly tested facts.

---

## Topic 10: Best Practices

- **Idempotency:** Tasks should produce the same result if re-run with the same inputs (critical for retries and backfills).
- **Avoid heavy logic at the top level of a DAG file:** Code outside of operators/callables runs **every time the scheduler parses the file** (very frequently) — keep top-level code lightweight (no API calls, no heavy computation at import time).
- **Use a static `start_date`,** not `datetime.now()` — a dynamic start date causes unpredictable/duplicate scheduling behavior.
- **Set `catchup=False`** unless you specifically need historical backfills, to avoid an unexpected flood of DAG runs.
- **Keep tasks atomic** — each task should do one logical unit of work so it can be retried independently without side effects.
- **Avoid large XComs** — pass references (file paths, IDs) instead of large payloads.
- **Use `default_args`** to DRY up retry/owner/alerting configuration across tasks.
- **Use Task Groups** (or sub-DAGs sparingly — Task Groups are preferred) to visually and logically organize related tasks.
- **Avoid tight coupling between DAGs** through brittle cross-DAG dependencies when possible; prefer clear, explicit dependency mechanisms (e.g., `ExternalTaskSensor`, datasets/asset scheduling) over implicit timing assumptions.
- **Don't store credentials in DAG code** — use **Connections** and **Variables** (ideally backed by a secrets backend) instead of hardcoding secrets.

**Exam tip:** "Given this DAG code, how would you improve it?" questions usually test 2–3 of the above points at once (e.g., dynamic `start_date` + heavy top-level API call + no `catchup` set).

---

## Topic 11: Connections

A **Connection** stores the information Airflow needs to talk to an external system (host, login, password, port, schema, extra JSON config).

### Ways to create a Connection
1. **Airflow UI** — Admin → Connections → "+"
2. **Airflow CLI** — `airflow connections add <conn_id> --conn-type ... --conn-host ...`
3. **Environment variable** — using the `AIRFLOW_CONN_<CONN_ID>` naming convention, with the value as a URI string
4. **`.env` file** *(in local/Astro CLI development contexts)* — defining connection environment variables that get loaded into the container's environment
5. **Secrets backend** — integrating with a secrets manager (AWS Secrets Manager, GCP Secret Manager, HashiCorp Vault, etc.) for production-grade secret storage

### Connection URI format
```
AIRFLOW_CONN_MY_CONN_ID='my-conn-type://login:password@host:port/schema'
```

**Identifying a connection ID:** Given a connection string like the one above, the **connection ID** is the identifier following the `AIRFLOW_CONN_` prefix, lower-cased (`MY_CONN_ID` → `my_conn_id`).

**Exam tip:** Know that environment-variable-defined connections always take the form `AIRFLOW_CONN_<CONN_ID_UPPERCASE>` and that the value is a URI.

---

## Topic 12: Tasks

A **task** is an instantiated operator — it represents one node in the DAG graph.

When asked "how many tasks will run," carefully:
1. Count every operator instantiation in the code (not just unique variable names — watch for operators created in a loop).
2. Check for tasks created dynamically (e.g., via a `for` loop generating one task per item in a list).
3. Watch for tasks that are instantiated but **never connected** to the DAG (`dag=` not set / not inside the `with DAG(...)` block) — these won't actually be scheduled, depending on context.
4. Remember that **Sensors and Transfer Operators are still tasks** and count toward the total.

**Exam tip:** Loop-generated tasks (`for table in tables: PythonOperator(task_id=f"process_{table}", ...)`) are a classic way the exam tests whether you can count tasks correctly — don't just count the lines of operator code, multiply by loop iterations.

---

## Topic 13: Sensors

A **Sensor** is a special operator that waits ("pokes") for a condition before allowing downstream tasks to proceed.

### Key facts
- **Default timeout:** `604800` seconds (**7 days**) if not explicitly set via the `timeout` parameter — after this, the sensor fails.
- **`poke_interval`:** how often (in seconds) the sensor checks the condition.
- **Mode parameter (`mode`):**
  - **`poke`** (default) — the sensor occupies a worker slot for its *entire* runtime, checking on each `poke_interval`. Best when `poke_interval` is **short** (frequent checks).
  - **`reschedule`** — the sensor releases its worker slot between checks and reschedules itself, freeing resources. Best when `poke_interval` is **long** (e.g., checking every 10+ minutes) since holding a slot the whole time would be wasteful.

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/data/incoming/file.csv",
    poke_interval=600,     # check every 10 minutes
    timeout=3600,          # give up after 1 hour
    mode="reschedule",     # long interval -> free the worker slot between checks
)
```

**Exam tip:** If `poke_interval` is described as "long" or "infrequent" in a scenario, the *correct* mode is `reschedule`. If it's short/frequent, `poke` is appropriate (and is the default).

---

## Topic 14: Variables

**Variables** are a generic key-value store in Airflow's metadata database, used for storing settings or configuration shared across DAGs/tasks (not tied to a single DAG run, unlike XComs).

### Key facts
- **Purpose:** Store static configuration values (e.g., an environment name, a file path, a feature flag) accessible from any DAG.
- **Data types supported:** Plain strings, or **JSON** (Airflow can deserialize a Variable's value into a dict/list automatically if stored as JSON).
- **Defining a variable:**
  - Via UI: Admin → Variables → "+"
  - Via CLI: `airflow variables set my_key my_value`
  - Via code (rare/discouraged for secrets): `Variable.set("my_key", "my_value")`
- **Fetching a variable:**
  ```python
  from airflow.models import Variable

  value = Variable.get("my_key")
  config_dict = Variable.get("my_json_key", deserialize_json=True)
  ```
- **Masking in the UI:** Any variable whose **key name** contains a sensitive substring (e.g., `password`, `secret`, `token`, `api_key`) is automatically **masked/hidden** in the Airflow UI, regardless of what value is stored — this is a key exam point.

**Exam tip:** "Given a variable name, will it be visible in the UI?" — the answer hinges entirely on whether the **key** contains a flagged keyword like "password" or "secret," not on the actual content of the value.

---

## Topic 15: Airflow UI

### Key views and when to use them
|
 View 
|
 Best Used For 
|
|
---
|
---
|
|
**
Grid View
**
 (formerly Tree View) 
|
 Seeing the historical status of every DAG run and task instance at a glance — best for spotting patterns/trends over time. 
|
|
**
Graph View
**
|
 Visualizing the dependency structure of a 
*
single
*
 DAG run — best for understanding task order and current run status. 
|
|
**
Gantt Chart
**
|
 Analyzing task 
**
duration
**
 and overlap — best for identifying bottlenecks or long-running tasks. 
|
|
**
Calendar View
**
|
 Seeing DAG run success/failure at a glance across a calendar timeframe. 
|
|
**
Code View
**
|
 Viewing the actual Python source of the DAG file directly in the UI. 
|

### Other UI facts
- **Default time for new DAGs to appear:** the scheduler parses the `DAGS_FOLDER` on a recurring interval (default around every 30 seconds, configurable via `min_file_process_interval` / `dag_dir_list_interval`), so a brand-new DAG file isn't instant — there's a short delay before it's parsed and shown.
- **Deleting a DAG via the UI:** removes the DAG's **metadata** (DAG runs, task instance history, etc.) from the database — it does **not** delete the underlying `.py` file. If the file still exists in `DAGS_FOLDER`, the DAG will simply reappear (with fresh history) the next time the scheduler parses it.
- **"Last Run" column:** shows the most recent DAG run's logical date/time and status at a glance from the DAGs list page — useful for quickly checking whether the latest run succeeded without drilling in.

**Exam tip:** "Given a scenario, which view should you check?" questions map directly to the table above — duration issues → Gantt; dependency/order issues → Graph; historical trend issues → Grid.

---

## Topic 16: Cron Cheat Sheet

Format: `minute hour day-of-month month day-of-week`

|
 Goal 
|
 Cron 
|
|
---
|
---
|
|
 Every minute 
|
`* * * * *`
|
|
 Every hour, on the hour 
|
`0 * * * *`
|
|
 Every day at 2:00 AM 
|
`0 2 * * *`
|
|
 Every day at 2:00 PM 
|
`0 14 * * *`
|
|
 Every 2 hours 
|
`0 */2 * * *`
|
|
 Every 2 hours, weekends only 
|
`0 */2 * * 6,0`
 (day-of-week: 0 and 6 = Sunday and Saturday) 
|
|
 Every weekday (Mon–Fri) at 9 AM 
|
`0 9 * * 1-5`
|
|
 Every Monday at midnight 
|
`0 0 * * 1`
|
|
 First day of every month at midnight 
|
`0 0 1 * *`
|
|
 Every 15 minutes 
|
`*/15 * * * *`
|

**Day-of-week numbering:** `0` and `7` both mean Sunday; `1`=Monday … `6`=Saturday.

---

## Topic 17: Self-Test Practice Questions

Try these on your own, then check the answer key below.

**Q1.** Which Airflow component is responsible for actually executing a task's code?
a) Scheduler  b) Executor  c) Worker  d) Webserver

**Q2.** A DAG has `start_date=datetime(2024,1,1)`, `schedule="@daily"`, and `catchup=True`. If today is `2024,1,5` and the DAG has never run, how many DAG runs will the scheduler create?
a) 1  b) 4  c) 5  d) 0

**Q3.** Which sensor `mode` should you use if a sensor checks a condition only once every 30 minutes, to avoid wasting a worker slot?
a) `poke`  b) `reschedule`  c) `async`  d) `defer`

**Q4.** What happens if two separate DAG files both define a DAG with `dag_id="daily_etl"`?
a) Airflow throws a startup error and refuses to parse either file
b) Both DAGs run independently with separate histories
c) The scheduler treats the most recently parsed definition as authoritative, causing conflicts
d) Airflow automatically renames one of them

**Q5.** A variable is created with the key `db_password`. Will its value be visible in the Airflow UI?
a) Yes, all variable values are visible
b) No, because the key contains a sensitive keyword and will be masked
c) Only if it's stored as JSON
d) Only admins can ever see any variable

**Q6.** What is the default XCom backend's intended use case?
a) Passing multi-gigabyte datasets between tasks
b) Passing small pieces of metadata (IDs, flags, paths) between tasks
c) Long-term configuration storage across DAGs
d) Storing user credentials

**Q7.** Which CLI command lets you test a single task without persisting its state to the metadata database?
a) `airflow dags trigger`  b) `airflow tasks run`  c) `airflow tasks test`  d) `airflow tasks state`

### Answer Key
1. **c** — Workers execute task code; the executor decides *how/where*, the scheduler decides *when*.
2. **c** — 5 runs (Jan 1, 2, 3, 4, 5 intervals all backfilled since `catchup=True`, assuming the daily schedule has had 5 opportunities to fire by Jan 5).
3. **b** — `reschedule` frees the worker slot between infrequent checks.
4. **c** — duplicate `dag_id`s cause the most recently parsed version to take precedence, leading to conflicts.
5. **b** — keys containing "password," "secret," etc. are auto-masked regardless of value.
6. **b** — XComs are for small metadata, not bulk data transfer.
7. **c** — `airflow tasks test` runs a task in isolation without recording state.

---

## Final Study Checklist

- [ ] I can name and describe the role of each core architectural component (scheduler, executor, worker, webserver, metadata DB).
- [ ] I can read a dependency graph and write the matching `>>`/`<<` code (and vice versa).
- [ ] I can calculate the number of DAG runs given `start_date`, `schedule_interval`, `catchup`, and a "today" date.
- [ ] I can translate a plain-English schedule goal into a cron expression.
- [ ] I know the default timeout (7 days) and the difference between `poke` and `reschedule` sensor modes.
- [ ] I know XComs are for small data only, and how `xcom_push`/`xcom_pull` work.
- [ ] I can spot common DAG bugs: missing `start_date`, duplicate `task_id`, tasks not bound to the DAG, cyclic dependencies.
- [ ] I know the purpose of each major Airflow CLI command listed in Topic 8.
- [ ] I know the different ways to create a Connection, and how to read a connection URI.
- [ ] I know which Airflow UI view to use for a given debugging scenario.
- [ ] I've reviewed Airflow best practices (idempotency, static `start_date`, lightweight top-level code, atomic tasks).
