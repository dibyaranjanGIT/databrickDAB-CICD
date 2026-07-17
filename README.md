# cdc_pipeline — GitHub + Databricks Asset Bundle CI/CD

Repo structure:

```
databricks.yml                          bundle root config (dev/prod targets)
resources/jobs.yml                      the Workflow job definition (tiered CDC load + silver + gold)
src/notebooks/
  02_get_tier_tables.py                 orchestration: fetch one tier from control table
  03_generic_incremental_loader.py      orchestration: load one table (bronze)
  orchestrate_silver.py                 orchestration: bronze -> silver
  orchestrate_gold.py                   orchestration: silver -> gold star schema
src/transformations/
  customers.py, products.py,
  orders.py, payments.py,               all silver-layer transform functions
  dimensions.py, facts.py               all gold-layer dim/fact builders
sql/
  01_setup_source_and_control.sql       source_sim + pipeline_control setup
  05_create_silver_gold_schemas.sql     silver/gold schema creation
tests/test_customers.py                 pytest unit tests, no cluster needed
.github/workflows/deploy-dev.yml        CI/CD: test -> deploy on push to `dev`
```

## What changed from the flat workspace setup

- Notebook paths in `resources/jobs.yml` are now **relative** (`../src/notebooks/...`),
  not hardcoded `/Workspace/project_workflow/...`. `databricks bundle deploy`
  uploads the repo and rewrites these to the correct deployed path automatically.
- `orchestrate_silver.py` / `orchestrate_gold.py` resolve the `transformations`
  import path **dynamically** at runtime (from the notebook's own deployed
  path), instead of a hardcoded path — necessary because a bundle deploys to
  a different path per target (`dev`/`prod`) and per user.

## One-time setup

**1. Install the Databricks CLI locally** (to test before wiring up CI):
```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks --version   # need v0.218+ for bundle support
```

**2. Generate a Databricks personal access token**
Workspace → click your username (top right) → **Settings** → **Developer** →
**Access tokens** → **Generate new token**. Copy it now — it's shown once.

*(For production, use an OAuth service principal — `client_id`/`client_secret`
— instead of a PAT tied to your personal account. PAT is fine to start with.)*

**3. Validate and deploy manually once, to confirm it works**
```bash
export DATABRICKS_HOST="https://<your-workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="<the token from step 2>"

databricks bundle validate -t dev
databricks bundle deploy -t dev
```
If this succeeds, check Workflows in your workspace — you'll see a job named
something like `[dev yourname] cdc_incremental_load_tiered` (bundle dev mode
prefixes the job name so multiple people can deploy their own dev copies
without colliding).

**4. Push this repo to GitHub**
```bash
git init
git add .
git commit -m "Initial CDC pipeline bundle"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main

git checkout -b dev
git push -u origin dev
```

**5. Add GitHub secrets**
Repo → **Settings** → **Environments** → **New environment** → name it `dev`
(this matches `environment: dev` in the workflow file, and lets you optionally
require manual approval before deploy). Inside that environment, add:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

*(If you don't want to use Environments, add them under Settings → Secrets and
variables → Actions → Repository secrets instead — just drop `environment: dev`
from the workflow file.)*

## The CI/CD flow, end to end

Every push to `dev`:
1. **`test` job** — installs `pyspark`/`pytest`, runs `tests/test_customers.py`.
   If any test fails, the pipeline stops here — nothing gets deployed.
2. **`deploy` job`** (only runs if `test` passed) — installs the Databricks CLI,
   runs `databricks bundle validate -t dev` (catches YAML/schema errors before
   touching the workspace), then `databricks bundle deploy -t dev`, which
   uploads all notebooks/`.py` files and (re)creates/updates the Workflow job.

So the actual answer to "how do changes get to Databricks": you edit code
locally, commit, `git push origin dev` — GitHub Actions picks it up
automatically from there. No manual upload, ever.

## Extending to a real prod deploy

The `prod` target in `databricks.yml` is already stubbed in. To wire up a
second workflow (`.github/workflows/deploy-prod.yml`) triggered on push to
`main` instead of `dev`, copy `deploy-dev.yml` and change:
- `branches: [dev]` → `branches: [main]`
- `-t dev` → `-t prod` in both CLI calls
- point its secrets at a separate GitHub environment (`prod`) with its own
  `DATABRICKS_HOST`/`DATABRICKS_TOKEN` if prod lives in a different workspace
- consider adding `required reviewers` on the `prod` GitHub environment, so a
  human has to approve before anything deploys to production
