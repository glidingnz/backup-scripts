# Backup scripts
GNZ owns several sites, and these scripts are used to back each of them up.


## Retention Policy Strategy

This script follows the tiered retention strategy used by [spatie/laravel-backup](https://github.com/spatie/laravel-backup). 

### How it works

The policy evaluates each backup independently across several calendar-based periods. A single backup can fulfill multiple roles (e.g., the newest backup in a week is often also the newest backup for that specific day).

1.  **Keep-all**: All backups within the configured `RETENTION_KEEP_ALL_DAYS` window are kept.
2.  **Daily**: For each day in the `RETENTION_DAILY_DAYS` window, only the *newest* backup of that day is kept.
3.  **Weekly**: For each ISO week in the `RETENTION_WEEKLY_WEEKS` window, only the *newest* backup of that week is kept. 
    *   *Note*: In a standard daily backup routine, this naturally keeps the **Sunday** backup for each week.
4.  **Monthly**: For each month in the `RETENTION_MONTHLY_MONTHS` window, only the *newest* backup of that month is kept.
    *   *Note*: This naturally keeps the **last day of the month**.
5.  **Yearly**: For each year in the `RETENTION_YEARLY_YEARS` window, only the *newest* backup of that year is kept.

### Output Formatting

When running, the script outputs a **Retention Plan** that labels each backup with all the rules it satisfies. For example:
- `KEEP   2026-04-15T12-00-00Z (Keep-all period #1, Daily #1, Weekly #1, Monthly #1, Yearly #1)`
- `DELETE 2026-03-30T12-00-00Z (To be deleted)`

This transparency ensures you know exactly why each backup is being retained or pruned.

## Installation
1. Setup new Bucket in Backblaze B2
2. Create a new application key restricted to that bucket with at least `listFiles`, `writeFiles`, and `deleteFiles`
3. Do not enable a bucket lifecycle rule for backup expiry; `backup.py` now manages retention.
4. Run `./setup.sh` to create `.venv` and install Python dependencies
5. Setup `my.cnf` from `my.cnf.example`
6. Setup `.env` from `.env.example`
7. Add the `RETENTION_*` settings if you want to override the default Spatie-style retention windows
8. Setup cronjob to run `/path/to/backup-scripts/.venv/bin/python /path/to/backup-scripts/backup.py` every day

`--dry-run` is a true simulation: it prints the backup and retention actions it would take, but does not create archives, upload files, or delete remote backups.

## Python environment note

On current Debian and Ubuntu systems, `python3 -m pip install -r requirements.txt` may fail with an `externally-managed-environment` error. That is expected: the OS prevents `pip` from modifying the system Python.

Use the bootstrap script instead:

1. `cd /path/to/backup-scripts`
2. `./setup.sh`
3. `cp .env.example .env`
4. `cp my.cnf.example my.cnf`
5. `./.venv/bin/python backup.py --dry-run`

You do not need to activate the virtualenv in your shell or cron job; calling `.venv/bin/python` directly is enough.
The dry run still reads the remote backup list so it can preview retention pruning, so the B2 key must still have `listFiles`.

## Troubleshooting

### `externally-managed-environment`

Do not install into the system Python. Run `./setup.sh` and use `./.venv/bin/python`.

### `ensurepip is not available`

Install the virtualenv package, then rerun `./setup.sh`:

1. `sudo apt install python3-venv`
2. If your distro wants a versioned package instead, use `sudo apt install python3.12-venv`

### `Command '﻿python3' not found`

That usually means a hidden character was pasted into the shell before `python3` or `.venv/bin/python`. Retype the command manually, or run `./setup.sh` so you do not need to paste the longer setup commands.

### `PermissionError` reading `.env` or `my.cnf`

Do not run `backup.py` with `sudo`. Fix file ownership instead:

1. `sudo chown forge:forge .env my.cnf`
2. `chmod 600 .env my.cnf`
3. `./.venv/bin/python backup.py --dry-run`

If you use a different deploy user, replace `forge:forge` with that user and group.

### Backblaze `unauthorized`

The old write-only key setup is no longer enough. This script needs to upload new backups, list existing backups, and delete expired ones.

Create or update the Backblaze application key so it is restricted to the backup bucket and includes at least:

1. `listFiles`
2. `writeFiles`
3. `deleteFiles`

Then update `.env` with the new key ID and application key and rerun `./.venv/bin/python backup.py --dry-run`.

## Retention policy

By default, the script keeps:

1. All backups from the last 7 days
2. One backup per day from the last 16 days
3. One backup per week from the last 8 weeks
4. One backup per month from the last 4 months
5. One backup per year from the last 2 years

These windows overlap (all measured from now), so a recent backup can satisfy multiple rules simultaneously.

The script always keeps at least one backup set, even if every existing backup is older than the configured windows.

## Migration note

If this bucket previously used a 90-day Backblaze lifecycle rule, remove that rule in the Backblaze console before deploying the Python script. Running both retention systems together will cause unexpected deletions.
