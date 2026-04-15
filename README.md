# Backup scripts
GNZ owns several sites, and these scripts are used to back each of them up.


## Installation
1. Setup new Bucket in Backblaze B2
2. Create new application key for that bucket, write only
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
The dry run still reads the remote backup list so it can preview retention pruning, but it does not write or upload anything.

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

## Retention policy

By default, the script keeps:

1. All backups for 7 days
2. One backup per day for the next 7 days
3. One backup per week for the next 8 weeks
4. One backup per month for the next 4 months
5. One backup per year for the next 2 years

The script always keeps at least one backup set, even if every existing backup is older than the configured windows.

## Migration note

If this bucket previously used a 90-day Backblaze lifecycle rule, remove that rule in the Backblaze console before deploying the Python script. Running both retention systems together will cause unexpected deletions.
