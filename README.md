# Backup scripts
GNZ owns several sites, and these scripts are used to back each of them up.


## Installation
1. Setup new Bucket in Backblaze B2
2. Create new application key for that bucket, write only
3. Do not enable a bucket lifecycle rule for backup expiry; `backup.py` now manages retention.
4. Install Python dependencies with `python3 -m pip install -r requirements.txt`
5. Setup `my.cnf` from `my.cnf.example`
6. Setup `.env` from `.env.example`
7. Add the `RETENTION_*` settings if you want to override the default Spatie-style retention windows
8. Setup cronjob to run `python3 /path/to/backup.py` every day

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
