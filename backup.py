#!/usr/bin/env python3
"""Backup script: dumps DB and archives files, uploads to Backblaze B2,
then enforces a tiered retention policy on existing backups."""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class RemoteFile:
    name: str
    file_id: str


@dataclass
class RetentionPolicy:
    keep_all_days: int = 7
    daily_days: int = 7
    weekly_weeks: int = 8
    monthly_months: int = 4
    yearly_years: int = 2


# ---------------------------------------------------------------------------
# Adapter protocols
# ---------------------------------------------------------------------------

class B2Adapter(Protocol):
    def upload(self, local_path: Path, remote_name: str) -> None: ...
    def list_files(self) -> list[RemoteFile]: ...
    def delete(self, file: RemoteFile) -> None: ...


class SystemAdapter(Protocol):
    def dump_db(self, my_cnf: Path, db_name: str, dest: Path) -> None: ...
    def archive_files(self, source: Path, dest: Path) -> None: ...


# ---------------------------------------------------------------------------
# Concrete adapters
# ---------------------------------------------------------------------------

class B2SdkAdapter:
    def __init__(self, key_id: str, app_key: str, bucket_name: str) -> None:
        from b2sdk.v2 import B2Api, InMemoryAccountInfo
        self._api = B2Api(InMemoryAccountInfo())
        self._api.authorize_account("production", key_id, app_key)
        self._bucket = self._api.get_bucket_by_name(bucket_name)

    def upload(self, local_path: Path, remote_name: str) -> None:
        self._bucket.upload_local_file(local_file=str(local_path), file_name=remote_name)

    def list_files(self) -> list[RemoteFile]:
        files = []
        for fv, folder in self._bucket.ls(latest_only=False, recursive=True):
            if folder is None:
                files.append(RemoteFile(name=fv.file_name, file_id=fv.id_))
        return files

    def delete(self, file: RemoteFile) -> None:
        self._api.delete_file_version(file_id=file.file_id, file_name=file.name)


class SubprocessAdapter:
    def dump_db(self, my_cnf: Path, db_name: str, dest: Path) -> None:
        with open(dest, 'wb') as out_file:
            dump = subprocess.Popen(
                ['mysqldump', f'--defaults-extra-file={my_cnf}', '--single-transaction', db_name],
                stdout=subprocess.PIPE,
            )
            gzip = subprocess.Popen(['gzip'], stdin=dump.stdout, stdout=out_file)
            dump.stdout.close()  # Allow dump to receive SIGPIPE if gzip exits early
            gzip.wait()
            dump.wait()
        if dump.returncode != 0:
            raise subprocess.CalledProcessError(dump.returncode, 'mysqldump')
        if gzip.returncode != 0:
            raise subprocess.CalledProcessError(gzip.returncode, 'gzip')

    def archive_files(self, source: Path, dest: Path) -> None:
        subprocess.run(
            ['tar', '--exclude=.git', '-czf', str(dest), str(source)],
            check=True,
        )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

BACKUP_FILE_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)-(db\.sql\.gz|files\.tar\.gz)$'
)
STEM_TIME_FMT = '%Y-%m-%dT%H-%M-%SZ'


def parse_stem_time(stem: str) -> datetime:
    return datetime.strptime(stem, STEM_TIME_FMT).replace(tzinfo=timezone.utc)


def make_stem() -> str:
    return datetime.now(tz=timezone.utc).strftime(STEM_TIME_FMT)


def group_by_stem(files: list[RemoteFile]) -> dict[str, list[RemoteFile]]:
    stems: dict[str, list[RemoteFile]] = {}
    for f in files:
        m = BACKUP_FILE_RE.match(f.name)
        if m:
            stems.setdefault(m.group(1), []).append(f)
    return stems


def load_env(env_path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    with open(env_path) as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, value = line.partition('=')
                env[key.strip()] = value.strip()
    return env


def parse_policy(env: dict[str, str]) -> RetentionPolicy:
    return RetentionPolicy(
        keep_all_days=int(env.get('RETENTION_KEEP_ALL_DAYS', 7)),
        daily_days=int(env.get('RETENTION_DAILY_DAYS', 7)),
        weekly_weeks=int(env.get('RETENTION_WEEKLY_WEEKS', 8)),
        monthly_months=int(env.get('RETENTION_MONTHLY_MONTHS', 4)),
        yearly_years=int(env.get('RETENTION_YEARLY_YEARS', 2)),
    )


# ---------------------------------------------------------------------------
# Pure retention logic (no I/O)
# ---------------------------------------------------------------------------

def stems_to_delete(
    stems: dict[str, list[RemoteFile]],
    now: datetime,
    policy: RetentionPolicy,
) -> set[str]:
    """Return the set of stems that should be deleted according to the policy.

    Stems are sorted newest-first so the first backup seen in each period
    bucket is the most recent one — it gets kept, all older ones in the same
    bucket are deleted.
    """
    keep_all_cutoff = now - timedelta(days=policy.keep_all_days)
    daily_cutoff = keep_all_cutoff - timedelta(days=policy.daily_days)
    weekly_cutoff = daily_cutoff - timedelta(weeks=policy.weekly_weeks)
    monthly_cutoff = weekly_cutoff - timedelta(days=policy.monthly_months * 30)
    yearly_cutoff = monthly_cutoff - timedelta(days=policy.yearly_years * 365)

    sorted_stems = sorted(stems, key=parse_stem_time, reverse=True)

    seen_days: set = set()
    seen_weeks: set = set()
    seen_months: set = set()
    seen_years: set = set()
    keep: set[str] = set()

    for stem in sorted_stems:
        t = parse_stem_time(stem)
        if t >= keep_all_cutoff:
            keep.add(stem)
        elif t >= daily_cutoff:
            key = t.date()
            if key not in seen_days:
                seen_days.add(key)
                keep.add(stem)
        elif t >= weekly_cutoff:
            key = t.isocalendar()[:2]
            if key not in seen_weeks:
                seen_weeks.add(key)
                keep.add(stem)
        elif t >= monthly_cutoff:
            key = (t.year, t.month)
            if key not in seen_months:
                seen_months.add(key)
                keep.add(stem)
        elif t >= yearly_cutoff:
            key = t.year
            if key not in seen_years:
                seen_years.add(key)
                keep.add(stem)
        # else: older than all periods — will be deleted

    if sorted_stems and not keep:
        keep.add(sorted_stems[0])

    return set(stems.keys()) - keep


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_backup(
    b2: B2Adapter,
    system: SystemAdapter,
    env: dict[str, str],
    script_dir: Path,
    dry_run: bool = False,
) -> None:
    stem = make_stem()
    db_name = f'{stem}-db.sql.gz'
    files_name = f'{stem}-files.tar.gz'

    backup_dest = Path(env['BACKUP_DEST'])
    backup_dest.mkdir(parents=True, exist_ok=True)
    db_path = backup_dest / db_name
    files_path = backup_dest / files_name

    try:
        print(f'Dumping DB ({env["MYSQL_DB"]})')
        system.dump_db(
            my_cnf=script_dir / 'my.cnf',
            db_name=env['MYSQL_DB'],
            dest=db_path,
        )

        print(f'Archiving files from {env["BACKUP_SOURCE"]}')
        system.archive_files(source=Path(env['BACKUP_SOURCE']), dest=files_path)

        print(f'Uploading {db_name}')
        b2.upload(db_path, db_name)

        print(f'Uploading {files_name}')
        b2.upload(files_path, files_name)
    finally:
        for p in (db_path, files_path):
            if p.exists():
                p.unlink()

    prune_backups(b2=b2, policy=parse_policy(env), dry_run=dry_run)


def prune_backups(b2: B2Adapter, policy: RetentionPolicy, dry_run: bool = False) -> None:
    print('Listing remote backups for retention check')
    remote_files = b2.list_files()
    stems = group_by_stem(remote_files)
    now = datetime.now(tz=timezone.utc)
    to_delete = stems_to_delete(stems, now, policy)

    kept = len(stems) - len(to_delete)
    print(f'Retention: {kept} set(s) kept, {len(to_delete)} set(s) to prune')

    if not to_delete:
        return

    total_files = sum(len(stems[s]) for s in to_delete)
    prefix = '[dry-run] ' if dry_run else ''
    print(f'{prefix}Deleting {len(to_delete)} backup set(s) ({total_files} file(s))')

    for stem in sorted(to_delete):
        for f in stems[stem]:
            print(f'  {prefix}{f.name}')
            if not dry_run:
                b2.delete(f)


def main() -> None:
    parser = argparse.ArgumentParser(description='Backup and prune B2 backups')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run backup normally but skip deleting old backups')
    parser.add_argument('--prune-only', action='store_true',
                        help='Skip backup creation, only run retention pruning')
    args = parser.parse_args()

    script_dir = Path(__file__).parent.resolve()
    env_path = script_dir / '.env'
    my_cnf_path = script_dir / 'my.cnf'

    if not env_path.exists():
        raise SystemExit(f'Error: {env_path} not found')
    if not args.prune_only and not my_cnf_path.exists():
        raise SystemExit(f'Error: {my_cnf_path} not found')

    env = load_env(env_path)
    b2 = B2SdkAdapter(
        key_id=env['B2_APPLICATION_KEY_ID'],
        app_key=env['B2_APPLICATION_KEY'],
        bucket_name=env['BUCKET_NAME'],
    )

    print(f'Starting at {datetime.now(tz=timezone.utc).isoformat()}')

    if args.prune_only:
        prune_backups(b2=b2, policy=parse_policy(env), dry_run=args.dry_run)
    else:
        run_backup(b2=b2, system=SubprocessAdapter(), env=env,
                   script_dir=script_dir, dry_run=args.dry_run)

    print(f'Done at {datetime.now(tz=timezone.utc).isoformat()}')


if __name__ == '__main__':
    main()
