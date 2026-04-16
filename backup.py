#!/usr/bin/env python3
"""Backup script: dumps DB and archives files, uploads to Backblaze B2,
then enforces a tiered retention policy on existing backups."""

from __future__ import annotations

import argparse
import getpass
import os
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


class B2ConfigurationError(RuntimeError):
    pass


class FileRetainedByLockError(RuntimeError):
    pass

@dataclass
class RemoteFile:
    name: str
    file_id: str


@dataclass
class RetentionPolicy:
    keep_all_days: int = 7
    daily_days: int = 16
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
        from b2sdk._internal.exception import Unauthorized
        from b2sdk.v2 import B2Api, InMemoryAccountInfo

        self._bucket_name = bucket_name
        self._api = B2Api(InMemoryAccountInfo())
        try:
            self._api.authorize_account("production", key_id, app_key)
            self._bucket = self._api.get_bucket_by_name(bucket_name)
        except Unauthorized as exc:
            raise B2ConfigurationError(build_b2_permission_error("authorize", bucket_name, str(exc))) from exc

    def upload(self, local_path: Path, remote_name: str) -> None:
        from b2sdk._internal.exception import Unauthorized

        try:
            self._bucket.upload_local_file(local_file=str(local_path), file_name=remote_name)
        except Unauthorized as exc:
            raise B2ConfigurationError(build_b2_permission_error("upload", self._bucket_name, str(exc))) from exc

    def list_files(self) -> list[RemoteFile]:
        from b2sdk._internal.exception import Unauthorized

        files = []
        try:
            for fv, folder in self._bucket.ls(latest_only=False, recursive=True):
                if folder is None:
                    files.append(RemoteFile(name=fv.file_name, file_id=fv.id_))
        except Unauthorized as exc:
            raise B2ConfigurationError(build_b2_permission_error("list", self._bucket_name, str(exc))) from exc
        return files

    def delete(self, file: RemoteFile) -> None:
        from b2sdk._internal.exception import AccessDenied, Unauthorized

        try:
            self._api.delete_file_version(file_id=file.file_id, file_name=file.name)
        except Unauthorized as exc:
            raise B2ConfigurationError(build_b2_permission_error("delete", self._bucket_name, str(exc))) from exc
        except AccessDenied as exc:
            raise FileRetainedByLockError(
                f'{file.name} is protected by Object Lock and cannot be deleted yet. '
                f'Backblaze details: {exc}'
            ) from exc


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
        daily_days=int(env.get('RETENTION_DAILY_DAYS', 16)),
        weekly_weeks=int(env.get('RETENTION_WEEKLY_WEEKS', 8)),
        monthly_months=int(env.get('RETENTION_MONTHLY_MONTHS', 4)),
        yearly_years=int(env.get('RETENTION_YEARLY_YEARS', 2)),
    )


def build_b2_permission_error(action: str, bucket_name: str, details: str) -> str:
    required_caps = "listFiles, writeFiles, and deleteFiles"
    if action == "authorize":
        return (
            f'Backblaze B2 rejected the configured application key for bucket "{bucket_name}". '
            f'Use a key scoped to this bucket with at least {required_caps}. '
            f'Backblaze details: {details}'
        )
    if action == "list":
        return (
            f'Backblaze B2 could not list files in bucket "{bucket_name}". '
            f'This script needs list access to preview and enforce retention. '
            f'Use a key scoped to this bucket with at least {required_caps}. '
            f'Backblaze details: {details}'
        )
    if action == "delete":
        return (
            f'Backblaze B2 could not delete files in bucket "{bucket_name}". '
            f'This script needs delete access to enforce retention. '
            f'Use a key scoped to this bucket with at least {required_caps}. '
            f'Backblaze details: {details}'
        )
    return (
        f'Backblaze B2 could not upload to bucket "{bucket_name}". '
        f'Use a key scoped to this bucket with at least {required_caps}. '
        f'Backblaze details: {details}'
    )


def ensure_readable(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f'Error: {path} not found')
    if not os.access(path, os.R_OK):
        user = getpass.getuser()
        raise SystemExit(
            f'Error: {path} is not readable by {user}. '
            f'Fix ownership/permissions and rerun without sudo, for example: '
            f'sudo chown {user}:{user} {path} && chmod 600 {path}'
        )


# ---------------------------------------------------------------------------
# Pure retention logic (no I/O)
# ---------------------------------------------------------------------------

def evaluate_retention(
    stems: dict[str, list[RemoteFile]],
    now: datetime,
    policy: RetentionPolicy,
) -> tuple[dict[str, str], dict[str, str]]:
    """Evaluate retention policy and return a plan of what to keep and what to delete.

    Matches the strategy used by Spatie's laravel-backup. Rules are evaluated
    independently, so a single backup can satisfy multiple periods (e.g., a
    backup on the last Sunday of a year could be the daily, weekly, monthly,
    and yearly backup simultaneously).

    Returns:
        A tuple of (keep_dict, delete_dict), where each is a mapping of stem -> reasons.
    """
    keep_all_cutoff = now - timedelta(days=policy.keep_all_days)
    daily_cutoff = now - timedelta(days=policy.daily_days)
    weekly_cutoff = now - timedelta(weeks=policy.weekly_weeks)
    monthly_cutoff = now - timedelta(days=policy.monthly_months * 30)
    yearly_cutoff = now - timedelta(days=policy.yearly_years * 365)

    sorted_stems = sorted(stems, key=parse_stem_time, reverse=True)

    seen_days: set = set()
    seen_weeks: set = set()
    seen_months: set = set()
    seen_years: set = set()

    keep: dict[str, str] = {}
    to_delete: dict[str, str] = {}

    keep_all_count = 0
    daily_count = 0
    weekly_count = 0
    monthly_count = 0
    yearly_count = 0

    for stem in sorted_stems:
        t = parse_stem_time(stem)
        day_key = t.date()
        week_key = t.isocalendar()[:2]
        month_key = (t.year, t.month)
        year_key = t.year

        reasons = []

        # 1. Keep All
        if t >= keep_all_cutoff:
            keep_all_count += 1
            reasons.append(f"Keep-all period #{keep_all_count}")

        # 2. Daily (latest per day)
        if t >= daily_cutoff:
            if day_key not in seen_days:
                seen_days.add(day_key)
                daily_count += 1
                reasons.append(f"Daily #{daily_count}")

        # 3. Weekly (latest per week)
        if t >= weekly_cutoff:
            if week_key not in seen_weeks:
                seen_weeks.add(week_key)
                weekly_count += 1
                reasons.append(f"Weekly #{weekly_count}")

        # 4. Monthly (latest per month)
        if t >= monthly_cutoff:
            if month_key not in seen_months:
                seen_months.add(month_key)
                monthly_count += 1
                reasons.append(f"Monthly #{monthly_count}")

        # 5. Yearly (latest per year)
        if t >= yearly_cutoff:
            if year_key not in seen_years:
                seen_years.add(year_key)
                yearly_count += 1
                reasons.append(f"Yearly #{yearly_count}")

        if reasons:
            keep[stem] = ", ".join(reasons)
        else:
            to_delete[stem] = "To be deleted"

    if sorted_stems and not keep:
        stem = sorted_stems[0]
        keep[stem] = "Safety: keeping only existing backup"
        to_delete.pop(stem, None)

    return keep, to_delete


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
    db_path = backup_dest / db_name
    files_path = backup_dest / files_name

    if dry_run:
        print(f'[dry-run] Would dump DB ({env["MYSQL_DB"]}) to {db_path}')
        print(f'[dry-run] Would archive files from {env["BACKUP_SOURCE"]} to {files_path}')
        print(f'[dry-run] Would upload {db_name}')
        print(f'[dry-run] Would upload {files_name}')
        prune_backups(b2=b2, policy=parse_policy(env), dry_run=True)
        return

    backup_dest.mkdir(parents=True, exist_ok=True)

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
    keep_plan, delete_plan = evaluate_retention(stems, now, policy)

    print('\nRetention Plan:')
    for stem in sorted(stems, key=parse_stem_time, reverse=True):
        if stem in keep_plan:
            print(f"  KEEP   {stem} ({keep_plan[stem]})")
        else:
            print(f"  DELETE {stem} ({delete_plan[stem]})")
    print()

    print(f'Retention summary: {len(keep_plan)} set(s) kept, {len(delete_plan)} set(s) to prune')

    if not delete_plan:
        return

    total_files = sum(len(stems[s]) for s in delete_plan)
    prefix = '[dry-run] ' if dry_run else ''
    print(f'{prefix}Executing deletion of {len(delete_plan)} backup set(s) ({total_files} file(s))')

    locked = 0
    for stem in sorted(delete_plan):
        for f in stems[stem]:
            print(f'  {prefix}{f.name}')
            if not dry_run:
                try:
                    b2.delete(f)
                except FileRetainedByLockError:
                    locked += 1
                    print(f'    ^ skipped (Object Lock retention still active)')

    if locked:
        print(f'\n{locked} file(s) skipped due to Object Lock — they will be deleted once retention expires')


def main() -> None:
    parser = argparse.ArgumentParser(description='Backup and prune B2 backups')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview backup and pruning actions without creating, uploading, or deleting files')
    parser.add_argument('--prune-only', action='store_true',
                        help='Skip backup creation, only run retention pruning')
    args = parser.parse_args()

    script_dir = Path(__file__).parent.resolve()
    env_path = script_dir / '.env'
    my_cnf_path = script_dir / 'my.cnf'

    ensure_readable(env_path)
    if not args.prune_only:
        ensure_readable(my_cnf_path)

    env = load_env(env_path)
    try:
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
    except B2ConfigurationError as exc:
        raise SystemExit(f'Error: {exc}') from exc

    print(f'Done at {datetime.now(tz=timezone.utc).isoformat()}')


if __name__ == '__main__':
    main()
