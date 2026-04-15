from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import backup
import pytest
from backup import RemoteFile, RetentionPolicy, group_by_stem, run_backup, stems_to_delete


def remote_files(*stems: str) -> list[RemoteFile]:
    files: list[RemoteFile] = []
    for stem in stems:
        files.append(RemoteFile(name=f"{stem}-db.sql.gz", file_id=f"{stem}-db"))
        files.append(RemoteFile(name=f"{stem}-files.tar.gz", file_id=f"{stem}-files"))
    return files


def grouped_stems(*stems: str) -> dict[str, list[RemoteFile]]:
    return group_by_stem(remote_files(*stems))


class FakeB2:
    def __init__(self, existing: list[RemoteFile] | None = None) -> None:
        self.files = list(existing or [])
        self.uploaded: list[str] = []
        self.deleted: list[RemoteFile] = []

    def upload(self, local_path: Path, remote_name: str) -> None:
        self.uploaded.append(remote_name)

    def list_files(self) -> list[RemoteFile]:
        return list(self.files)

    def delete(self, file: RemoteFile) -> None:
        self.deleted.append(file)
        self.files.remove(file)


class FakeSystem:
    def dump_db(self, my_cnf: Path, db_name: str, dest: Path) -> None:
        dest.touch()

    def archive_files(self, source: Path, dest: Path) -> None:
        dest.touch()


DEFAULT_POLICY = RetentionPolicy()


def test_stems_to_delete_returns_empty_for_no_backups() -> None:
    assert stems_to_delete({}, now=datetime.now(tz=timezone.utc), policy=DEFAULT_POLICY) == set()


def test_ensure_readable_reports_permission_issue(tmp_path: Path, monkeypatch) -> None:
    config_file = tmp_path / ".env"
    config_file.touch()
    monkeypatch.setattr(backup.os, "access", lambda path, mode: False)
    monkeypatch.setattr(backup.getpass, "getuser", lambda: "forge")

    with pytest.raises(SystemExit, match=r"not readable by forge"):
        backup.ensure_readable(config_file)


def test_stems_to_delete_keeps_all_backups_inside_keep_all_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems(
        "2026-04-15T10-00-00Z",
        "2026-04-14T10-00-00Z",
        "2026-04-10T10-00-00Z",
    )

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == set()


def test_stems_to_delete_prunes_older_duplicate_in_daily_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2026-04-04T18-00-00Z", "2026-04-04T06-00-00Z")

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == {"2026-04-04T06-00-00Z"}


def test_stems_to_delete_prunes_older_duplicate_in_weekly_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2026-03-28T10-00-00Z", "2026-03-23T10-00-00Z")

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == {"2026-03-23T10-00-00Z"}


def test_stems_to_delete_prunes_older_duplicate_in_monthly_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2026-01-20T10-00-00Z", "2026-01-05T10-00-00Z")

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == {"2026-01-05T10-00-00Z"}


def test_stems_to_delete_prunes_older_duplicate_in_yearly_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2024-11-01T10-00-00Z", "2024-02-01T10-00-00Z")

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == {"2024-02-01T10-00-00Z"}


def test_stems_to_delete_prunes_backups_older_than_all_retention_periods() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2019-01-01T10-00-00Z", "2018-01-01T10-00-00Z")

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == {"2018-01-01T10-00-00Z"}


def test_stems_to_delete_keeps_single_old_backup() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2018-01-01T10-00-00Z")

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == set()


def test_stems_to_delete_handles_boundary_cutoffs() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems(
        "2026-04-08T12-00-00Z",
        "2026-04-08T11-59-59Z",
        "2026-04-01T12-00-00Z",
        "2026-02-04T12-00-00Z",
        "2024-02-10T12-00-00Z",
    )

    assert stems_to_delete(stems, now=now, policy=DEFAULT_POLICY) == set()


def test_run_backup_uploads_both_files_and_prunes_old_sets(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(backup, "make_stem", lambda: "2026-04-15T12-00-00Z")
    b2 = FakeB2(existing=remote_files("2018-01-01T10-00-00Z", "2017-01-01T10-00-00Z"))
    env = {
        "BACKUP_DEST": str(tmp_path / "backups"),
        "MYSQL_DB": "app_db",
        "BACKUP_SOURCE": str(tmp_path / "site"),
        "RETENTION_KEEP_ALL_DAYS": "7",
        "RETENTION_DAILY_DAYS": "7",
        "RETENTION_WEEKLY_WEEKS": "8",
        "RETENTION_MONTHLY_MONTHS": "4",
        "RETENTION_YEARLY_YEARS": "2",
    }

    run_backup(
        b2=b2,
        system=FakeSystem(),
        env=env,
        script_dir=tmp_path,
        dry_run=False,
    )

    assert b2.uploaded == [
        "2026-04-15T12-00-00Z-db.sql.gz",
        "2026-04-15T12-00-00Z-files.tar.gz",
    ]
    assert [file.name for file in b2.deleted] == [
        "2017-01-01T10-00-00Z-db.sql.gz",
        "2017-01-01T10-00-00Z-files.tar.gz",
    ]
    assert not any((tmp_path / "backups").iterdir())


def test_run_backup_dry_run_does_not_delete_remote_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(backup, "make_stem", lambda: "2026-04-15T12-00-00Z")
    existing = remote_files("2018-01-01T10-00-00Z", "2017-01-01T10-00-00Z")
    b2 = FakeB2(existing=existing)
    env = {
        "BACKUP_DEST": str(tmp_path / "backups"),
        "MYSQL_DB": "app_db",
        "BACKUP_SOURCE": str(tmp_path / "site"),
    }

    run_backup(
        b2=b2,
        system=FakeSystem(),
        env=env,
        script_dir=tmp_path,
        dry_run=True,
    )

    assert b2.uploaded == []
    assert b2.deleted == []
    assert b2.files == existing
    assert not (tmp_path / "backups").exists()
