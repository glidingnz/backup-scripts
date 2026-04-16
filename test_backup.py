from datetime import datetime, timezone, timedelta
from pathlib import Path

import backup
import pytest
from backup import RemoteFile, RetentionPolicy, group_by_stem, run_backup, evaluate_retention


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


def test_evaluate_retention_returns_empty_for_no_backups() -> None:
    keep, delete = evaluate_retention({}, now=datetime.now(tz=timezone.utc), policy=DEFAULT_POLICY)
    assert keep == {}
    assert delete == {}


def test_ensure_readable_reports_permission_issue(tmp_path: Path, monkeypatch) -> None:
    config_file = tmp_path / ".env"
    config_file.touch()
    monkeypatch.setattr(backup.os, "access", lambda path, mode: False)
    monkeypatch.setattr(backup.getpass, "getuser", lambda: "forge")

    with pytest.raises(SystemExit, match=r"not readable by forge"):
        backup.ensure_readable(config_file)


def test_build_b2_permission_error_for_list_mentions_retention_capabilities() -> None:
    message = backup.build_b2_permission_error("list", "backup-bucket", "unauthorized")

    assert 'could not list files in bucket "backup-bucket"' in message
    assert "preview and enforce retention" in message
    assert "listFiles, writeFiles, and deleteFiles" in message


def test_evaluate_retention_keeps_all_backups_inside_keep_all_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems(
        "2026-04-15T10-00-00Z",
        "2026-04-14T10-00-00Z",
        "2026-04-10T10-00-00Z",
    )

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    assert delete == {}
    assert keep == {
        "2026-04-15T10-00-00Z": "Keep-all period #1, Daily #1, Weekly #1, Monthly #1, Yearly #1",
        "2026-04-14T10-00-00Z": "Keep-all period #2, Daily #2",
        "2026-04-10T10-00-00Z": "Keep-all period #3, Daily #3, Weekly #2",
    }


def test_evaluate_retention_prunes_older_duplicate_in_daily_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2026-04-04T18-00-00Z", "2026-04-04T06-00-00Z")

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    assert keep == {"2026-04-04T18-00-00Z": "Weekly #1, Monthly #1, Yearly #1"}
    assert delete == {"2026-04-04T06-00-00Z": "To be deleted"}


def test_evaluate_retention_prunes_older_duplicate_in_weekly_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2026-03-28T10-00-00Z", "2026-03-23T10-00-00Z")

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    assert keep == {"2026-03-28T10-00-00Z": "Weekly #1, Monthly #1, Yearly #1"}
    assert delete == {"2026-03-23T10-00-00Z": "To be deleted"}


def test_evaluate_retention_prunes_older_duplicate_in_monthly_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2026-01-20T10-00-00Z", "2026-01-05T10-00-00Z")

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    assert keep == {"2026-01-20T10-00-00Z": "Monthly #1, Yearly #1"}
    assert delete == {"2026-01-05T10-00-00Z": "To be deleted"}


def test_evaluate_retention_prunes_older_duplicate_in_yearly_window() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2024-11-01T10-00-00Z", "2024-02-01T10-00-00Z")

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    assert keep == {"2024-11-01T10-00-00Z": "Yearly #1"}
    assert delete == {"2024-02-01T10-00-00Z": "To be deleted"}


def test_evaluate_retention_prunes_backups_older_than_all_retention_periods() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2019-01-01T10-00-00Z", "2018-01-01T10-00-00Z")

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    assert keep == {"2019-01-01T10-00-00Z": "Safety: keeping only existing backup"}
    assert delete == {"2018-01-01T10-00-00Z": "To be deleted"}


def test_evaluate_retention_keeps_single_old_backup() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems("2018-01-01T10-00-00Z")

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    assert delete == {}
    assert keep == {"2018-01-01T10-00-00Z": "Safety: keeping only existing backup"}


def test_evaluate_retention_handles_boundary_cutoffs() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    stems = grouped_stems(
        "2026-04-08T12-00-00Z",
        "2026-04-08T11-59-59Z",
        "2026-04-01T12-00-00Z",
        "2026-02-04T12-00-00Z",
        "2024-02-10T12-00-00Z",
    )

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    actual = {**keep, **delete}
    expected = {
        "2026-04-08T12-00-00Z": "Keep-all period #1, Daily #1, Weekly #1, Monthly #1, Yearly #1",
        "2026-04-01T12-00-00Z": "Weekly #2",
        "2026-02-04T12-00-00Z": "Monthly #2",
        "2026-04-08T11-59-59Z": "To be deleted",
        "2024-02-10T12-00-00Z": "To be deleted"
    }
    assert actual == expected


def test_evaluate_retention_90_days_of_daily_backups() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    
    # Generate 90 days of daily backups (Apr 15 back to Jan 16)
    stems_list = []
    for i in range(90):
        backup_time = now - timedelta(days=i)
        stems_list.append(backup_time.strftime(backup.STEM_TIME_FMT))
        
    stems = grouped_stems(*stems_list)
    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    actual = {**keep, **delete}

    expected = {
        "2026-04-15T12-00-00Z": "Keep-all period #1, Daily #1, Weekly #1, Monthly #1, Yearly #1",
        "2026-04-14T12-00-00Z": "Keep-all period #2, Daily #2",
        "2026-04-13T12-00-00Z": "Keep-all period #3, Daily #3",
        "2026-04-12T12-00-00Z": "Keep-all period #4, Daily #4, Weekly #2",
        "2026-04-11T12-00-00Z": "Keep-all period #5, Daily #5",
        "2026-04-10T12-00-00Z": "Keep-all period #6, Daily #6",
        "2026-04-09T12-00-00Z": "Keep-all period #7, Daily #7",
        "2026-04-08T12-00-00Z": "Keep-all period #8, Daily #8",
        "2026-04-07T12-00-00Z": "To be deleted",
        "2026-04-06T12-00-00Z": "To be deleted",
        "2026-04-05T12-00-00Z": "Weekly #3",
        "2026-04-04T12-00-00Z": "To be deleted",
        "2026-04-03T12-00-00Z": "To be deleted",
        "2026-04-02T12-00-00Z": "To be deleted",
        "2026-04-01T12-00-00Z": "To be deleted",
        "2026-03-31T12-00-00Z": "Monthly #2",
        "2026-03-30T12-00-00Z": "To be deleted",
        "2026-03-29T12-00-00Z": "Weekly #4",
        "2026-03-28T12-00-00Z": "To be deleted",
        "2026-03-27T12-00-00Z": "To be deleted",
        "2026-03-26T12-00-00Z": "To be deleted",
        "2026-03-25T12-00-00Z": "To be deleted",
        "2026-03-24T12-00-00Z": "To be deleted",
        "2026-03-23T12-00-00Z": "To be deleted",
        "2026-03-22T12-00-00Z": "Weekly #5",
        "2026-03-21T12-00-00Z": "To be deleted",
        "2026-03-20T12-00-00Z": "To be deleted",
        "2026-03-19T12-00-00Z": "To be deleted",
        "2026-03-18T12-00-00Z": "To be deleted",
        "2026-03-17T12-00-00Z": "To be deleted",
        "2026-03-16T12-00-00Z": "To be deleted",
        "2026-03-15T12-00-00Z": "Weekly #6",
        "2026-03-14T12-00-00Z": "To be deleted",
        "2026-03-13T12-00-00Z": "To be deleted",
        "2026-03-12T12-00-00Z": "To be deleted",
        "2026-03-11T12-00-00Z": "To be deleted",
        "2026-03-10T12-00-00Z": "To be deleted",
        "2026-03-09T12-00-00Z": "To be deleted",
        "2026-03-08T12-00-00Z": "Weekly #7",
        "2026-03-07T12-00-00Z": "To be deleted",
        "2026-03-06T12-00-00Z": "To be deleted",
        "2026-03-05T12-00-00Z": "To be deleted",
        "2026-03-04T12-00-00Z": "To be deleted",
        "2026-03-03T12-00-00Z": "To be deleted",
        "2026-03-02T12-00-00Z": "To be deleted",
        "2026-03-01T12-00-00Z": "Weekly #8",
        "2026-02-28T12-00-00Z": "Monthly #3",
        "2026-02-27T12-00-00Z": "To be deleted",
        "2026-02-26T12-00-00Z": "To be deleted",
        "2026-02-25T12-00-00Z": "To be deleted",
        "2026-02-24T12-00-00Z": "To be deleted",
        "2026-02-23T12-00-00Z": "To be deleted",
        "2026-02-22T12-00-00Z": "Weekly #9",
        "2026-02-21T12-00-00Z": "To be deleted",
        "2026-02-20T12-00-00Z": "To be deleted",
        "2026-02-19T12-00-00Z": "To be deleted",
        "2026-02-18T12-00-00Z": "To be deleted",
        "2026-02-17T12-00-00Z": "To be deleted",
        "2026-02-16T12-00-00Z": "To be deleted",
        "2026-02-15T12-00-00Z": "To be deleted",
        "2026-02-14T12-00-00Z": "To be deleted",
        "2026-02-13T12-00-00Z": "To be deleted",
        "2026-02-12T12-00-00Z": "To be deleted",
        "2026-02-11T12-00-00Z": "To be deleted",
        "2026-02-10T12-00-00Z": "To be deleted",
        "2026-02-09T12-00-00Z": "To be deleted",
        "2026-02-08T12-00-00Z": "To be deleted",
        "2026-02-07T12-00-00Z": "To be deleted",
        "2026-02-06T12-00-00Z": "To be deleted",
        "2026-02-05T12-00-00Z": "To be deleted",
        "2026-02-04T12-00-00Z": "To be deleted",
        "2026-02-03T12-00-00Z": "To be deleted",
        "2026-02-02T12-00-00Z": "To be deleted",
        "2026-02-01T12-00-00Z": "To be deleted",
        "2026-01-31T12-00-00Z": "Monthly #4",
        "2026-01-30T12-00-00Z": "To be deleted",
        "2026-01-29T12-00-00Z": "To be deleted",
        "2026-01-28T12-00-00Z": "To be deleted",
        "2026-01-27T12-00-00Z": "To be deleted",
        "2026-01-26T12-00-00Z": "To be deleted",
        "2026-01-25T12-00-00Z": "To be deleted",
        "2026-01-24T12-00-00Z": "To be deleted",
        "2026-01-23T12-00-00Z": "To be deleted",
        "2026-01-22T12-00-00Z": "To be deleted",
        "2026-01-21T12-00-00Z": "To be deleted",
        "2026-01-20T12-00-00Z": "To be deleted",
        "2026-01-19T12-00-00Z": "To be deleted",
        "2026-01-18T12-00-00Z": "To be deleted",
        "2026-01-17T12-00-00Z": "To be deleted",
        "2026-01-16T12-00-00Z": "To be deleted",
    }

    assert actual == expected


def test_evaluate_retention_yearly_retention_across_years() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    # 2025-12-31 (Yearly #1), 2024-12-31 (Yearly #2)
    stems = grouped_stems(
        "2025-12-31T12-00-00Z",
        "2025-01-01T12-00-00Z",
        "2024-12-31T12-00-00Z",
        "2024-05-01T12-00-00Z",
        "2020-01-01T12-00-00Z",
    )

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    # 2025-12-31 is within 4 months, so it is Monthly #1 and Yearly #1.
    assert keep == {
        "2025-12-31T12-00-00Z": "Monthly #1, Yearly #1",
        "2024-12-31T12-00-00Z": "Yearly #2",
    }
    assert delete == {
        "2025-01-01T12-00-00Z": "To be deleted",
        "2024-05-01T12-00-00Z": "To be deleted",
        "2020-01-01T12-00-00Z": "To be deleted",
    }


def test_evaluate_retention_never_deletes_newest_even_if_outside_all_windows() -> None:
    now = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    # Two old backups
    stems = grouped_stems("2019-01-01T12-00-00Z", "2018-01-01T12-00-00Z")

    keep, delete = evaluate_retention(stems, now=now, policy=DEFAULT_POLICY)
    # The newest one (2019) should be kept for safety.
    assert keep == {"2019-01-01T12-00-00Z": "Safety: keeping only existing backup"}
    assert delete == {"2018-01-01T12-00-00Z": "To be deleted"}


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
