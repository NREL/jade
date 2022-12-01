import sqlite3

from jade.utils.run_command import check_run_command


SUBMIT_JOBS = "jade submit-jobs -f -l -m cpu -m memory -m process -m disk -m network"


def test_resource_stats(tmp_path):
    script = tmp_path / "sleep.sh"
    script.write_text("sleep 3\n")
    commands_file = tmp_path / "commands.txt"
    commands_file.write_text(f"bash {script}\n")
    config_file = tmp_path / "config.json"
    check_run_command(f"jade config create {commands_file} -c {config_file}")

    for mode in ("aggregation", "periodic"):
        output_dir = tmp_path / "output-dir"
        cmd = f"{SUBMIT_JOBS} {config_file} -p 1 -R {mode} -r 1 -o {output_dir}"
        check_run_command(cmd)
        db_file = output_dir / "results.db"
        files = [
            db_file,
            output_dir / "stats_summary.json",
        ]
        if mode == "periodic":
            files += [
                output_dir / "stats" / "CpuStatsViewer__resource_monitor_batch_0_0.html",
                output_dir / "stats" / "DiskStatsViewer__resource_monitor_batch_0_0.html",
                output_dir / "stats" / "MemoryStatsViewer__resource_monitor_batch_0_0.html",
                output_dir / "stats" / "NetworkStatsViewer__resource_monitor_batch_0_0.html",
                output_dir / "stats" / "ProcessStatsViewer__cpu_percent.html",
                output_dir / "stats" / "ProcessStatsViewer__rss.html",
            ]
        for filename in files:
            assert filename.exists()
        con = sqlite3.connect(db_file)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {x[0] for x in cur.fetchall()}
        assert not {"cpu", "disk", "memory", "network", "process"}.difference(tables)
        for table in tables:
            cur.execute(f"SELECT * from {table}")
            data = cur.fetchall()
            assert data
