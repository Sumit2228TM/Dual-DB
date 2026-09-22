import os
import csv
import logging
import smtplib
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage
from urllib.parse import urlparse

import mysql.connector
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

log = logging.getLogger(__name__)

OUTPUT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "openspecimen_table_comparison.csv",
)

CONTEXT_XML_PATH = "/usr/local/openspecimen/os-prod/tomcat-as/conf/context.xml"

OPS_RESOURCE_NAME = "jdbc/openspecimen"
REPORTING_RESOURCE_NAME = "openspecimen_reporting"

DATABASE_NAME = "indiana_prod"
DB_USER = "admin"

DRIFT_QUERY = f"""
SELECT
    t.TABLE_SCHEMA AS database_name,
    t.TABLE_NAME AS table_name,
    SUM(c.source_cnt) AS source_row_count,
    SUM(c.this_cnt) AS target_row_count,
    CASE
        WHEN COUNT(c.tbl) = 0
        THEN 'Skipped'
        WHEN SUM(
            CASE
                WHEN c.this_crc <> c.source_crc
                     OR c.this_cnt <> c.source_cnt
                THEN 1
                ELSE 0
            END
        ) > 0
        THEN 'Not Matching'
        ELSE 'Matching'
    END AS comparison
FROM information_schema.tables t
LEFT JOIN percona.checksums c
    ON c.db = t.TABLE_SCHEMA
   AND c.tbl = t.TABLE_NAME
WHERE t.TABLE_SCHEMA = '{DATABASE_NAME}'
  AND t.TABLE_TYPE = 'BASE TABLE'
GROUP BY
    t.TABLE_SCHEMA,
    t.TABLE_NAME
ORDER BY
    t.TABLE_NAME;
"""


class ReplicationError(RuntimeError):
    pass


class ChecksumError(RuntimeError):
    pass


class ConfigError(RuntimeError):
    pass


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ConfigError(
            f"Missing or empty required environment variable: {name}. "
            f"Checked .env at: {ENV_PATH} (exists: {ENV_PATH.exists()})"
        )
    return value.strip()


def load_smtp_config() -> dict:
    recipients = [
        e.strip() for e in os.getenv("RECIPIENT_EMAILS", "").split(",") if e.strip()
    ]

    if not recipients:
        raise ConfigError("No RECIPIENT_EMAILS configured")

    return {
        "server": require_env("SMTP_SERVER"),
        "port": int(require_env("SMTP_PORT")),
        "username": require_env("EMAIL_USERNAME"),
        "password": require_env("EMAIL_PASSWORD"),
        "sender": require_env("SENDER_EMAIL"),
        "recipients": recipients,
    }


def parse_jdbc_url(url: str) -> tuple:
    if not url.startswith("jdbc:mysql://"):
        raise ConfigError(
            f"Unexpected JDBC URL format (missing jdbc:mysql:// prefix): {url}"
        )

    stripped = url[len("jdbc:"):]
    parsed = urlparse(stripped)

    host = parsed.hostname
    port = parsed.port or 3306

    if not host:
        raise ConfigError(f"Could not parse host from JDBC URL: {url}")

    return host, port


def load_db_config_from_tomcat(resource_name: str) -> dict:
    context_path = Path(CONTEXT_XML_PATH)

    if not context_path.exists():
        raise ConfigError(f"context.xml not found at {CONTEXT_XML_PATH}")

    tree = ET.parse(context_path)
    root = tree.getroot()

    resource = None

    for elem in root.iter("Resource"):
        if elem.get("name") == resource_name:
            resource = elem
            break

    if resource is None:
        raise ConfigError(
            f"No <Resource name=\"{resource_name}\"> found in {CONTEXT_XML_PATH}"
        )

    url = resource.get("url")
    password = resource.get("password")

    if url is None or password is None:
        raise ConfigError(
            f"<Resource name=\"{resource_name}\"> is missing url/password "
            f"in {CONTEXT_XML_PATH}"
        )

    host, port = parse_jdbc_url(url)

    cfg = {
        "host": host,
        "port": port,
        "user": DB_USER,
        "password": password,
        "database": DATABASE_NAME,
        "connection_timeout": 10,
    }

    log.info(
        "Loaded config for resource '%s' -> host=%s port=%s user=%s database=%s",
        resource_name,
        cfg["host"],
        cfg["port"],
        cfg["user"],
        cfg["database"],
    )

    return cfg


def check_replication_status(cursor2) -> dict:
    cursor2.execute("SHOW REPLICA STATUS")
    row = cursor2.fetchone()

    if row is None:
        raise ReplicationError("SHOW REPLICA STATUS returned no rows")

    columns = [desc[0] for desc in cursor2.description]
    status = dict(zip(columns, row))

    io_running = status.get("Replica_IO_Running")
    sql_running = status.get("Replica_SQL_Running")
    lag_seconds = status.get("Seconds_Behind_Source")

    if io_running != "Yes" or sql_running != "Yes":
        raise ReplicationError(
            f"Replication is not running (IO={io_running}, SQL={sql_running}). "
            f"Last error: {status.get('Last_Error') or status.get('Last_SQL_Error')}"
        )

    return {
        "lag_seconds": lag_seconds,
        "io_running": io_running,
        "sql_running": sql_running,
    }


def truncate_previous_checksums(db1_cfg: dict) -> None:
    cmd = [
        "mysql",
        "-h", db1_cfg["host"],
        "-P", str(db1_cfg["port"]),
        "-u", db1_cfg["user"],
        f"-p{db1_cfg['password']}",
        "-e", "TRUNCATE TABLE percona.checksums;",
    ]

    log.info("Clearing stale rows from percona.checksums on Production before this run...")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0 and result.stderr.strip():
        if "doesn't exist" not in result.stderr:
            raise ChecksumError(
                f"Failed to truncate percona.checksums: "
                f"{result.stderr.strip()[:500]}"
            )


def run_checksum(db1_cfg: dict, db2_cfg: dict) -> None:
    if not db1_cfg.get("host"):
        raise ConfigError(
            "db1_cfg['host'] is empty - check the ops Resource in context.xml"
        )

    if not db2_cfg.get("host"):
        raise ConfigError(
            "db2_cfg['host'] is empty - check the reporting Resource in context.xml"
        )

    dsn = (
        f"h={db1_cfg['host']},P={db1_cfg['port']},u={db1_cfg['user']},"
        f"p={db1_cfg['password']},D={DATABASE_NAME},s=1"
    )

    cmd = [
        "pt-table-checksum",
        dsn,
        "--replicate=percona.checksums",
        "--recursion-method=none",
        "--no-check-binlog-format",
    ]

    log.info(
        "Running pt-table-checksum against Production (%s:%s), checking all tables...",
        db1_cfg["host"],
        db1_cfg["port"],
    )

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode == 0:
        log.info("pt-table-checksum completed successfully.")
        return

    if result.returncode == 255:
        log.error("pt-table-checksum actual fatal error: %s", result.stderr)
        raise ChecksumError(
            f"pt-table-checksum failed with fatal error (exit status 255)"
        )

    actual_error_flags = 1 | 2 | 4 | 8 | 128

    if result.returncode & actual_error_flags:
        log.error(
            "pt-table-checksum actual error (exit status %d): %s",
            result.returncode,
            result.stderr,
        )
        raise ChecksumError(
            f"pt-table-checksum failed with actual error "
            f"(exit status {result.returncode})"
        )

    non_fatal_flags = 16 | 32 | 64

    if result.returncode & non_fatal_flags:
        if result.returncode & 16:
            log.warning(
                "pt-table-checksum found checksum differences. "
                "Continuing to drift query."
            )

        if result.returncode & 32:
            log.warning(
                "pt-table-checksum skipped one or more chunks because they were "
                "oversized or otherwise skipped. Continuing to drift query."
            )

        if result.returncode & 64:
            log.warning(
                "pt-table-checksum skipped one or more tables. "
                "Continuing to drift query."
            )

        log.info(
            "pt-table-checksum returned non-fatal exit status %d. "
            "Continuing to drift query.",
            result.returncode,
        )
        return

    log.error(
        "pt-table-checksum returned unexpected exit status %d: %s",
        result.returncode,
        result.stderr,
    )

    raise ChecksumError(
        f"pt-table-checksum failed with unexpected exit status "
        f"{result.returncode}"
    )


def get_drift_rows(db2_cfg: dict) -> list:
    cmd = [
        "mysql",
        "-h", db2_cfg["host"],
        "-P", str(db2_cfg["port"]),
        "-u", db2_cfg["user"],
        f"-p{db2_cfg['password']}",
        "--batch",
        "--raw",
        "-e", DRIFT_QUERY,
    ]

    log.info(
        "Querying Reporting (%s:%s) for checksum comparison results...",
        db2_cfg["host"],
        db2_cfg["port"],
    )

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        log.error("Drift query stderr: %s", result.stderr)
        raise ChecksumError(
            f"Drift query failed: {result.stderr.strip()[:500]}"
        )

    lines = [line for line in result.stdout.splitlines() if line.strip()]

    if not lines:
        return []

    data_lines = lines[1:]
    rows = []

    for line in data_lines:
        db_name, table, src_cnt, tgt_cnt, comparison = line.split("\t")

        rows.append([
            table,
            src_cnt if src_cnt != "NULL" else "",
            tgt_cnt if tgt_cnt != "NULL" else "",
            comparison,
        ])

    return rows


def write_csv(rows: list, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Table Name", "Production Count", "Reporting Count", "Comparison"]
        )
        writer.writerows(rows)

    log.info("CSV written to %s", path)


def build_email_body(drifted_count: int, lag_seconds) -> str:
    lag_line = (
        f"Replication lag at time of check: {lag_seconds} seconds.\n\n"
        if lag_seconds
        else ""
    )

    status_line = "Replication Status: Healthy\n\n"

    if drifted_count == 0:
        return (
            "Hello all,\n\n"
            + status_line
            + lag_line
            + "pt-table-checksum found no out-of-sync tables between Production and Reporting.\n"
            "See attached CSV for details.\n"
        )

    return (
        "Hello all,\n\n"
        + status_line
        + lag_line
        + f"pt-table-checksum found {drifted_count} out-of-sync table(s) between "
        "Production and Reporting.\n"
        "See attached CSV for details.\n"
    )


def send_email(
    smtp_cfg: dict,
    subject: str,
    body: str,
    attachment_path: str,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_cfg["sender"]
    msg["To"] = ", ".join(smtp_cfg["recipients"])
    msg.set_content(body)

    with open(attachment_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=os.path.basename(attachment_path),
        )

    with smtplib.SMTP(
        smtp_cfg["server"],
        smtp_cfg["port"],
        timeout=15,
    ) as server:
        server.starttls()
        server.login(
            smtp_cfg["username"],
            smtp_cfg["password"],
        )
        server.send_message(msg)

    log.info("Email sent to %s", ", ".join(smtp_cfg["recipients"]))


def send_failure_alert(
    smtp_cfg: dict,
    error: Exception,
) -> None:
    today = datetime.now().strftime("%d/%m/%Y")

    if isinstance(error, ReplicationError):
        status_line = "Replication Status: Broken\n\n"
    elif isinstance(error, ConfigError):
        status_line = (
            "Replication Status: Unknown "
            "(configuration problem, not a DB issue)\n\n"
        )
    else:
        status_line = "Replication Status: Unknown\n\n"

    msg = EmailMessage()
    msg["Subject"] = f"Checksum drift check FAILED on {today}"
    msg["From"] = smtp_cfg["sender"]
    msg["To"] = ", ".join(smtp_cfg["recipients"])

    msg.set_content(
        "Hello all,\n\n"
        + status_line
        + "The Production/Reporting checksum drift check failed to complete.\n"
        f"Error: {error}\n\n"
        "No checksum comparison was performed for this run.\n"
    )

    with smtplib.SMTP(
        smtp_cfg["server"],
        smtp_cfg["port"],
        timeout=15,
    ) as server:
        server.starttls()
        server.login(
            smtp_cfg["username"],
            smtp_cfg["password"],
        )
        server.send_message(msg)


def main(smtp_cfg: dict) -> None:
    db1_cfg = load_db_config_from_tomcat(OPS_RESOURCE_NAME)
    db2_cfg = load_db_config_from_tomcat(REPORTING_RESOURCE_NAME)

    log.info("Connecting to Reporting database to check replication status...")

    try:
        with mysql.connector.connect(**db2_cfg) as conn2:
            with conn2.cursor() as cursor2:
                repl_status = check_replication_status(cursor2)

                log.info(
                    "Replication OK (IO=%s, SQL=%s, lag=%ss)",
                    repl_status["io_running"],
                    repl_status["sql_running"],
                    repl_status["lag_seconds"],
                )

    except mysql.connector.Error as exc:
        log.error("Database error: %s", exc)
        raise

    truncate_previous_checksums(db1_cfg)
    run_checksum(db1_cfg, db2_cfg)

    rows = get_drift_rows(db2_cfg)
    write_csv(rows, OUTPUT_FILE)

    drifted_count = sum(
        1 for row in rows
        if row[3] == "Not Matching"
    )

    today = datetime.now().strftime("%d/%m/%Y")
    status = "Success" if drifted_count == 0 else "Drift Detected"

    body = build_email_body(
        drifted_count,
        repl_status.get("lag_seconds"),
    )

    subject = f"Checksum drift check on {today}: {status}"

    try:
        send_email(
            smtp_cfg,
            subject,
            body,
            OUTPUT_FILE,
        )

    except smtplib.SMTPException as exc:
        log.error("Failed to send email: %s", exc)
        raise

    log.info(
        "Checksum drift check completed. Out-of-sync tables: %d",
        drifted_count,
    )


if __name__ == "__main__":
    smtp_cfg = None

    try:
        smtp_cfg = load_smtp_config()
        main(smtp_cfg)

    except Exception as exc:
        log.error(
            "Checksum drift check failed: %s",
            exc,
            exc_info=True,
        )

        if smtp_cfg:
            try:
                send_failure_alert(smtp_cfg, exc)
            except Exception as alert_exc:
                log.error(
                    "Also failed to send failure alert: %s",
                    alert_exc,
                )

        raise
