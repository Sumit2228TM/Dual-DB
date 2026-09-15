import os
import csv
import logging
import smtplib
from datetime import datetime
from email.message import EmailMessage

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

OUTPUT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "openspecimen_table_comparison.csv",
)

class ReplicationError(RuntimeError):
    pass

def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def load_db_config(prefix: str) -> dict:
    return {
        "host": require_env(f"{prefix}_HOST"),
        "port": int(require_env(f"{prefix}_PORT")),
        "user": require_env(f"{prefix}_USER"),
        "password": require_env(f"{prefix}_PASSWORD"),
        "database": require_env(f"{prefix}_DATABASE"),
        "connection_timeout": 10,
    }


def load_smtp_config() -> dict:
    recipients = [
        e.strip()
        for e in os.getenv("RECIPIENT_EMAILS", "").split(",")
        if e.strip()
    ]
    if not recipients:
        raise RuntimeError("No RECIPIENT_EMAILS configured")

    return {
        "server": require_env("SMTP_SERVER"),
        "port": int(require_env("SMTP_PORT")),
        "username": require_env("EMAIL_USERNAME"),
        "password": require_env("EMAIL_PASSWORD"),
        "sender": require_env("SENDER_EMAIL"),
        "recipients": recipients,
    }


def get_tables(cursor, database: str) -> list:
    cursor.execute(
        """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """,
        (database,),
    )
    return [row[0] for row in cursor.fetchall()]


def get_table_count(cursor, table: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
    return cursor.fetchone()[0]


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


def compare_tables(cursor1, cursor2, db1_name: str, db2_name: str):
    tables_db1 = set(get_tables(cursor1, db1_name))
    tables_db2 = set(get_tables(cursor2, db2_name))
    all_tables = sorted(tables_db1 | tables_db2)

    log.info("Total tables found across both DBs: %d", len(all_tables))

    results = []
    mismatched_tables = []

    for table in all_tables:
        count_db1 = get_table_count(cursor1, table) if table in tables_db1 else None
        count_db2 = get_table_count(cursor2, table) if table in tables_db2 else None

        matches = (
            count_db1 is not None
            and count_db2 is not None
            and count_db1 == count_db2
        )
        comparison = "Matching" if matches else "Not Matching"
        if not matches:
            mismatched_tables.append(table)

        results.append([table, count_db1, count_db2, comparison])

    return results, mismatched_tables


def write_csv(results: list, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Table Name", "Production Count", "Reporting Count", "Comparison"])
        writer.writerows(results)
    log.info("CSV written to %s", path)


def build_email_body(mismatch_count: int) -> str:
    status_line = "Replication Status: Healthy\n\n"

    if mismatch_count == 0:
        return (
            "Hello all,\n\n"
            + status_line
            + "All table counts between the Production and Reporting databases match.\n"
            "See attached CSV for details.\n"
        )
    return (
        "Hello all,\n\n"
        + status_line
        + f"Counts of {mismatch_count} tables are not matching. See attached CSV for details.\n"
    )


def send_email(smtp_cfg: dict, subject: str, body: str, attachment_path: str) -> None:
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

    with smtplib.SMTP(smtp_cfg["server"], smtp_cfg["port"], timeout=15) as server:
        server.starttls()
        server.login(smtp_cfg["username"], smtp_cfg["password"])
        server.send_message(msg)

    log.info("Email sent to %s", ", ".join(smtp_cfg["recipients"]))


def send_failure_alert(smtp_cfg: dict, error: Exception) -> None:
    today = datetime.now().strftime("%d/%m/%Y")

    if isinstance(error, ReplicationError):
        status_line = "Replication Status: Broken\n\n"
    else:
        status_line = "Replication Status: Unknown\n\n"

    msg = EmailMessage()
    msg["Subject"] = f"Replication validation script FAILED on {today}"
    msg["From"] = smtp_cfg["sender"]
    msg["To"] = ", ".join(smtp_cfg["recipients"])
    msg.set_content(
        "Hello all,\n\n"
        + status_line
        + "The Production/Reporting DB validation script failed to complete.\n"
        f"Error: {error}\n\n"
        "No table-count comparison was performed for this run.\n"
    )
    with smtplib.SMTP(smtp_cfg["server"], smtp_cfg["port"], timeout=15) as server:
        server.starttls()
        server.login(smtp_cfg["username"], smtp_cfg["password"])
        server.send_message(msg)


def main() -> None:
    db1_cfg = load_db_config("DB1")
    db2_cfg = load_db_config("DB2")
    smtp_cfg = load_smtp_config()

    log.info("Connecting to Production database...")
    log.info("Connecting to Reporting database...")

    try:
        with mysql.connector.connect(**db1_cfg) as conn1, \
             mysql.connector.connect(**db2_cfg) as conn2:

            with conn1.cursor() as cursor1, conn2.cursor() as cursor2:
                repl_status = check_replication_status(cursor2)
                log.info(
                    "Replication OK (IO=%s, SQL=%s, lag=%ss)",
                    repl_status["io_running"],
                    repl_status["sql_running"],
                    repl_status["lag_seconds"],
                )

                results, mismatched = compare_tables(
                    cursor1, cursor2, db1_cfg["database"], db2_cfg["database"]
                )
    except mysql.connector.Error as exc:
        log.error("Database error: %s", exc)
        raise

    write_csv(results, OUTPUT_FILE)

    mismatch_count = len(mismatched)
    today = datetime.now().strftime("%d/%m/%Y")
    status = "Success" if mismatch_count == 0 else "Failure"

    body = build_email_body(mismatch_count)
    subject = f"Replication status on {today}: {status}"

    try:
        send_email(smtp_cfg, subject, body, OUTPUT_FILE)
    except smtplib.SMTPException as exc:
        log.error("Failed to send email: %s", exc)
        raise

    log.info("Validation completed successfully.")
    log.info("Total tables checked : %d", len(results))
    log.info("Tables not matching  : %d", mismatch_count)
    log.info("CSV file             : %s", OUTPUT_FILE)
    log.info("Email status         : %s", status)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Validation run failed: %s", exc)
        try:
            smtp_cfg = load_smtp_config()
            send_failure_alert(smtp_cfg, exc)
        except Exception as alert_exc:
            log.error("Also failed to send failure alert: %s", alert_exc)
        raise
