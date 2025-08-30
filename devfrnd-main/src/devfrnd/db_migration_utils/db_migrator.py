import math
import signal
import sys
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConfigurationError, ConnectionFailure, OperationFailure
import click
from devfrnd.base_utils.logger_utils import RichLogger
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)
from rich.console import Console

# Logger
CONFIG_PATH = Path(__file__).parent / "logger.conf.json"
logger = RichLogger()
console = Console()

_stop = False  # graceful shutdown flag


def handle_sigint(sig, frame):
    """Handle Ctrl+C interrupt"""
    global _stop
    _stop = True
    logger.error("⚠ Migration interrupted by user. Closing connections...")

signal.signal(signal.SIGINT, handle_sigint)


@click.command("db_migrator")
@click.help_option("-h", "--help")
@click.option(
    "--to_local", "-tl", "direction", flag_value="to_local", required=True,
    help="Migrate from cloud to local"
)
# @click.option(
#     "--to_cloud", "-tc", "direction", flag_value="to_cloud", required=True,
#     help="Migrate from local to cloud"
# )
@click.argument("connection_str", type=str)
@click.argument("db_name", type=str)
@click.option("--percentage", "-p", type=int, default=100, help="Percentage of data to migrate (1-100)")
@click.option("--batch-size", "-b", type=int, default=1000, help="Batch size for inserts")
def migrate(direction, connection_str, db_name, percentage, batch_size):
    """Migrate MongoDB data between cloud and local.

    \b
    Examples:
      devfrnd migrate --to_local "<CLOUD_URI>" mydb --percentage 50
    #   devfrnd migrate --to_cloud "<CLOUD_URI>" mydb --batch-size 2000 (not active)
    """

    if not (1 <= percentage <= 100):
        logger.error("Percentage must be between 1 and 100")
        sys.exit(1)

    if direction.lower() == "to_local":
        source_uri, target_uri = connection_str, "mongodb://localhost:27017/"
    # elif direction.lower() == "to_cloud":
    #     source_uri, target_uri = "mongodb://localhost:27017/", connection_str

    _migrate(source_uri, target_uri, db_name, percentage, batch_size)


def _migrate(source_uri, target_uri, db_name, percentage, batch_size):
    global _stop
    source_client = None
    target_client = None
    logger.log_to_file(level="info", message=f"==============================================================")

    try:
        # Connect to Mongo
        try:
            source_client = MongoClient(source_uri)
            target_client = MongoClient(target_uri)
            source_client.admin.command("ping")
            target_client.admin.command("ping")
        except (ConfigurationError, ConnectionFailure, OperationFailure) as conn_err:
            logger.error(f"MongoDB connection error: {conn_err}")
            return

        source_db = source_client[db_name]
        target_db = target_client[db_name]

        logger.info(f"Starting migration for database: {db_name}")

        collections = source_db.list_collection_names()
        if not collections:
            logger.warning("No collections found in source DB.")
            return

        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            console=console, ) as progress:

            overall_task = progress.add_task("[green]Overall Migration", total=len(collections))

            for coll_name in collections:
                if _stop:
                    break

                source_coll = source_db[coll_name]
                target_coll = target_db[coll_name]

                total_docs = source_coll.estimated_document_count()
                if total_docs == 0:
                    # Empty collection: just create it
                    if coll_name not in target_db.list_collection_names():
                        target_db.create_collection(coll_name)
                    logger.log_to_file(level="info", message=f"📂 {coll_name} - Created empty collection.")
                    progress.advance(overall_task)
                    continue

                num_to_migrate = math.ceil(total_docs * (percentage / 100.0))
                logger.log_to_file(level="info", message=f"📂 {coll_name}: migrating {num_to_migrate}/{total_docs} docs")

                task = progress.add_task(f"[cyan]{coll_name}", total=num_to_migrate)
                migrated_count = 0

                cursor = source_coll.find({}, no_cursor_timeout=True).batch_size(batch_size)
                try:
                    while migrated_count < num_to_migrate and not _stop:
                        docs = []
                        try:
                            for _ in range(batch_size):
                                if migrated_count >= num_to_migrate:
                                    break
                                docs.append(next(cursor))
                        except StopIteration:
                            # Cursor exhausted
                            pass

                        if not docs:
                            break

                        try:
                            target_coll.insert_many(docs, ordered=False)
                        except BulkWriteError as bwe:
                            for err in bwe.details.get("writeErrors", []):
                                if err.get("code") == 11000:
                                    logger.warning(f"Duplicate _id in {coll_name}")
                                else:
                                    logger.error(f"Write error in {coll_name}: {err}")
                            logger.warning(f"BulkWriteError in {coll_name}")
                        except Exception as write_err:
                            logger.error(f"Write error in {coll_name}: {write_err}")
                            break

                        migrated_count += len(docs)
                        progress.update(task, advance=len(docs))
                finally:
                    cursor.close()

                progress.advance(overall_task)
                logger.log_to_file(level="info", message=f"✅ Completed {coll_name}: {migrated_count}/{total_docs}")

            if not _stop:
                logger.info("🎉 Migration complete.")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        if source_client:
            source_client.close()
        if target_client:
            target_client.close()
        logger.log_to_file(level="info", message=f"==============================================================")
        
