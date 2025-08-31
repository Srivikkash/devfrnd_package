import math
import signal
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConfigurationError, ConnectionFailure, OperationFailure, PyMongoError

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
logger = RichLogger()
console = Console()

_stop = False  # graceful shutdown flag


def handle_sigint(sig, frame):
    """Handle Ctrl+C interrupt"""
    global _stop
    _stop = True
    logger.error("⚠ Migration interrupted by user. Closing connections...")

signal.signal(signal.SIGINT, handle_sigint)

def _safe_close(client, name):
    try:
        if client:
            client.close()
    except Exception as e:
        logger.error(f"Error closing {name} client: {e}")

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
        except (ConfigurationError, ConnectionFailure, OperationFailure, PyMongoError) as conn_err:
            logger.error(f"MongoDB connection error: {conn_err}")
            return

        try:
            source_db = source_client[db_name]
            target_db = target_client[db_name]
        except Exception as db_err:
            logger.error(f"Error accessing database: {db_err}")
            return

        logger.info(f"Starting migration for database: {db_name}")

        try:
            collections = source_db.list_collection_names()
        except Exception as coll_err:
            logger.error(f"Error listing collections: {coll_err}")
            return

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

                try:
                    source_coll = source_db[coll_name]
                    target_coll = target_db[coll_name]
                except Exception as coll_access_err:
                    logger.error(f"Error accessing collection {coll_name}: {coll_access_err}")
                    progress.advance(overall_task)
                    continue

                try:
                    total_docs = source_coll.estimated_document_count()
                except Exception as count_err:
                    logger.error(f"Error counting documents in {coll_name}: {count_err}")
                    progress.advance(overall_task)
                    continue

                if total_docs == 0:
                    try:
                        if coll_name not in target_db.list_collection_names():
                            target_db.create_collection(coll_name)
                        logger.log_to_file(level="info", message=f"📂 {coll_name} - Created empty collection.")
                    except Exception as create_err:
                        logger.log_to_file(f"Error creating empty collection {coll_name}: {create_err}")
                    progress.advance(overall_task)
                    continue

                num_to_migrate = math.ceil(total_docs * (percentage / 100.0))
                logger.log_to_file(level="info", message=f"📂 {coll_name}: migrating {num_to_migrate}/{total_docs} docs")

                task = progress.add_task(f"[cyan]{coll_name}", total=num_to_migrate)
                migrated_count = 0

                try:
                    cursor = source_coll.find({}, no_cursor_timeout=True).batch_size(batch_size)
                except Exception as cursor_err:
                    logger.error(f"Error creating cursor for {coll_name}: {cursor_err}")
                    progress.advance(overall_task)
                    continue

                try:
                    while migrated_count < num_to_migrate and not _stop:
                        docs = []
                        try:
                            for _ in range(batch_size):
                                if migrated_count >= num_to_migrate:
                                    break
                                try:
                                    docs.append(next(cursor))
                                except StopIteration:
                                    # Cursor exhausted
                                    pass
                                except Exception as doc_err:
                                    logger.error(f"Error fetching document from {coll_name}: {doc_err}")
                                    break
                        except Exception as batch_err:
                            logger.log_to_file(f"Error batching documents in {coll_name}: {batch_err}")

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
                        except PyMongoError as write_err:
                            logger.error(f"Write error in {coll_name}: {write_err}")
                            break
                        except Exception as write_unexp:
                            logger.error(f"Unexpected error during write in {coll_name}: {write_unexp}")
                            break

                        migrated_count += len(docs)
                        progress.update(task, advance=len(docs))
                except Exception as migrate_err:
                    logger.error(f"Error migrating documents in {coll_name}: {migrate_err}")
                finally:
                    try:
                        cursor.close()
                    except Exception as close_err:
                        logger.error(f"Error closing cursor for {coll_name}: {close_err}")

                progress.advance(overall_task)
                logger.log_to_file(level="info", message=f"✅ Completed {coll_name}: {migrated_count}/{total_docs}")

            if not _stop:
                logger.info("🎉 Migration complete.")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
    finally:
        _safe_close(source_client, "source")
        _safe_close(target_client, "target")
        logger.log_to_file(level="info", message=f"==============================================================")
