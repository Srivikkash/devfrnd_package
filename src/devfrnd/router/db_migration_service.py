import sys
import click
from devfrnd.db_migration_utils.db_migrator import _migrate
from devfrnd.base_utils.logger_utils import RichLogger

logger = RichLogger()

@click.command("db_migrator")
@click.help_option("-h", "--help")
@click.option("--to_local", "-tl", "direction", flag_value="to_local", required=True,
    help="Migrate from cloud to local")
# @click.option("--to_cloud", "-tc", "direction", flag_value="to_cloud", required=True,
#     help="Migrate from local to cloud")
# @click.option("--cloud_to_cloud", "-cc", "direction", flag_value="cloud_to_cloud", required=True,
#     help="Migrate from cloud to cloud (requires --connect-string-target)")
@click.argument("connection_str", type=str)
@click.argument("db_name", type=str)
@click.option("--percentage", "-p", type=int, default=100, help="Percentage of data to migrate (1-100)")
@click.option("--batch-size", "-b", type=int, default=1000, help="Batch size for inserts")
@click.option("--connect-string-target", "-cts", type=str, default=None, help="Target connection string (for cloud_to_cloud)")
def migrate(direction, connection_str, db_name, percentage, batch_size, connect_string_target=None):
    """Migrate MongoDB data between cloud and local.

    \b
    Examples:
      devfrnd migrate --to_local "<CLOUD_URI>" mydb --percentage 50
      devfrnd migrate --to_cloud "<CLOUD_URI>" mydb --batch-size 2000 (not active)
      devfrnd migrate --cloud_to_cloud "<SOURCE_CLOUD_URI>" mydb --connect-string-target "<TARGET_CLOUD_URI>" (not active)
    """

    try:
        if not (1 <= percentage <= 100):
            logger.error("Percentage must be between 1 and 100")
            sys.exit(1)

        if direction.lower() == "to_local":
            source_uri, target_uri = connection_str, "mongodb://localhost:27017/"
        # elif direction.lower() == "to_cloud":
        #     source_uri, target_uri = "mongodb://localhost:27017/", connection_str
        # elif direction.lower() == "cloud_to_cloud":
        #     if not connect_string_target:
        #         logger.error("Target connection string is required for cloud_to_cloud migration")
        #         sys.exit(1)
        #     source_uri, target_uri = connection_str, connect_string_target
        else:
            logger.error(f"Unknown migration direction: {direction}")
            sys.exit(1)

        try:
            _migrate(source_uri, target_uri, db_name, percentage, batch_size)
            logger.info("Migration completed successfully.")
        except Exception as migrate_err:
            logger.error(f"Migration failed: {migrate_err}")
            sys.exit(1)

    except Exception as err:
        logger.error(f"Unexpected error: {err}")
        sys.exit(1)