import sys
import os
import importlib
import click
from devfrnd.flask_helper_utils.app_endpoints_fetcher import flask_endpoint_fetcher
from devfrnd.base_utils.logger_utils import RichLogger

logger = RichLogger()

@click.command("flask_endpoints", help="Fetch and display Flask app endpoints (in testing phase)")
@click.argument("app_import_path", type=str, required=True, metavar="<app_import_path>")
@click.option("--output", "-o", default="excel", type=click.Choice(["excel", "console", "json"]), help="Output format (default: excel)")
@click.option("--get", "-g", is_flag=True, help="To Fetch GET methods")
@click.option("--post", "-p", is_flag=True, help="To Fetch POST methods")
@click.option("--put", "-u", is_flag=True, help="To Fetch PUT methods")
@click.option("--delete", "-d", is_flag=True, help="To Fetch DELETE methods")
@click.option("--patch", "-a", is_flag=True, help="To Fetch PATCH methods")
@click.option("--blueprint-name", "-b", default="", help="Filter by blueprint name")
def flask_endpoints(app_import_path, output, get=False, post=False, put=False, delete=False, patch=False, blueprint_name=""):
    """
    CLI command to fetch and display Flask app endpoints.
    """
    try:
        # Ensure current working directory is in sys.path for dynamic import
        cwd = os.getcwd()
        logger.log_to_file(level="info", message=f"Current working directory: {cwd}")
        if cwd not in sys.path:
            sys.path.insert(0, cwd)

        if ":" not in app_import_path:
            logger.error("app_import_path must be in the format 'module:attribute'")
            sys.exit(1)
        module_name, app_attr = app_import_path.split(":")

        try:
            module = importlib.import_module(module_name)
            logger.log_to_file(level="info", message=f"Successfully imported module '{module_name}'")
        except ImportError as e:
            logger.error(f"Could not import module '{module_name}': {e}")
            sys.exit(1)

        try:
            app_obj = getattr(module, app_attr, None)
            logger.log_to_file(level="info", message=f"Retrieved attribute '{app_attr}' from module '{module_name}'")
            print(app_obj)
            if app_obj is None:
                logger.error(f"Could not find attribute '{app_attr}' in module '{module_name}'")
                sys.exit(1)
            app = app_obj
            if not app:
                logger.error(f"Could not instantiate Flask app from {app_import_path}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Error while retrieving or instantiating Flask app: {e}")
            sys.exit(1)

        try:
            flask_endpoint_fetcher(
                app=app,
                get=get,
                post=post,
                put=put,
                delete=delete,
                patch=patch,
                blueprint_name=blueprint_name,
                output=output
            )
        except Exception as e:
            logger.error(f"Error while fetching Flask endpoints: {e}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
