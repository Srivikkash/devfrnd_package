import click
from devfrnd.router.db_migration_service import migrate
from devfrnd.router.flask_helper_service import flask_endpoints
from devfrnd import __version__

@click.group()
@click.version_option(__version__, "-v", "--version", message="devfrnd version %(version)s")
@click.help_option("-h", "--help")
def cli():
    """
        Developer Friend CLI Tool
        
        (A collection of utilities for developers)
        
        Use --help after any command to see usage instructions.
        
        Available Commands:
            
            migrate          Database migration utility
            
            flask_endpoints  Fetch and display Flask app endpoints
        
            More commands coming soon!    
    """
    pass

cli.add_command(migrate)
cli.add_command(flask_endpoints)

if __name__ == "__main__":
    cli()