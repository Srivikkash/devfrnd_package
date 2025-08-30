import click
from devfrnd.db_migration_utils.db_migrator import migrate
from devfrnd import __version__

@click.group()
@click.version_option(__version__, "-v", "--version", message="devfrnd version %(version)s")
@click.help_option("-h", "--help")
def cli():
    """
        Developer Friend CLI Tool
        
        (A collection of utilities for developers)
        
        Use --help after any command to see usage instructions.
        
        Example: devfrnd db_migrator --help
        
        More commands coming soon!    
    """
    pass

cli.add_command(migrate)

if __name__ == "__main__":
    cli()