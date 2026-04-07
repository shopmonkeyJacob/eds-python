"""Allow `python -m eds` as an entry point."""

from eds.cli.main import cli

if __name__ == "__main__":
    cli()
