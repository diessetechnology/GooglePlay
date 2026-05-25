import sys

from airbyte_cdk.entrypoint import launch

from .source import SourceGooglePlayConsole


def main() -> None:
    launch(SourceGooglePlayConsole(), sys.argv[1:])
