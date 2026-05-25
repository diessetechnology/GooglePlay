import sys
import warnings

from airbyte_cdk.entrypoint import launch

from .source import SourceGooglePlayConsole


def main() -> None:
    warnings.filterwarnings("ignore")
    launch(SourceGooglePlayConsole(), sys.argv[1:])
