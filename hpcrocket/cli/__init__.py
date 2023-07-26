from typing import List, Union

from hpcrocket.core.filesystem import Filesystem
from hpcrocket.core.launchoptions import Options

from ._builders import create_options
from ._parsers import get_parser
from ._yaml import ParseError


def parse_cli_args(
    args: List[str], filesystem: Filesystem
) -> Union[Options, ParseError]:
    parser = get_parser()
    config = parser.parse_args(args)
    return create_options(config, filesystem)


__all__ = ["parse_cli_args", "ParseError"]
