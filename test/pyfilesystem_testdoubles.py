from typing import Callable, Optional, Text

import fs.base
import fs.subfs
from fs.memoryfs import MemoryFS


class ArbitraryArgsMemoryFS(MemoryFS):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()


class OnlySubFSMemoryFS(ArbitraryArgsMemoryFS):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def opendir(self, path: Text, factory: Optional[Callable[['OnlySubFSMemoryFS', Text], fs.subfs.SubFS['OnlySubFSMemoryFS']]] = None) -> fs.subfs.SubFS['OnlySubFSMemoryFS']:
        return super().opendir(path, factory=fs.subfs.SubFS)
