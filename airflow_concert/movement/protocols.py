from typing import Protocol

#
# Phrase Protocols
#


class PStartPhrase(Protocol):
    pass


class PIngestionSourceToLandingStoragePhrase(Protocol):
    pass


class PLandingStorageToDataWarehouseRawPhrase(Protocol):
    pass


class PDataWarehouseRawToTrustedPhrase(Protocol):
    pass


class PEndPhrase(Protocol):
    pass
