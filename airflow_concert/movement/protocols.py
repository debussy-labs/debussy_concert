from typing import Protocol
from airflow_concert.phrase.phrase_base import PPhrase
#
# Phrase Protocols
#


class PStartPhrase(PPhrase, Protocol):
    pass


class PIngestionSourceToLandingStoragePhrase(PPhrase, Protocol):
    pass


class PLandingStorageToDataWarehouseRawPhrase(PPhrase, Protocol):
    pass


class PDataWarehouseRawToTrustedPhrase(PPhrase, Protocol):
    pass


class PEndPhrase(PPhrase, Protocol):
    pass
