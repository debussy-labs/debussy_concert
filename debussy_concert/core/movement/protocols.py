from typing import Protocol
from debussy_concert.core.phrase.phrase_base import PPhrase
from debussy_concert.core.config.movement_parameters.base import MovementParametersType
#
# Phrase Protocols
#


class PStartPhrase(PPhrase, Protocol):
    pass


class PIngestionSourceToLandingStoragePhrase(PPhrase, Protocol):

    def setup(self, destination_storage_uri):
        pass


class PLandingStorageToDataWarehouseRawPhrase(PPhrase, Protocol):

    def setup(self, movement_parameters: MovementParametersType,
              source_storage_uri_prefix, datawarehouse_raw_uri):
        pass


class PEndPhrase(PPhrase, Protocol):
    pass
