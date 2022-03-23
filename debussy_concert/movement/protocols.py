from typing import Protocol
from debussy_concert.phrase.protocols import (PExportDataToStorageMotif,
                                              PCreateExternalTableMotif,
                                              PMergeTableMotif)
from debussy_concert.phrase.phrase_base import PPhrase
from debussy_concert.config.config_composition import ConfigComposition
from debussy_concert.entities.table import Table
#
# Phrase Protocols
#


class PStartPhrase(PPhrase, Protocol):
    pass


class PIngestionSourceToLandingStoragePhrase(PPhrase, Protocol):
    export_data_to_storage_motif: PExportDataToStorageMotif

    def setup(self, destination_storage_uri):
        pass


class PLandingStorageToDataWarehouseRawPhrase(PPhrase, Protocol):
    create_external_table_motif: PCreateExternalTableMotif
    merge_table_motif: PMergeTableMotif

    def setup(self, config: ConfigComposition, table: Table,
              source_storage_uri_prefix, datawarehouse_raw_uri):
        pass


class PDataWarehouseRawToTrustedPhrase(PPhrase, Protocol):
    pass


class PEndPhrase(PPhrase, Protocol):
    pass
