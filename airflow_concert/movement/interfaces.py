from abc import ABC

#
# Phrase Interfaces
#


class StartPhrase(ABC):
    pass


class IngestionSourceToLandingStoragePhrase(ABC):
    pass


class LandingStorageToDataWarehouseRawPhrase(ABC):
    pass


class DataWarehouseRawToTrustedPhrase(ABC):
    pass


class EndPhrase(ABC):
    pass


#
# Motif Interfaces
#
