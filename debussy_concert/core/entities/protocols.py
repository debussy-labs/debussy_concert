from typing import Protocol


class PGroup(Protocol):
    pass


class PMovementGroup(PGroup):
    pass


class PPhraseGroup(PGroup):
    pass


class PMotifGroup(PGroup):
    pass


class PWorkflowDag(Protocol):
    pass


class PConfigComposition(Protocol):
    def load_from_file(cls, composition_config_file_path, env_file_path):
        pass
