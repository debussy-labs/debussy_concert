# AirflowConcert
Abstraction layers for Apache Airflow with musical theme

Mount examples folder on airflow dags folder

```mermaid
classDiagram
class CompositionBase {
    <<interface>>
    ConfigIntegration config
    build(Callable movement_builder) DAG
}

class MovementBase {
    <<interface>>
    String name
    MovementBase[] phrases
    build(DAG dag) TaskGroup
}

class PhraseBase{
    <<interface>>
    String name
    MotifBase[] motifs
    build(DAG dag, TaskGroup task_group) TaskGroup
}

class MotifBase {
    <<interface>>
    String name
    ConfigIntegration config
    build(DAG dag, TaskGroup task_group) TaskMixin
}

CompositionBase --> MovementBase
MovementBase --> PhraseBase
PhraseBase --> MotifBase
```