# AirflowConcert
Abstraction layers for Apache Airflow with musical theme

Mount examples folder on airflow dags folder

```mermaid
classDiagram
class CompositionBase {
    ConfigIntegration config
    build(movement_callable) DAG
}

class MovementBase {
    source_info
    String name
    ConfigIntegration config
    MovementBase[] phrases
    build(dag) TaskGroup
}

class PhraseBase{
    String name
    ConfigIntegration config
    MotifBase[] motifs
    build(dag, task_group) TaskGroup
}

class MotifBase {
    String name
    ConfigIntegration config
    build(dag, task_group) TaskMixin
}

CompositionBase --> MovementBase
MovementBase --> PhraseBase
PhraseBase --> MotifBase
```