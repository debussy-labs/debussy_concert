# AirflowConcert
Abstraction layers for Apache Airflow with musical theme

Mount examples folder on airflow dags folder

```mermaid
classDiagram
class Compositor {
    composition_factory(ConfigIntegration) CompositionBase
}
class CompositionBase {
    ConfigIntegration config
    CompositionBase[] movements
    build() DAG
}
class MovementBase{
    String name
    ConfigIntegration config
    PhraseBase[] phrases
    build(dag) TaskGroup
}
class PhraseBase {
    String name
    ConfigIntegration config
    build(dag, task_group) TaskMixin
}
CompositionBase --> MovementBase
MovementBase --> PhraseBase
Compositor ..|> CompositionBase
```