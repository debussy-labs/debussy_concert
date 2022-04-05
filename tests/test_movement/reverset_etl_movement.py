from debussy_concert.reverse_etl.movement.reverse_etl import ReverseEtlMovement

from tests.resource.core_for_testing import DummyDag, create_empty_phrase


def csv_retl_movement():
    start_phrase = create_empty_phrase("start_phrase")
    data_warehouse_to_reverse_etl_phrase = create_empty_phrase("data_warehouse_to_reverse_etl_phrase")
    data_warehouse_reverse_etl_to_storage_phrase = create_empty_phrase(
        "data_warehouse_reverse_etl_to_storage_phrase")
    storage_to_destination_phrase = create_empty_phrase("storage_to_destination_phrase")
    end_phrase = create_empty_phrase("end_phrase")
    movement = ReverseEtlMovement(
        start_phrase=start_phrase,
        data_warehouse_to_reverse_etl_phrase=data_warehouse_to_reverse_etl_phrase,
        data_warehouse_reverse_etl_to_storage_phrase=data_warehouse_reverse_etl_to_storage_phrase,
        storage_to_destination_phrase=storage_to_destination_phrase,
        end_phrase=end_phrase

    )
    retl_mov_param = movement.config.movements_parameters[0]
    movement.setup(movement_parameters=retl_mov_param)
    dag_params = movement.config.dag_parameters
    dag = DummyDag(**dag_params)
    movement.build(dag)
    return movement
