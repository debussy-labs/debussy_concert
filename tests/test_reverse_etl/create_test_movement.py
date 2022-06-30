from debussy_concert.reverse_etl.movement.reverse_etl import ReverseEtlMovement

from tests.resource.core_for_testing import DummyDag, DummyMotif, create_empty_phrase


def csv_reverse_etl_movement_dummy_phrases():
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


def csv_reverse_etl_movement():
    from debussy_concert.reverse_etl.phrase.dw_to_reverse_etl import DataWarehouseToReverseEtlPhrase
    from debussy_concert.reverse_etl.phrase.reverse_etl_to_storage import DataWarehouseReverseEtlToTempToStoragePhrase
    from debussy_concert.reverse_etl.phrase.storage_to_destination import StorageToDestinationPhrase
    start_phrase = create_empty_phrase("start_phrase")
    end_phrase = create_empty_phrase("end_phrase")

    data_warehouse_to_reverse_etl_phrase = DataWarehouseToReverseEtlPhrase(
        dw_to_reverse_etl_motif=DummyMotif(name='dw_to_reverse_etl_motif'))
    data_warehouse_reverse_etl_to_storage_phrase = DataWarehouseReverseEtlToTempToStoragePhrase(
        datawarehouse_reverse_etl_to_temp_table_motif=DummyMotif(name='datawarehouse_reverse_etl_to_temp_table_motif'),
        export_temp_table_to_storage_motif=DummyMotif(name='export_temp_table_to_storage_motif'))
    storage_to_destination_phrase = StorageToDestinationPhrase(
        storage_to_destination_motif=DummyMotif(name='storage_to_destination_motif'))

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
    data_warehouse_reverse_etl_to_storage_phrase.datawarehouse_reverse_etl_to_temp_table_motif.__dict__
    # print(data_warehouse_reverse_etl_to_storage_phrase.__dict__)
    # print(data_warehouse_reverse_etl_to_storage_phrase.datawarehouse_reverse_etl_to_temp_table_motif.__dict__)
    movement.build(dag)
    return movement
