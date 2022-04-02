from debussy_concert.reverse_etl.movement.reverse_etl import ReverseEtlMovement

from tests.resource.core_for_testing import DummyMotif, DummyPhrase, DummyDag


def create_empty_phrase(name):
    motif = DummyMotif(name=f'{name}_motif')
    return DummyPhrase(name=name, motifs=[motif])


def retl_movement():
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


def test_data_warehouse_to_reverse_etl_phrase(inject_testing):
    movement = retl_movement()
    phrase = movement.data_warehouse_to_reverse_etl_phrase
    # simply execute the query given
    assert phrase._setup['reverse_etl_query'] == 'reverse_etl_query_test'
    # build the uri from {project}.{dataset}.{composition_name}_{extraction_name}
    assert phrase._setup['reverse_etl_table_uri'] == ('project_test.'
                                                      'reverse_etl_dataset_test.'
                                                      'composition_name_'
                                                      'test_extraction_name_test')
