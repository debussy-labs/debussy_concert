from debussy_concert.reverse_etl.config.movement_parameters.reverse_etl import CsvFile, ReverseEtlMovementParameters

from tests.test_movement.reverset_etl_movement import csv_retl_movement


def test_csv_retl_movement_isinstances(inject_testing):
    movement = csv_retl_movement()
    retl_mov_param: ReverseEtlMovementParameters = movement.config.movements_parameters[0]
    output_config: CsvFile = retl_mov_param.output_config
    assert isinstance(retl_mov_param, ReverseEtlMovementParameters)
    assert isinstance(output_config, CsvFile)


def test_data_warehouse_to_reverse_etl_phrase(inject_testing):
    movement = csv_retl_movement()
    phrase = movement.data_warehouse_to_reverse_etl_phrase
    # simply execute the query given
    assert phrase._setup['reverse_etl_query'] == 'reverse_etl_query_test'
    # build the uri from {project}.{dataset}.{composition_name}_{extraction_name}
    assert phrase._setup['reverse_etl_table_uri'] == (
        'project_test.'
        'reverse_etl_dataset_test.'
        'composition_name_'
        'test_extraction_name_test')


def test_data_warehouse_reverse_etl_to_storage_phrase(inject_testing):
    movement = csv_retl_movement()
    phrase = movement.data_warehouse_reverse_etl_to_storage_phrase
    assert phrase._setup['extract_query'] == 'extract_query_from_temp_test'
    assert phrase._setup['storage_uri_prefix'] == (
        "gs://reverse_etl_bucket_test/"
        "composition_name_test/extraction_name_test/file_name_test.format")


def test_storage_to_destination_phrase(inject_testing):
    movement = csv_retl_movement()
    phrase = movement.storage_to_destination_phrase
    assert phrase._setup['storage_uri_prefix'] == (
        "gs://reverse_etl_bucket_test/"
        "composition_name_test/extraction_name_test/file_name_test.format")
