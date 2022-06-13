from tests.test_reverse_etl.create_test_movement import csv_reverse_etl_movement
from debussy_concert.reverse_etl.movement.reverse_etl import ReverseEtlMovement


def test_DataWarehouseToReverseEtlPhrase(inject_testing):
    movement: ReverseEtlMovement = csv_reverse_etl_movement()
    motif = movement.data_warehouse_to_reverse_etl_phrase.dw_to_reverse_etl_motif
    assert motif._setup['sql_query'] == 'reverse_etl_query_test'
    assert motif._setup['destination_table'] == (
        'project_test.'
        'reverse_etl_dataset_test.'
        'composition_name_'
        'test_extraction_name_test')


def test_DataWarehouseReverseEtlToTempToStoragePhrase(inject_testing):
    movement: ReverseEtlMovement = csv_reverse_etl_movement()
    motif = movement.data_warehouse_reverse_etl_to_storage_phrase.datawarehouse_reverse_etl_to_temp_table_motif
    temp_staging_table = 'project_test.temp_dataset_test.composition_name_test_extraction_name_test'
    assert motif._setup['sql_query'] == 'extract_query_from_temp_test'
    assert motif._setup['destination_table'] == temp_staging_table
    motif = movement.data_warehouse_reverse_etl_to_storage_phrase.export_temp_table_to_storage_motif
    assert motif._setup['source_table_uri'] == temp_staging_table
    assert motif._setup['destination_uris'] == [('gs://reverse_etl_bucket_test/composition_name_test/'
                                                 'extraction_name_test/file_name_test.format')]


def test_StorageToDestinationPhrase(inject_testing):
    movement: ReverseEtlMovement = csv_reverse_etl_movement()
    motif = movement.storage_to_destination_phrase.storage_to_destination_motif
    assert motif._setup['storage_uri_prefix'] == ('gs://reverse_etl_bucket_test/composition_name_test/'
                                                  'extraction_name_test/file_name_test.format')
