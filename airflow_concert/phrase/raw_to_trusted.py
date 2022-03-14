from airflow_concert.phrase.phrase_base import PhraseBase
from airflow_concert.motif.bigquery_to_bigquery import BigQueryToBigQueryMotif


class BigQueryRawToBigQueryTrustedPhrase(PhraseBase):
    def __init__(self, config, name=None) -> None:
        super().__init__(name=name,
                         motifs=[BigQueryToBigQueryMotif(config)])
