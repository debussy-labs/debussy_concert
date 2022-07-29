[![GitHub issues](https://img.shields.io/github/issues/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/issues)
[![GitHub forks](https://img.shields.io/github/forks/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/network)
[![GitHub stars](https://img.shields.io/github/stars/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/stargazers)
[![GitHub license](https://img.shields.io/github/license/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/blob/master/LICENSE)

<img src="https://github.com/DotzInc/debussy_concert/blob/master/docs/images/banner_debussy.png"/>

# Debussy Concert
Abstraction layers for Apache Airflow with musical theme. Depends on debussy_framework

Mount examples folder on airflow dags folder


## Overview

The framework's philosophy is to persist¹ the data after each transformation.

With this philosophy in mind, the framework prioritizes ELT (Extract, Load, Transform) over ETL.

Data ingestion pipelines must implement a step of extracting the source data and persisting it in the Raw Vault layer on parquet whenever the data is loaded (duplicating the data when necessary), without making transformations to the content. Some adjusments to the data may be necessary to load the data in the parquet format. Then the file is loaded into the raw layer, removing the duplicate processing (data may still have duplicates!)

The Reverse ETL pipeline in turn must extract the data from the data lakehouse and take it to the Reverse ETL dataset, where all the data must be persisted after the actual transformations. After that, the data must be taken to a storage layer in the format closest to what it will be sent to the destination. For example, in case of sending a CSV file to an SFTP, the CSV file that will be sent must be saved, in case of making a call in a REST endpoint, the body of the request must be saved as JSON. When the Reverse ETL targets another database, the .SQL with the record insertion code must be saved.

The Transformation pipeline (still to be developed) will be done using dbt software (Data Build Tool)

¹ Persisted data can be discarded after some time, for example after 6 months it can go to cheaper storage and after 2 years it can be removed from a certain layer

## Full Documentation
See the [Wiki](https://github.com/DotzInc/debussy_concert/wiki) for full documentation, examples, operational details and other information.

## Communication
[GitHub Issues](https://github.com/DotzInc/debussy_concert/issues)

## License
Copyright 2022 Dotz, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
