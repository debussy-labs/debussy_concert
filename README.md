[![GitHub issues](https://img.shields.io/github/issues/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/issues)
[![GitHub forks](https://img.shields.io/github/forks/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/network)
[![GitHub stars](https://img.shields.io/github/stars/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/stargazers)
[![GitHub license](https://img.shields.io/github/license/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/blob/master/LICENSE)


<img align="right" src="https://github.com/DotzInc/debussy_concert/blob/master/docs/images/debussy_logo.png" width="250" height="250">

# Debussy Concert

[Debussy](https://github.com/DotzInc/debussy_concert/wiki) is a free, open-source, opinionated Data Architecture and Engineering framework. It enables data analysts and engineers to build better data platforms through first class data pipelines, following a low-code and self-service approach. 

<p align="center">
  <a href="#description">Description</a>
  <span> · </span>
  <a href="#key-features">Key Features</a>
  <span> · </span>
  <a href="#key-benefits">Key Benefits</a>
  <span> · </span>
  <a href="#quick-start">Quick Start</a>
  <span> · </span>
  <a href="#integrations">Integrations</a>
  </br>
  <a href="#full-documentation">Full Documentation</a>
  <span> · </span>
  <a href="#communication">Communication</a>
  <span> · </span>
  <a href="#contributions">Contributions</a>
  <span> · </span>
  <a href="#license">License</a>
</h3>

---

## Description

In the data engineering field, everyone is reinventing the wheel all the time – it's still rare to see the adoption of software engineering best practices, such as [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself), [KISS](https://en.wikipedia.org/wiki/KISS_principle) or [YAGNI](https://en.wikipedia.org/wiki/You_aren%27t_gonna_need_it). Despite the existence of several tools for data orchestration (e.g. [Apache Airflow](https://airflow.apache.org/), [Prefect](https://www.prefect.io/), [Dagster](https://dagster.io/)) and distributed data processing (e.g. [Apache Spark](https://spark.apache.org/), [Apache Beam](https://beam.apache.org/)), every time a new data pipeline demand arises it usually implies lengthy development projects. Think of developing a web application without the help of a web framework such as [Django](https://www.djangoproject.com/) or [Flask](https://palletsprojects.com/p/flask/)!

What's even worse, although sharing key concepts, these data orchestration tools have very distinct syntaxes and features, making migrations a daunting task! Moreover, simply adopting these tools does not guarantee that best practices are being followed, including with regard to data architecture (think of data modeling, data management lifecycle, among others).

While lots of companies have faced these same issues, most of them have decided to develop their own in-house solutions, missing the opportunity for colaboration and wider adoption of data architecture and sofware engineering best practices.

With that in mind, we created Debussy! Debussy Concert is the core component of Debussy. It's a code generation engine for orchestration tools, currently supporting only Airflow, but with others on the Roadmap. It provides abstraction layers in the form of a musical themed semantic model, decoupling the pipeline logic to the underlying orchestration tool, and enabling a low-code approach to data engineering. We also provides pipelines templates (e.g. data ingestion, data transformation and reverse ETL) built with our engine, while always striving to offer the aforementioned best practices.

## Key Features
- Dynamic data pipeline generation from YAML configuration files or directly through Python
- Provides a semantic model for data pipeline development, abstracting the inner orchestration engine
- Enables seamless integration of first class data projects, such as Airflow, Spark, and dbt

## Key Benefits

&#10004; It provides lower time to delivery and costs related to data pipeline development, while enabling higher ROI <br />
&#10004; Avoid pipeline debt by following sound software engineering design principles <br />
&#10004; Ensure your platform is following data architecture best practices <br />

## Quick Start

Debussy works on any installation of Apache Airflow 2.0, but since we currently support only GCP based data platforms as the target Data Lakehouse, we recommend a deployment to [Cloud Composer](https://cloud.google.com/composer).

In order to use Debussy, you first need to go through the following steps:

1. [Select or create a Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager).
2. [Enable billing for your project](https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project).
3. [Create a Cloud Composer 2 environment](https://cloud.google.com/composer/docs/composer-2/create-environments).
4. Install Debussy on your Cloud Composer instance: just upload the project to your `plugins/` folder.
5. Check our [User's Guide](https://github.com/DotzInc/debussy_concert/wiki/User's-Guide) and [examples](https://github.com/DotzInc/debussy_concert/tree/master/examples) to learn how to use it!

Integrations
-------------------------------------------------------------------------------
Debussy works with the tools and systems that you're already using with your data, including:

<table>
	<thead>
		<tr>
			<th colspan="2">Integration</th>
			<th>Notes</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://raw.githubusercontent.com/apache/airflow/master/docs/apache-airflow/img/logos/wordmark_1.png" /></td>
			<td style="width: 200px;">Apache Airflow</td>
			<td>An open source orchestration engine</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://spark.apache.org/images/spark-logo-trademark.png" /></td>
			<td style="width: 200px;">Spark</td>
			<td>Open source distributed processing engine, used for the data ingestion pipelines</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://www.getdbt.com/ui/img/logos/dbt-logo.svg" /></td>
			<td style="width: 200px;">dbt</td>
			<td>dbt is an open-source data transformation tool, used for the data transformation pipelines</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://assets.website-files.com/60d5e12b5c772dbf7315804e/62cddd0e6400a93d1dbcdf37_Google%20Cloud%20Storage.svg" /></td>
			<td style="width: 200px;">Google Cloud Storage</td>
			<td>Cloud based blob storage, supported as data source or destination</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://raw.githubusercontent.com/gist/nelsonauner/be8160f2e576a327bfcde085b334f622/raw/b4ec25dd4d698abdc37e6c1887ec69ddcca1d27d/google_bigquery_logo.svg" /></td>
			<td style="width: 200px;">BigQuery</td>
			<td>Google serverless massive-scale SQL analytics platform, supported as the analytical environment (aka. Data Lakehouse)</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://www.mysql.com/common/logos/powered-by-mysql-167x86.png" /></td>
			<td style="width: 200px;">MySQL</td>
			<td>Leading open source database, supported as a data source</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://wiki.postgresql.org/images/3/30/PostgreSQL_logo.3colors.120x120.png" /></td>
			<td style="width: 200px;">PostgreSQL</td>
			<td>Leading open source database, supported as a data source</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://www.oracle.com/a/ocom/img/jdbc.svg" /></td>
			<td style="width: 200px;">Other SQL Relational DBs</td>
			<td>Most RDBMS are supported as data sources via JDBC drivers through Spark</td>
		</tr>
		<tr>
			<td style="text-align: center; height=40px;"><img height="40" src="https://braze-marketing-assets.s3.amazonaws.com/images/partner_logos/amazon-s3.png" /></td>
			<td style="width: 200px;">AWS S3</td>
			<td>Cloud based blob storage, supported as data source or destination</td>
		</tr>
	</tbody>
</table>


## Full Documentation
See the [Wiki](https://github.com/DotzInc/debussy_concert/wiki) for full documentation, examples, operational details and other information.

## Communication
[GitHub Issues](https://github.com/DotzInc/debussy_concert/issues)

[Discord Server](https://discord.gg/FpNX79pY)

## Contributions
We welcome all community contributions!

In order to have a more open and welcoming community, Debussy adheres to a [code of conduct](https://github.com/DotzInc/debussy_concert/wiki/Code-of-Conduct) adapted from Contributor Covenant.

Please read through our [contributing guidelines](https://github.com/DotzInc/debussy_concert/wiki/Contributing-Guide). Included are directions for opening issues, coding standards, and notes on development.

## License
Copyright 2022 Dotz, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
