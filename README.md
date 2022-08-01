[![GitHub issues](https://img.shields.io/github/issues/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/issues)
[![GitHub forks](https://img.shields.io/github/forks/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/network)
[![GitHub stars](https://img.shields.io/github/stars/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/stargazers)
[![GitHub license](https://img.shields.io/github/license/DotzInc/debussy_concert)](https://github.com/DotzInc/debussy_concert/blob/master/LICENSE)


<img align="right" src="https://github.com/DotzInc/debussy_concert/blob/master/docs/images/debussy_logo.png" width="250" height="250">

# Debussy Concert

Debussy Concert allows users to build first class data pipelines without needing to code. It's the core component of [Debussy](https://github.com/DotzInc/debussy_concert/wiki), a free, open-source, opinionated Data Architecture and Engineering framework, enabling data analysts and engineers to build better platforms and pipelines.

Debussy Concert is a code generation engine for orchestration tools, currently supporting [Apache Airflow](https://airflow.apache.org/) only, but with others on the Roadmap. It provides abstraction layers in the form of a musical themed sementic model, decoupling the pipeline logic to the underlying orchestration tool, and enabling a low-code approach to data engineering.

Key benefits:

&#10004; It provides lower time to delivery and costs related to data pipeline development, while enabling higher ROI <br />
&#10004; Avoid pipeline debt by following sound software engineering design principles <br />
&#10004; Ensure your platform is following data architecture best practices <br />

## Quick Start

Debussy works on any installation of Apache Airflow 2.0, but since we currently support only GCP based data platforms as the target Data Lakehouse, we recommend a deployment to [Cloud Composer](https://cloud.google.com/composer).

In order to use Debussy, you first need to go through the following steps:

1. [Select or create a Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager).
2. [Enable billing for your project](https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project).
3. [Create a Cloud Composer 2 environment](https://cloud.google.com/composer/docs/composer-2/create-environments).
4. Install Debussy on your Cloud Composer instance.

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
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://raw.githubusercontent.com/apache/airflow/master/docs/apache-airflow/img/logos/wordmark_1.png" /></td><td style="width: 200px;">Apache Airflow           </td><td>An open source orchestration engine</td></tr>
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://spark.apache.org/images/spark-logo-trademark.png" />                             </td><td style="width: 200px;">Spark                    </td><td>Open source distributed processing engine, used for batch ingestions</td></tr>
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://beam.apache.org/images/beam_logo_navbar.png" />                             </td><td style="width: 200px;">Apache Beam                    </td><td>Open source distributed processing engine, used for realtime ingestions</td></tr>
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://assets.website-files.com/60d5e12b5c772dbf7315804e/62cddd0e6400a93d1dbcdf37_Google%20Cloud%20Storage.svg" />   </td><td style="width: 200px;">Google Cloud Storage                   </td><td>Cloud based blob storage</td></tr>
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://raw.githubusercontent.com/gist/nelsonauner/be8160f2e576a327bfcde085b334f622/raw/b4ec25dd4d698abdc37e6c1887ec69ddcca1d27d/google_bigquery_logo.svg" /></td><td style="width: 200px;">BigQuery</td><td>Google serverless massive-scale SQL analytics platform</td></tr>
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://www.mysql.com/common/logos/powered-by-mysql-167x86.png" />                       </td><td style="width: 200px;">MySQL                    </td><td>Leading open source database</td></tr>
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://wiki.postgresql.org/images/3/30/PostgreSQL_logo.3colors.120x120.png" />          </td><td style="width: 200px;">PostgreSQL                 </td><td>Leading open source database</td></tr>
<tr><td style="text-align: center; height=40px;"><img height="40" src="https://www.oracle.com/a/ocom/img/jdbc.svg" />                                         </td><td style="width: 200px;">Other SQL Relational DBs </td><td>Most RDBMS are supported via JDBC drivers thorugh Spark</td></tr>		
<tr><td style="text-align: center; height=40px;"><img height="40" src="https://braze-marketing-assets.s3.amazonaws.com/images/partner_logos/amazon-s3.png" />   </td><td style="width: 200px;">AWS S3                   </td><td>Cloud based blob storage</td></tr>
		<tr><td style="text-align: center; height=40px;"><img height="40" src="https://cdn.brandfolder.io/5H442O3W/as/pl546j-7le8zk-5guop3/Slack_RGB.png" />            </td><td style="width: 200px;">Slack                    </td><td> Get automatic data pipeline notifications!</td></tr>
	</tbody>
</table>

## Full Documentation
See the [Wiki](https://github.com/DotzInc/debussy_concert/wiki) for full documentation, examples, operational details and other information.

## Communication
[GitHub Issues](https://github.com/DotzInc/debussy_concert/issues)

## License
Copyright 2022 Dotz, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
