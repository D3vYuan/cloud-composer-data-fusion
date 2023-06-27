<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<div id="top"></div>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <h2 align="center">Cloud Composer</h2>
  <p align="center">
    Case Study - Orchestrating Data Fusion
  </p>
</div>

---

<!-- TABLE OF CONTENTS -->

## Table of Contents
<!-- <details> -->
<ol>
<li>
    <a href="#about-the-project">About The Project</a>
</li>
<li>
    <a href="#setup">Setup</a>
    <ul>
    <li><a href="#data-fusion-environment">Data Fusion Environment</a></li>
    <li><a href="#cloud-composer-environment">Cloud Composer Environment</a></li>
    </ul>
</li>
<li>
    <a href="#implementation">Implementation</a>
    <ul>
    <li><a href="#data-fusion-pipeline">Data Fusion Pipeline</a></li>
    <li><a href="#cloud-composer-pipeline">Cloud Composer Pipeline</a></li>
    </ul>
</li>
<li><a href="#usage">Usage</a>
    <ul>
    <li><a href="#data-fusion-run">Data Fusion Run</a></li>
    <li><a href="#cloud-composer-run">Cloud Composer Run</a></li>
    </ul>
</li>
<li><a href="#challenges">Challenges</a></li>
<li><a href="#acknowledgments">Acknowledgments</a></li>
</ol>
<!-- </details> -->

---

<!-- ABOUT THE PROJECT -->
## About The Project

This project is created to showcase how we can leverage `Cloud Composer` to spinoff `Cloud Data Fusion` jobs. 

The following are some of the requirements:
* Check the status of the data fusion pipeline
* Retry when the data fusion pipeline fails

<p align="right">(<a href="#top">back to top</a>)</p>

---

<!-- Setup -->
## Setup

Base on the requirements, the following are required for the proof of concept:
* `Data Fusion` - Simulate reading data from `CloudSQL` to `BigQuery`
* `Cloud Composer` - Trigger and monitor `Data Fusion` pipelines and enable retry on fail status

<p align="right">(<a href="#top">back to top</a>)</p>

---

### Data Fusion Environment

In order to create the data fusion pipeline, we need to perform the following
* Enable `Data Fusion` API <br/>
APIs & Services > Enabled APIs & Services > Search for `Cloud Data Fusion API` > Enable it 
<br/><br/>
![Data Fusion API][data-fusion-api] 
<br/><br/>

* Service Account for `Data Fusion` <br/>
IAM & Admin > IAM > Select *Include Google-provided role grants* > Ensure that `xxxxx@gcp-sa-datafusion.iam.gserviceaccount.com` has the `Cloud Data Fusion API Service Agent` permission
<br/><br/>
![Data Fusion Service Account][data-fusion-service-account] 
<br/><br/>

* Create `Data Fusion` Instance <br/>
Data Fusion > Instances > Create Instance
<br/><br/>
![Data Fusion Instance][data-fusion-instance]
<br/><br/>


<p align="right">(<a href="#top">back to top</a>)</p>

---

### Cloud Composer Environment

In order to create the cloud composer pipeline, we need to perform the following
* Enable `Cloud Composer` API <br/>
APIs & Services > Enabled APIs & Services > Search for `Cloud Composer API` > Enable it 
<br/><br/>
![Cloud Composer API][cloud-composer-api] 
<br/><br/>

* Service Account for `Cloud Composer` <br/>
IAM & Admin > IAM > Select *Include Google-provided role grants* > Ensure that `xxxxx@cloudcomposer.accounts.iam.gserviceaccount.com` has the `Cloud Composer API Service Agent` permission
<br/><br/>
![Cloud Composer Service Account][cloud-composer-service-account] 
<br/><br/>

* Create `Cloud Composer` Instance <br/>
Cloud Composer > Create
<br/><br/>
![Cloud Composer Instance][cloud-composer-instance]
<br/><br/>

<p align="right">(<a href="#top">back to top</a>)</p>

---

## Implementation
Base on the requirements, the following are the pipelines to be built:
* `Data Fusion` Pipeline - Simulate reading data from `CloudSQL` to `BigQuery`
* `Cloud Composer` Pipeline - Trigger and monitor `Data Fusion` pipelines and enable retry on fail status

<p align="right">(<a href="#top">back to top</a>)</p>

---

### Data Fusion Pipeline

The following are the steps for the `Data Fusion` Pipeline:

<br/>

![Data Fusion Pipeline][data-fusion-pipeline]

<br/>

| Pipeline Name | Description | Dependencies
| ----------- | ----------- | ----------- |
| `Read MySQL` | Read data from `CloudSQL` base on runtime argument |
| `Add Processed Columns` | Add a processed time to the data for tracking of ingestion | _Read MySQL_
| `Save To Cloud Storage` | Save the data to a file in `Cloud Storage` for tracking| _Add Processed Columns_
| `Import To BigQuery` | Save the data to `BigQuery` for analytics | _Add Processed Columns_
| `Update watermark` | Update the last processed time so that incremental data can be query the next run| _Import To BigQuery_

<br/>

<p align="right">(<a href="#top">back to top</a>)</p>

---

### Cloud Composer Pipeline

The following are the steps for the `Cloud Composer` Pipeline:

<br/>

![Cloud Composer Pipeline][cloud-composer-pipeline]

<br/>

| Pipeline Name | Operators | Description | Dependencies
| ----------- | ----------- | ----------- | ----------- |
|`start`|EmptyOperator|Just an indicator for start of pipeline|
|`truncate table`|BigQueryInsertJobOperator|Empty the output table in `BigQuery` before loading in new data|_start_
|`get configuration`|BigQueryGetDataOperator|Get all the configurations stored in `BigQuery`|_truncate_table_
|`filter arguments`|PythonOperator|Filter only active configuration based on the *is_active* column|_get configuration_
|`start_pipelines`|TaskGroupOperator,<br/>CloudDataFusionStartPipelineOperator|Schedule and Run the `Data Fusion` Pipeline with Retries on Failure|_filter arguments_
|`end`|EmptyOperator|Just an indicator for end of pipeline|_start_pipelines_


<br/>

<p align="right">(<a href="#top">back to top</a>)</p>

---

<!-- USAGE EXAMPLES -->
## Usage

Base on the requirements, the following are the execution steps for the respective pipelines:

* `Data Fusion` Run - Input the runtime arguments and run the pipeline
* `Cloud Composer` Run - Navigate to graph and run the pipeline

<p align="right">(<a href="#top">back to top</a>)</p>

---

### Data Fusion Run

<br/>

![Data Fusion Run][data-fusion-run]

The following are the variables that have to setup before the run:

variable|description
|-------|-------|
|`business_unit`|Dataset where the source data is
|`data_source`|Table where the source data is

<br/>

The following are the steps to trigger the `Data Fusion` run:
* Input `business_unit` and `data_source`
* Click _Run_
* To check the run status, there is a _Status_ at the top of the pipline 
* Similarly, one can view the run logs by clicking on the _Logs_ icon

<br/>

<p align="right">(<a href="#top">back to top</a>)</p>

---

### Cloud Composer Run

<br/>

![Cloud Composer Run][cloud-composer-run]

The following are the variables that have to setup before the run:

variable|description|sample
|-------|-------|-------|
|`bq_config_max_results`|BigQuery max result to return for the configuration|10
|`bq_config_table`|BigQuery Table to extract runtime arguments for the pipeline|configuration
|`bq_dataset_id`|BigQuery Dataset to save or extract information from|cdf_output
|`bq_location`|Location of the BigQuery Instance|US
|`bq_output_table`|BigQuery Table to save extracted data|
|`cdf_instance`|Name of the Cloud Data Fusion Instance|
|`cdf_location`|Location of Cloud Data Fusion instance|us-west1
|`cdf_pipeline_name`|Name of the Cloud Data Fusion Pipeline to trigger|
|`cdf_pipeline_run_arguments`|Runtime Arguments for the Cloud Data Fusion Pipeline. <br/>[`Note`] Create this variable with empty value and the pipeline will update this variable accordingly|[{'business_unit': 'hr', 'data_source': 'engineer'}, {'business_unit': 'hr', 'data_source': 'payroll'}]
|`cdf_pipeline_timeout_seconds`|Timeout in seconds for the Cloud Data Fusion pipeline to run|3600
|`dag_retry_interval_minutes`|Time between Dag retries in minutes|3
|`dag_timeout_minutes`|Time for Dag to run before timeout in minutes|10
|`project_id`|Project where resources are located|

<br/>

The following are the steps to trigger the `Cloud Composer` run:
* Setup Variables
* Click _Run_ Button
* To check the status, match the color of each box to the status at the top of the pipeline
* Click each box to view the respective logs for more information of the run

<br/>

<p align="right">(<a href="#top">back to top</a>)</p>

---

## Challenges

The following are some challenges encountered:
* Dag Run Status Inconsistent with `Data Fusion` Pipeline

<p align="right">(<a href="#top">back to top</a>)</p>

---

### Challenge #1:  Dag Run Status Inconsistent with Data Fusion Pipeline

<br/>
<b>Observation</b><br/>

`Cloud Composer` _start_pipelines_ show as `success`, even though the `Data Fusion` pipeline is still running 

The following are some sample logs
```
[2023-04-13, 10:12:26 UTC] {datafusion.py:902} INFO - Waiting when pipeline b08b351d-d9e3-11ed-8b80-42b59a849022 will be in one of the success states
[2023-04-13, 10:15:58 UTC] {datafusion.py:911} INFO - Job b08b351d-d9e3-11ed-8b80-42b59a849022 discover success state.
[2023-04-13, 10:15:58 UTC] {taskinstance.py:1416} INFO - Marking task as SUCCESS. dag_id=cdf-trigger-dag-yj, task_id=start_pipeine_0, execution_date=20230413T101201, start_date=20230413T101223, end_date=20230413T101558
[2023-04-13, 10:15:59 UTC] {local_task_job.py:220} WARNING - State of this instance has been externally set to success. Terminating instance.
[2023-04-13, 10:15:59 UTC] {process_utils.py:125} INFO - Sending Signals.SIGTERM to group 8949. PIDs of all processes in the group: [8949]
[2023-04-13, 10:15:59 UTC] {process_utils.py:80} INFO - Sending the signal Signals.SIGTERM to group 8949
[2023-04-13, 10:15:59 UTC] {process_utils.py:75} INFO - Process psutil.Process(pid=8949, status='terminated', exitcode=0, started='10:12:23') (8949) terminated with exit code 0
```

<b>Finding</b><br/>

After some digging in the source code, it seems that if the `success_states` for the _CloudDataFusionStartPipelineOperator_ is not specified, the operator will treat both `completed` and `running` as successful condition, and hence show `success` status even when `Data Fusion` pipeline has not completed running <br/>

```python
if success_states:
	self.success_states = success_states
else:
	self.success_states = SUCCESS_STATES + [PipelineStates.RUNNING]
```

<b>Solution</b>

The solution was to specified the `success_states` along with the timeout value

```python
pipeline_run = CloudDataFusionStartPipelineOperator(
            location=DATA_FUSION_LOCATION,
            pipeline_name=DATA_FUSION_PIPELINE_NAME,
            instance_name=DATA_FUSION_INSTANCE_NAME,
            asynchronous = False,
            task_id=f"start_pipeline_{business_unit}_{data_source}",
            runtime_args=configuration,
            success_states=[PipelineStates.COMPLETED],
            pipeline_timeout=DATA_FUSION_PIPELINE_TIMEOUT_SECONDS #default is 300seconds
        )
```

---

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [CloudDataFusionStartPipelineOperator Source Code][data-fusion-start-pipeline-source]
* [Readme Template][template-resource]

<p align="right">(<a href="#top">back to top</a>)</p>

---

<!-- MARKDOWN LINKS & IMAGES -->
[template-resource]: https://github.com/othneildrew/Best-README-Template/blob/master/README.md
[data-fusion-start-pipeline-source]: https://github.com/apache/airflow/blob/main/airflow/providers/google/cloud/operators/datafusion.py
[data-fusion-api]: ./images/data_fusion_api.png
[data-fusion-service-account]: ./images/data_fusion_service_account.png
[data-fusion-instance]: ./images/data_fusion_instance.png 
[data-fusion-pipeline]: ./images/data_fusion_pipeline.png
[data-fusion-run]: ./images/data_fusion_run.png
[cloud-composer-api]: ./images/cloud_composer_api.png
[cloud-composer-service-account]: ./images/cloud_composer_service_account.png
[cloud-composer-instance]: ./images/cloud_composer_instance.png 
[cloud-composer-pipeline]: ./images/cloud_composer_pipeline.png
[cloud-composer-run]: ./images/cloud_composer_run.png