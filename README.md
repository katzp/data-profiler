# DNA Data Profiler
This is a lightweight profiler tool that saves column profile results in Snowflake but is mainly meant to be consumed by the following Looker report.

https://cimpress.eu.looker.com/dashboards-next/7961

This is meant as a way to create some profiling metrics after an ETL run and thus contains a CLI for submitting a new profiling job.

## Submitting a Profiling Job
1. Install the CLI tool
```
pip install dna-profiler --extra-index-url https://$DNA_ARTIFACTORY_CLIENT:$DNA_ARTIFACTORY_SECRET@vistaprint.jfrog.io/vistaprint/api/pypi/pypi-virtual/simple 
```
2. Set the following environment variable with Databricks API token. This should be a Databricks token from the DnA Common PRD environment.
```
export DNA_PROFILER_DATABRICKS_TOKEN=XXXXXXXXXXX
```
3. Submit a profiling job
```
dna-profiler profile --table vistaprint.transactions.order_info --tag my_first_profile
```
There are a few other CLI options. See below for usage text.
```
Usage: dna-profiler profile [OPTIONS]

Options:
  --table TEXT       [Required] Snowflake table to profile with format [db].[schema].[table]
  -c, --column TEXT  [Optional] Snowflake table columns to profile if you don't need all columns. Can specify  
                     many. For example -c column_a -c column_b
  --where TEXT       [Optional] Snowflake where predicate to filter rows to profile.
                     For example --where "WEBSITE_LOCALE = 'US'"
  --tag TEXT         [Optional] Tag to apply to profiling job. For helping to filter in Looker and grouping profiling jobs.
  --help             Show this message and exit.
```
4. Check back to the Looker report in about 5 minutes to see you results

If you have questions reach out to pkatz@vistaprint.com