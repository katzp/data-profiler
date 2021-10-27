# DNA Data Profiler
This is a lightweight data profiler tool that wraps the deequ package & saves column profile results in Snowflake.

This is meant as a way to create some profiling metrics after an ETL run and contains a CLI for submitting a new profiling job to Databricks

## Submitting a Profiling Job
1. Install the CLI tool
```
git clone https://github.com/katzp/data-profiler.git
cd data-profiler
pip install .
```
2. Submit a profiling job
See below for usage text.
```
Usage: data-profiler profile [OPTIONS]

Options:
  --table TEXT          Snowflake table to profile with format
                        [db].[schema].[table]  [required]
  -c, --column TEXT     [Optional] Snowflake table columns to profile. Can    
                        specify many. For example -c column_a -c column_b etc 
  --where TEXT          [Optional] Snowflake where predicate to filter rows to
                        profile. For example --where "COUNTRY = 'US'"
  --tag TEXT            [Optional] Tag to apply to profiling job. For helping 
                        to filter in Looker.
  --database TEXT       Snowflake database to save results  [required]        
  --schema TEXT         Snowflake schema to save results  [required]
  --user TEXT           Snowflake user  [required]
  --password TEXT       Snowflake password  [required]
  --role TEXT           Snowflake role  [required]
  --warehouse TEXT      Snowflake warehouse  [required]
  --snowflake-url TEXT  Snowflake account url  [required]
  --host TEXT           Databricks host name  [required]
  --token TEXT          Databricks API token  [required]
  --help                Show this message and exit.
```