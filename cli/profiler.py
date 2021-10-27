import json
from typing import Tuple
import click
import requests
from cli.config import CONFIG


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--table",
    required=True,
    help="Snowflake table to profile with format [db].[schema].[table]",
)
@click.option(
    "--column",
    "-c",
    multiple=True,
    default=[],
    help="[Optional] Snowflake table columns to profile. Can specify many. For example -c column_a -c column_b etc",
)
@click.option(
    "--where",
    help="""
    [Optional] Snowflake where predicate to filter rows to profile. For example --where "WEBSITE_LOCALE = 'US'"
    """,
)
@click.option(
    "--tag",
    help="[Optional] Tag to apply to profiling job. For helping to filter in Looker.",
)
@click.option("--database", required=True, help="Snowflake database")
@click.option("--schema", required=True, help="Snowflake schema")
@click.option("--user", required=True, help="Snowflake user")
@click.option("--password", required=True, help="Snowflake password")
@click.option("--role", required=True, help="Snowflake role")
@click.option("--warehouse", required=True, help="Snowflake warehouse")
@click.option("--snowflake-url", required=True, help="Snowflake account url")
@click.option("--host", required=True, help="Databricks host name")
@click.option("--token", required=True, help="Databricks API token")
def profile(
    table: str,
    column: Tuple[str],
    where: str,
    tag: str,
    database: str,
    schema: str,
    user: str,
    password: str,
    role: str,
    warehouse: str,
    snowflake_url: str,
    host: str,
    token: str,
):
    payload = json.loads(CONFIG)
    profile_name = f"DnaProfiler-{table}-{tag}"
    payload["run_name"] = profile_name
    jar_params = ["--table", table]
    if column and len(column) > 0:
        jar_params.extend(["--columns", ",".join(list(column))])
    if where:
        jar_params.extend(["--where", where.upper().replace("WHERE", "")])
    if tag:
        jar_params.extend(["--tag", tag])
    jar_params.extend(
        [
            "--database",
            database,
            "--schema",
            schema,
            "--user",
            user,
            "--password",
            password,
            "--role",
            role,
            "--warehouse",
            warehouse,
            "--snowflake-url",
            snowflake_url,
        ]
    )
    payload["spark_jar_task"]["parameters"] = jar_params
    endpoint = host + "/api/2.0/jobs/runs/submit"
    response = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
    )
    if response.ok:
        click.echo(
            f"Submitted profiling job. Check ouput {database}.{schema}.data_profiler in 5 minutes"
        )
    else:
        click.echo(f"ERROR in submitting profiler job: {response.json()}")


if __name__ == "__main__":
    cli()
