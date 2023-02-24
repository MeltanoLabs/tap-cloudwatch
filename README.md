# `tap-cloudwatch`

CloudWatch tap for extracting log data from AWS Cloudwatch Logs Insights API.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Settings

| Setting              | Required | Default | Description |
|:---------------------|:--------:|:-------:|:------------|
| aws_access_key_id    | False    | None    | The access key for your AWS account. |
| aws_secret_access_key| False    | None    | The secret key for your AWS account. |
| aws_session_token    | False    | None    | The session key for your AWS account. This is only needed when you are using temporary credentials. |
| aws_profile          | False    | None    | The AWS credentials profile name to use. The profile must be configured and accessible. |
| aws_endpoint_url     | False    | None    | The complete URL to use for the constructed client. |
| aws_region_name      | False    | None    | The AWS region name (e.g. us-east-1)  |
| start_date           | True     | None    | The earliest record date to sync |
| log_group_name       | True     | None    | The log group on which to perform the query. |
| query                | True     | None    | The query string to use. For more information, see [CloudWatch Logs Insights Query Syntax](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html). |
| batch_increment_s    | False    |    3600 | The size of the time window to query by, default 3,600 seconds (i.e. 1 hour). If the result set for a batch is greater than the max limit of 10,000 records then the tap will query the same window again where >= the most recent record received. This means that the same data is potentially being scanned >1 times but < 2 times, depending on the amount the results set went over the 10k max. For example a batch window with 15k records would scan the 15k once, receiving 10k results, then scan ~5k again to get the rest. The net result is the same data was scanned ~1.5 times for that batch. To avoid this you should set the batch window to avoid exceeding the 10k limit. |
| stream_maps          | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config    | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled   | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-cloudwatch --about`

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-cloudwatch` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-cloudwatch --version
tap-cloudwatch --help
tap-cloudwatch --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_cloudwatch/tests` subfolder and
  then run:

```bash
poetry run tox -e pytest
```

Coverage reports are generated at `tap_cloudwatch/tests/codecoverage/`.

You can also test the `tap-cloudwatch` CLI interface directly using `poetry run`:

```bash
poetry run tap-cloudwatch --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-cloudwatch
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-cloudwatch --version
# OR run a test `elt` pipeline:
meltano elt tap-cloudwatch target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

### Further Features

Using create_export_task to efficiently bulk export to S3 then read that data.
