# tap-chromedata

`tap-chromedata` is a Singer tap for chromedata.

Built with the Meltano [SDK](https://gitlab.com/meltano/sdk) for Singer Taps.

It uses the Chromedata API to fetch data from an FTP server containing Various Make, Model, Year, Style data and the mapping to [ACES dataset](https://www.autocare.org/data-and-information/data-standards/aftermarket-catalog-exchange-standard-(aces)) 
## Installation

```bash
pip install tap-chromedata
```

## Configuration

### Accepted Config Options

```js
{
  "TAP_CHROMEDATA_FTP_URL": "ftp.chromedata.com",
  "TAP_CHROMEDATA_FTP_USER": "some_username",
  "TAP_CHROMEDATA_FTP_PASS": "some_password"
}
```

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-chromedata --about
```

### Source Authentication and Authorization

Use of the API and accessing the FTP Server requires a Username and Password

## Usage

You can easily run `tap-chromedata` by itself or in a pipeline using [Meltano](www.meltano.com).

### Executing the Tap Directly

```bash
tap-chromedata --version
tap-chromedata --help
tap-chromedata --config CONFIG --discover > ./catalog.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_chromedata/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-chromedata` CLI interface directly using `poetry run`:

```bash
poetry run tap-chromedata --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-chromedata
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-chromedata --version
# OR run a test `elt` pipeline:
meltano elt tap-chromedata target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
