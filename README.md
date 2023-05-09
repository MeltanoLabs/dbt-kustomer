<p align="center">
    <a alt="License"
        href="https://github.com/brooklyn-data/dbt-kustomer/blob/main/LICENSE.md">
        <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" /></a>
    <a alt="dbt-core">
        <img src="https://img.shields.io/badge/dbt_Core™_version->=1.3.0_,<2.0.0-orange.svg" /></a>
    <a alt="Maintained?">
        <img src="https://img.shields.io/badge/Maintained%3F-yes-green.svg" /></a>
    <a alt="PRs">
        <img src="https://img.shields.io/badge/Contributions-welcome-blueviolet" /></a>
</p>

# Kustomer Meltano tap data transformation for dbt

This package is designed to work alongside the [Kustomer Meltano tap](https://github.com/brooklyn-data/tap-kustomer) to transform the data into an easy-to-use format.

Supported warehouses:

- Snowflake
- Bigquery
- PostgreSQL
- Redshift

## Installation and use

1. Include the following package version in your `packages.yml` file. Check [dbt Hub](https://hub.getdbt.com/) for the latest version to use.

```yml
packages:
    - package: meltano/kustomer # TBC
      version: 0.1.0 # TBC
```

2. Install the package by running `dbt deps`

3. In your `dbt_project.yml` file, include the location where the tap data is expected to load from:

```yml
vars:
  kustomer_schema: your_database_schema # By default this is `tap_kustomer`
```

4. Choose which schemas you want to build the tables in by including this in your `dbt_project.yml` file:

```yml
models:
  dbt_kustomer:
    marts:
      +schema: your_marts_schema # By default this is `kustomer`
    staging:
      +schema: your_staging_schema # By default this is `stg_kustomer`
```

5. Build the tables by running

```sh
dbt run --select package:dbt_kustomer
```

## Tables

These are the tables that will be built with this project.

TODO - Add ERD of tables once all created

## Contributing

Want to get involved? Please do! Feel free to create your own fork and create PRs with updates. If you aren't sure how to get started, then please reach out to one of the maintainers, and we'd be happy to help.

Spot something amiss? Please create an issue on [GitHub](https://github.com/brooklyn-data/dbt-kustomer/issues).
