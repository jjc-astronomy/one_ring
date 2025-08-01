# One Ring: Distributed Binary Pulsar Search Pipeline

**One Ring** is a high-performance, fault-tolerant Nextflow pipeline for large-scale binary pulsar searches. It can run across multiple heterogeneous HPC clusters or cloud environments (e.g. AWS) and streams real-time search metadata and candidate detections into a central relational database using [Apache Kafka](https://kafka.apache.org/).

All software components run inside containerized environments managed by [Apptainer (Singularity)](https://apptainer.org/), ensuring reproducibility and consistent deployment across diverse compute systems. Every command, its arguments, and the Apptainer image hash are automatically recorded in the database, enabling full reproducibility of each candidate and search result.


> Pulsar searching is an **embarrassingly parallel** problem. Each observation can be processed independently, making it ideal for distributed compute environments. One Ring is designed to take advantage of this by scaling across any number of clusters, while keeping the search state synchronized through a central database and message broker.

---

## Table of Contents

* [Key Features](#key-features)
* [Architecture Overview](#architecture-overview)
* [Requirements](#requirements)
* [Usage](#usage)
* [Repository Layout](#repository-layout)
* [Notes](#notes)
* [To Do](#to-do)
* [Contact](#contact)
* [Setup Instructions](SETUP.md)

---

## Key Features

* Distributed, cluster-independent pulsar search using [Nextflow](https://www.nextflow.io/).
* Currently supports Acceleration ([Johnston & Kulkarni 1991](https://ui.adsabs.harvard.edu/abs/1991ApJ...368..504J/abstract)) and three and five Keplerian parameter template bank searches ([Knispel 2011](https://ui.adsabs.harvard.edu/abs/2011PhDT.......293K/abstract), [Balakrishnan et al. 2022](https://ui.adsabs.harvard.edu/abs/2022MNRAS.511.1265B/abstract)) for circular and elliptical orbits.
* Real-time monitoring and candidate updates using [Apache Kafka](https://kafka.apache.org/).
* Structured SQL-based  of all metadata: pointings, beams, candidates, folding, and logs.
* Fault-tolerant architecture: processing continues if Kafka or DB is temporarily down.
* Fully containerized execution: no local Python or search tools required.
* Modular configuration per cluster using `configs/`.
* Safe resume support using `--resume`.

---

## Architecture Overview

Each HPC cluster runs a local instance of the Nextflow pipeline. The pipeline consumes a list of filterbank files and processes them using containerized pulsar search tools. JSON logs describing task status, candidate detections, and folding metadata are written to disk via the [`nf-live-tracking`](https://github.com/vishnubk/nf-live-tracking) plugin in real-time.

A containerized **watchdog script** monitors these logs and sends updates as Kafka events to a central broker. This component is fully isolated inside an Apptainer container—no local Python installation is required.

On the control node, **Kafka Connect** runs a JDBC sink connector that consumes these events and writes them into a structured **MariaDB** or **MySQL** database. All data transfers can be tunneled via SSH.

The pipeline is resilient: if Kafka or the database is temporarily unavailable, local JSON logs persist until connectivity is restored and the backlog is flushed.

---

## Requirements

To run the One Ring pipeline, the following components are required:

**1. MariaDB or MySQL database**

* Schema files are included under `include/db_schemas/`
* Can be hosted locally or inside Docker

**2. Apache Kafka**

* A working Kafka + Kafka Connect setup
* Docker Compose configuration is available at [compact\_kafka](https://github.com/erc-compact/compact_kafka)
* A self-contained version will also be included in `include/` in this repository

**3. Docker**

* Required to launch Kafka services if using Docker

**4. Nextflow**

* Must be installed on each compute cluster
* Requires Java 8 or later

**5. Apptainer (Singularity)**

* Used to run all processing and monitoring steps

**6. nf-live-tracking plugin**

* Installed once per compute cluster
* Captures Nextflow events as structured JSON logs

**7. SSH access (if required)**

* Used for secure forwarding of Kafka and DB traffic from clusters to the control node

No additional system packages or Python dependencies are required. All steps run inside containerized environments.

---

## Usage

Each cluster runs the pipeline independently using a configuration profile:

```bash
nextflow run search_pipeline.nf -profile <cluster-name>
```

Example:

```bash
nextflow run search_pipeline.nf -profile contra
```

Each profile should be defined in the `configs/` directory. 

---

## Repository Layout

```plaintext
.
├── search_pipeline.nf          # Main Nextflow pipeline
├── modules.nf                  # Definition for each process inside search_pipeline.nf
├── nextflow.config             # Global Nextflow configuration
├── configs/                    # Cluster-specific configs (e.g. contra.config)
├── scripts/                    # Python/Bash utilities (run via Apptainer)
├── include/
│   ├── fold_templates/         # Fold templates for PulsarX
│   └── avro_schema             # Avro schema files for kafka-connect
└── README.md
```

---



## Notes

* The pipeline supports full restart with `--resume` using Nextflow’s built-in checkpointing.
* Kafka messages are sent in Avro format, and schemas are provided under `include/avro_schema`. See the [SETUP.md](SETUP.md) for more details.
* Each Kafka topic maps directly to a specific table in the database, ensuring logical separation of message streams.
* The database uses UUIDs as primary keys across many tables. This allows completely decentralized processing without needing to coordinate with the central database during runtime.
* The watchdog script is **idempotent** and supports checkpointing. Re-running the same watchdog on previously processed JSON logs does not generate duplicate entries or new UUIDs.

---

---

## To Do

* Add support for jerk searches, FFA, and PRESTO
* Include Kafka and DB Docker Compose inside this repository under `include/`
* Add optional monitoring dashboards for Kafka
* Provide cloud-ready deployment examples (e.g. AWS Batch or Spot Instances)

---

## Contact

For bug reports, feature requests, or questions, please open an [issue](https://github.com/vishnubk/one_ring/issues) or reach out directly.

