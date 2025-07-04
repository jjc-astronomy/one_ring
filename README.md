# One Ring: Distributed Binary Pulsar Search Pipeline

**One Ring** is a high-performance Nextflow pipeline for large-scale binary pulsar searches. It is designed to run distributed across multiple HPC clusters, stream real-time status and candidate updates to a central MySQL/MariaDB database via Apache Kafka, and integrate seamlessly with modern containerized workflows.

---

## Key Features

* Fully distributed, HPC cluster-agnostic pulsar search.
* Currently supports Acceleration searches, Template-Bank Searches for Circular or Elliptical orbits.
* Supports live tracking of pipeline status with [`nf-live-tracking`](https://github.com/vishnubk/nf-live-tracking).
* Modular design — configurable per cluster using separate `config/*` profiles.
* Uses containerized software environments via Apptainer (Singularity).

---

## Requirements

* **Nextflow** ≥ [latest stable](https://www.nextflow.io/docs/latest/install.html)
* [`nf-live-tracking` plugin](https://github.com/vishnubk/nf-live-tracking)
* Apptainer/Singularity
* Kafka + MySQL/MariaDB backend for candidate streaming

---

## Setup

### 1. Install Nextflow (one time per cluster)

```bash
curl -s https://get.nextflow.io | bash
```

Add `nextflow` to your `$PATH`.

---

### 2. Install `nf-live-tracking` (one time per cluster)
Clone the [`repo`](https://github.com/vishnubk/nf-live-tracking) and run
```bash
make compile && make install
```

---

### 3. Pull required Singularity images

Each processing step relies on containerized tools hosted on DockerHub.
To pull them:

```bash
apptainer pull docker://vishnubk/<software_name>:<tag>
```

**Examples:**

```bash
apptainer pull docker://vishnubk/pulsarx:latest
apptainer pull docker://vishnubk/peasoup:keplerian
```

Ensure your `apptainer` (or `singularity`) is correctly configured with the right mount paths for your HPC environment.

---

## Usage

Run the pipeline with an appropriate profile for your cluster. Example:

```bash
nextflow run search_pipeline.nf -profile contra
```

Cluster-specific configs are stored in `configs/`.
Scratch directories, intermediate config files, and logs are managed automatically under `scratch/`.

---

## Repository Layout 

```plaintext
.
├── search_pipeline.nf      # Main pipeline entrypoint
├── nextflow.config         # Base config
├── configs/                # Cluster-specific configs: contra.config, hercules.config, etc.
├── scripts/                # Python/Bash Scripts used by Nextflow for data analysis
├── scratch/                # Runtime transient files 
└── README.md
```

---

## Notes

* Use `--resume` to safely resume incomplete runs.
* Database and Kafka connections are handled via environment variables or `nextflow.config` secrets — adapt accordingly for each HPC.
* All runtime-generated files (Nextflow configs, Docker image digests, logs) are written to `scratch/` by default.
* This pipeline assumes you have valid baseband or filterbank data prepared and accessible to each compute node.

---

## To Do

* Write more Documentation.
* Add support for Jerk Searches, FFA, and PRESTO.

---

## Contact

If you find a bug or have a feature request, please open an issue or reach out directly.

---
