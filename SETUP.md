# One Ring Pipeline: Setup Guide

This document provides a complete, step-by-step guide to setting up the One Ring distributed binary pulsar search pipeline. It covers installing dependencies, configuring the environment, launching Kafka and database connectors, and running the Nextflow pipeline across multiple HPC clusters.

---

## Step 1: Install Nextflow

Nextflow is required on each HPC cluster.

```bash
curl -s https://get.nextflow.io | bash
```


Official installation instructions: [https://www.nextflow.io/docs/latest/getstarted.html](https://www.nextflow.io/docs/latest/getstarted.html)

Add `nextflow` to your `PATH` after installation.

---

## Step 2: Install nf-live-tracking Plugin

Clone the plugin repository:

```bash
git clone https://github.com/vishnubk/nf-live-tracking
cd nf-live-tracking
make compile && make install
```

This plugin enables structured JSON logging of pipeline events (job submitted, started, failed, or successful).

---

## Step 3: Clone One Ring Repository

```bash
git clone https://github.com/erc-compact/one_ring.git
cd one_ring
```

Enable the plugin in `nextflow.config`:

```groovy
plugins {
    id 'nf-live-tracking@2.0.0'
}

live {
    enabled = true
    file = "/scratch/path/JSON_EVENTS/PROGRESS"
    overwrite = true
    interval = 5
}
```

**Important**: Create the directory specified in `file`. For example:

```bash
mkdir -p /scratch/path/JSON_EVENTS
```

The `PROGRESS` keyword is a prefix for output JSON logs. Keep this unchanged unless you know what you're doing.

---

## Step 4: Configure Search Parameters

The main configuration file is `nextflow.config`. This includes search tool arguments for:

* filtool
* peasoup (acceleration and template bank)
* pulsarx
* pics
* post-folding heuristics

### Example Peasoup Configuration

```json
peasoup = '[
  {
    "cfg_name": "example/acc0",
    "fft_size": "268435456",
    "start_sample": "0",
    "acc_start": "-150.0",
    "acc_end": "150.0",
    "min_freq": "0.1",
    "max_freq": "1100.0",
    "min_snr": "6.0",
    "ram_limit_gb": "45.0",
    "nh": "5",
    "ngpus": "1",
    "total_cands": "1250"
  },
  {
    "cfg_name": "example/tb1",
    "fft_size": "16777216",
    "start_sample": "42532864",
    "keplerian_template_bank": "/path/to/template_bank.txt",
    "min_snr": "8.0",
    "ram_limit_gb": "45.0",
    "ngpus": "1",
    "total_cands": "10000"
  }
]'
```

Ensure that each configuration gets a unique `cfg_name`.

---

## Step 5: Folding Configuration

Folding criteria are configured as a JSON list. Each element represents a parameter filter for candidates:

```groovy
configuration = [
  search1: [
    [
      spin_period: [min: 0.0001, max: 10.0],
      dm: [min: 0.0, max: 100.0],
      fft_snr: [min: 9.0, max: 10000.0],
      total_cands_limit: 2500
    ],
    [...],
    [...]
  ]
]
```

Folding uses the **union** of all parameter sets and removes duplicates before execution.

To avoid folding RFI or bright known pulsars, define a CSV file:

```csv
period_ms,period_tolerance_ms,dm,dm_tolerance,comment
4.99058,0.01,52.1346,0.5,PulsarA
```

---

## Step 6: Launch Kafka and Schema Registry

Clone the Kafka deployment repository:

```bash
git clone https://github.com/erc-compact/compact_kafka.git
cd compact_kafka
docker compose up -d
```

Wait 30 to 60 seconds for all services to be ready.

---

## Step 7: Configure Kafka Connectors

Run the following `curl` commands to register Kafka Connect JDBC sinks. Replace the placeholders with your database credentials:

**DB\_NAME**, **DB\_USER**, **DB\_PASSWORD**

### Connector 1: `processing`

```bash
curl -X PUT http://localhost:8083/connectors/sink-jdbc-mariadb-processing/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:mariadb://csql:3306/DB_NAME",
    "connection.user": "DB_USER",
    "connection.password": "DB_PASSWORD",
    "topics": "processing",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "session_id,task_id,run_name",
    "delete.enabled": "false",
    "transforms": "valueToKey",
    "transforms.valueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.valueToKey.fields": "session_id,task_id,run_name",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "errors.tolerance": "all",
    "errors.retry.timeout": "-1",
    "errors.retry.delay.max.ms": "300000",
    "offset.flush.interval.ms": "10000",
    "consumer.max.poll.interval.ms": "600000",
    "batch.size": "500",
    "max.retries": "1000",
    "retry.backoff.ms": "10000"
  }'
```

### Connector 2: `processing_dp_inputs`

```bash
curl -X PUT http://localhost:8083/connectors/sink-jdbc-mariadb-processing-dp-inputs/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:mariadb://csql:3306/DB_NAME",
    "connection.user": "DB_USER",
    "connection.password": "DB_PASSWORD",
    "topics": "processing_dp_inputs",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "dp_id,session_id,task_id,run_name",
    "delete.enabled": "false",
    "transforms": "valueToKey",
    "transforms.valueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.valueToKey.fields": "dp_id,session_id,task_id,run_name",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "errors.tolerance": "all",
    "errors.retry.timeout": "-1",
    "errors.retry.delay.max.ms": "300000",
    "offset.flush.interval.ms": "10000",
    "consumer.max.poll.interval.ms": "600000",
    "batch.size": "500",
    "max.retries": "1000",
    "retry.backoff.ms": "10000"
  }'
```

### Connector 3: All Other Tables

```bash
curl -X PUT http://localhost:8083/connectors/sink-jdbc-mariadb-01/config \
  -H "Content-Type: application/json" \
  -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:mariadb://csql:3306/DB_NAME",
    "connection.user": "DB_USER",
    "connection.password": "DB_PASSWORD",
    "topics": "project, telescope, antenna, beam, beam_antenna, beam_type, candidate_filter, circular_orbit_search, data_product, elliptical_orbit_search, file_type, filtool, fold_candidate, hardware, peasoup, pipeline, prepfold, pulsarx, rfifind, search_candidate, user, user_labels, target, candidate_tracker",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id",
    "delete.enabled": "false",
    "key.converter": "org.apache.kafka.connect.converters.IntegerConverter",
    "transforms": "valueToKey,extractId",
    "transforms.valueToKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
    "transforms.valueToKey.fields": "id",
    "transforms.extractId.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractId.field": "id",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "errors.tolerance": "all",
    "errors.retry.timeout": "-1",
    "errors.retry.delay.max.ms": "300000",
    "offset.flush.interval.ms": "10000",
    "consumer.max.poll.interval.ms": "600000",
    "batch.size": "500",
    "max.retries": "1000",
    "retry.backoff.ms": "10000"
  }'
```

---


## Step 8: Pull Singularity Images

```bash
apptainer pull docker://vishnubk/peasoup
apptainer pull docker://vishnubk/pulsarx
apptainer pull docker://vishnubk/pics:20230630_pics_model_update
apptainer pull docker://vishnubk/candy_picker
apptainer pull docker://vishnubk/watchdog
```

Update `scripts/get_docker_image_hash.sh` to reflect the images in use.

---

## Step 9: Cluster Configuration

Edit your cluster profile file (e.g. `config/ozstar.config`) to set:

* `publish_dir_prefix`
* paths to all Apptainer image files

---

## Step 10: Upload Observation Metadata (These scripts are set-up specifically for COMPACT's directory structure)

Clone utility repository:

```bash
git clone https://github.com/erc-compact/compact_utils
```

Use the appropriate Apptainer image for metadata prep:

```bash
apptainer shell -B /path1 -B /path2 /path/to/skyweaverpy_latest.sif
```

Run the CSV generation script:

```bash
python build_input_csv.py -r /data/path -o upload_input.csv -s TargetName -u 2024-01-01-12:00:00
```

Here is an example [output](https://github.com/erc-compact/one_ring/blob/main/include/db_upload_files/tb_sample_input.csv).

---

## Step 11: Upload Metadata to Database

Create a `.env` file with your DB credentials:

```env
DB_NAME=...
DB_HOST=...
DB_PORT=...
DB_USERNAME=...
DB_PASSWORD=...
```

Ensure this `.env` filename is reflected inside [scripts/upload_data.py](https://github.com/erc-compact/one_ring/blob/main/scripts/upload_data.py)

Run the upload script:

```bash
bash upload_data_to_database.bash -c upload_input.csv \
  -m /path/to/metadatafile.hdf5 \
  -p POINTING_ID \
  --hardware-name your_cluster_name
```

This will produce a `*.json` file with pipeline run information.

---

## Step 12: (Optional) Run Kafka Watchdog

Update `scripts/watchdog.yaml` to configure:

* `directory`: JSON output directory from Nextflow
* `remote_workdir`: remote path if results are from another HPC
* Lookup table paths, Kafka topics, schema files

Run inside Apptainer shell:

Recommendation: Run the example command below inside a `screen/tmux` session.

```bash
apptainer run -B /mnt /path/to/watchdog_latest.sif
python scripts/kafka_watchdog.py scripts/watchdog.yaml
```

---

## Step 13: Launch the Pipeline

```bash
nextflow run search_pipeline.nf \
  -profile your_hpc \
  -params-file pipeline_run.json \
  -w /your/workdir/run1 \
  -name run1
```

To resume a failed run:

```bash
nextflow run search_pipeline.nf \
  -profile your_hpc \
  -params-file pipeline_run.json \
  -w /your/workdir/run2 \
  -name run2 -resume
```

---

## Step 14: HPC Interconnect and Port Forwarding

To connect a remote HPC (e.g. Hercules) to the control machine:

```bash
ssh -R 33306:csql:3306 \
    -R 39092:localhost:39092 \
    -R 38083:localhost:8083 \
    -R 38081:localhost:8081 \
    -R 38088:localhost:8088 your-hpc1
```

Each port corresponds to:

* 33306: MariaDB
* 39092: Kafka listener port
* 38083: Kafka Connect
* 38081: Schema Registry


Update `.env` with `DB_PORT=33306` and Kafka listener port accordingly. Every HPC cluster should get a unique Kafka listener port. See examples inside [docker-compose.yml](https://github.com/erc-compact/compact_kafka/blob/main/docker-compose.yml).

---

## Step 15: Remote Watchdog (Offline) Mode (Optional)

If the SSH connection between the control node and the remote HPC is too slow for real-time access, first complete the processing on the remote HPC, then copy the results back to the control HPC afterwards.

```bash
# Copy nextflow workdir and json events from remote hpc to control hpc
rsync -Pav --exclude='*.fil' --exclude='*.ar' --exclude='*.png' --exclude='*.cands' remote:$DIR/NEXTFLOW_WORKDIR/* /local/REMOTE_HPC_CLUSTER_PROGRESS/HPC_NAME/NEXTFLOW_WORKDIR
rsync -av remote:/JSON_EVENTS_DIR/ /local/REMOTE_HPC_CLUSTER_PROGRESS/HPC_NAME/JSON_EVENTS
```

Update `scripts/watchdog.yaml` to point to the copied directories and run the watchdog on the control machine.
