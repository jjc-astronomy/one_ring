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

### Hierarchical template bank search

A circular-orbit template bank search can be run in two stages. Stage 1 is the normal peasoup
template bank search configured above (typically a coarse bank split into chunks, one peasoup
entry per chunk). With `hierarchical_search.enable = 1` the pipeline then, for every filterbank
and search segment:

1. groups the stage-1 `output.xml` files (all template bank chunks and, if `merge_dm_ranges = 1`,
   all incoherent DM ranges of the filterbank),
2. maps every candidate to Legendre-coefficient space and keeps only the S/N peaks
   (`scripts/hierarchical_search.py cluster`); a peak that rises less than `min_prominence`
   (in S/N) above the saddle towards a brighter touching cluster is absorbed into that cluster,
   so a bright pulsar does not spawn one fine grid per noise bump on its slope,
3. sizes the fine template grid around every peak (the lattice itself is regenerated per peak in
   stage 2). With `lattice = "linear"` this is a lattice in the eigen-basis of the orbital metric evaluated at the
   peak; with `lattice = "adaptive"` an advancing front grows the grid layer by layer, re-evaluating
   the metric at every step and measuring all distances in Legendre-coefficient space, followed by a
   gap-filling pass that guarantees the covering mismatch. The adaptive lattice is only worth its
   extra cost for large `search_halfrange_fac`, where the metric changes across the region,
4. searches the fine grid (stage 2). The dedispersed time series dumped by the first stage-1 chunk of
   every DM range are searched with
   `scripts/resample_timeseries.cu`, a tool built on peasoup's own GPU resampling kernels (built with
   `scripts/build_resample_timeseries.sh` inside the peasoup container, or on the fly in the job).
   With `fold.method = "gpu_fold"` (default) every fine-lattice template is resampled into its own
   emission frame and folded on the device in one pass, at its Doppler-corrected frequency
   (f_obs minus the linear term of the template relative to the peak), into `fold.nsub`
   sub-integrations; the small grid of frequency offsets (`fold.f0_halfrange_bins`,
   `fold.f0_step_bins`) is then searched on the device too, by whole-bin shifts of the
   sub-integrations, and scored with pyloki's boxcar matched filter, so that only the best
   (S/N, width, offset) per template is returned to the host. Nearest-neighbour resampling
   (`fold.resampler_mode`) keeps the noise of neighbouring bins independent; interpolating
   resamplers bias the boxcar S/N when the profile has more bins than samples per period.
   With `fold.method = "rotate"` only the peak template is resampled and the lattice is
   searched by rotating the sub-integration profiles, which is approximate for far templates and
   needs the automatic `nsub`. The peaks are split into jobs of `fold.peaks_per_job`, run on GPU
   nodes in the peasoup container. The DM range of a group is the union of the stage-1 ranges,
5. merges the stage-2 chunk results into a single XML file, clustered again if
   `cluster_stage2 = 1`, which is then folded with PulsarX. The S/N thresholds and candidate limit
   for this fold come from `hierarchical_search.folding_configuration` (same syntax as
   `folding.configuration`), because the stage-2 `snr` is the fold S/N of the aligned profile, not
   the FFT S/N of stage 1.

```groovy
hierarchical_search {
    enable = 1
    stage1_cfg_regex = "30m/ck02/tb_ck.*"   // which stage-1 runs take part (null = all)
    merge_dm_ranges = 1
    nominal_mismatch = null               // null: MISMATCH of the stage-1 template bank header
    tobs = null                           // null: TOBS of the bank header (else the segment length)
    period_min = null                     // null: SPIN PERIOD of the bank header
    clip_to_bank = 1                      // drop fine-grid templates outside the bank's PORB / mass region
    distance_tol = 50
    min_prominence = 2
    search_halfrange_fac = 0.6
    search_stepsize_fac = 0.2
    max_templates = 5000000               // cap the stage-2 search, brightest peaks first
    merge_stage2 = 1
    cluster_stage2 = 1
}
```

**Whitening of the stage-1 search.** Peasoup's default red-noise whitening is applied to the spectrum of the
unresampled time series, where a binary pulsar is smeared over many Fourier bins; the running median rides up
onto it and removes a large part of its power. Switching whitening off (`--no_deredden`) instead leaves
thousands of sub-hertz red-noise candidates per template and makes the stage-1 jobs candidate-bound. The
peasoup image used here (`peasoup_4f0472a.sif`, fork at
https://github.com/vishnubk/peasoup commit 78cb72b + branch `post-resample-deredden`) adds
`--deredden_post_resample`, which whitens the spectrum of every resampled time series instead, with wider
running-median windows at high frequency (`--boundary_125_freq`, `--boundary_625_freq`). Put

```
"extra_args": "--deredden_post_resample --boundary_125_freq 5 --boundary_625_freq 50"
```

in every stage-1 peasoup entry. Cost: about 10 % per template trial; a binary smeared over ~900 bins keeps its
full S/N (synthetic test: 91 vs 73 with the default whitening), and the red-noise candidates disappear.

The parameters that define the fine grid are not hard-coded: `nominal_mismatch`, `tobs` and
`period_min` default to the `# MISMATCH`, `# TOBS (h)` and `# SPIN PERIOD (ms)` values in the header of
the stage-1 template bank, found through `search_parameters/template_bank_file` of the stage-1 XML
(the same file is used by the stage-2 merge). Setting them in the config overrides the bank. With
`clip_to_bank = 1` the fine lattice is also cut to the physical region of the bank (`PORB MIN/MAX`
and the `asini/c` limit from the mass limits), so no time is spent on orbits the bank was not
designed for; a peak whose whole lattice falls outside keeps its stage-1 orbit.

When the hierarchical search is enabled nothing is published to `05_SEARCH`, `09_FOLD_FIL` or
`10_FOLD_FIL_FILTER`: both stages go to the hierarchical trees (`search_publish_dir`, `fold_publish_dir`
and `filter_publish_dir`, by default `11_`, `12_` and `13_HIERARCHICAL_SEARCH`), with the usual layout:

```
11_HIERARCHICAL_SEARCH/<target>/<utc>/<filstr>/<pipeline_name>/
    <dm_start>_<dm_end>/<cfg_name>/output.xml                 # stage-1 peasoup results (one per DM range and chunk)
    <dm_start>_<dm_end>/<stage-1 cfg dir>/timeseries/*.dat,*.inf   # dedispersed time series per DM trial
    <dm_start>_<dm_end>/<stage-1 cfg dir>/<cfg_suffix>/       # stage 2, DM range = union of the stage-1 ranges
        clustered_peaks.csv, hierarchical_summary.json        # clustering summary
        fold_ck000/output.xml, ...                            # stage-2 fold-search results per job
        merged/output.xml                                     # merged (and clustered) result that is folded
12_HIERARCHICAL_SEARCH/.../<stage-1 cfg dir>/<cfg_suffix>/merged/<fold group>/   # PulsarX folds
13_HIERARCHICAL_SEARCH/.../<stage-1 cfg dir>/<cfg_suffix>/merged/<fold group>/   # PICS and post-folding heuristics
```

The DM range directory is the union of the stage-1 ranges when `merge_dm_ranges = 1`.

The clustering and merging steps run inside the PulsarX container (they need numpy and scipy only).
Set `search_only = true` to stop after stage 2 without folding, and
`fold_stage1_candidates = 1` to fold the stage-1 candidates as well.

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
