#!/usr/bin/env bash
# Build the time series resampler (peasoup kernels) with nvcc, e.g. inside the peasoup container:
#   apptainer exec <peasoup image> bash src/hierarchical_fold/build_resample_timeseries.sh [output path]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="${1:-$here/resample_timeseries}"
nvcc -O3 -std=c++17 \
    -gencode arch=compute_70,code=sm_70 -gencode arch=compute_75,code=sm_75 \
    -gencode arch=compute_80,code=sm_80 -gencode arch=compute_86,code=sm_86 \
    -gencode arch=compute_89,code=sm_89 -gencode arch=compute_89,code=compute_89 \
    -o "$out" "$here/resample_timeseries.cu"
echo "built $out"
