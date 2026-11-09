#!/bin/bash
# Build the stage-2/3 image (hierarchical_fold.def) on a compute node of hercules:
#   sbatch src/hierarchical_fold/build_hierarchical_fold_sif.sbatch
# The image is named after the last commit that changed src/hierarchical_fold, so a new source version gives a
# new image name (and a new task hash in Nextflow). Commit the source before building.
#SBATCH -J hierarchical_fold_sif
#SBATCH -p short.q
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH -t 02:00:00
#SBATCH -o build_hierarchical_fold_sif_%j.log
set -euo pipefail

SRC_DIR=${SRC_DIR:-/hercules/scratch/jijin/COMPACT/hierarchical_search/one_ring/src/hierarchical_fold}
OUT_DIR=${OUT_DIR:-/hercules/u/jijin/singularity_images}
BASE_IMAGE=${BASE_IMAGE:-/hercules/u/jijin/singularity_images/peasoup_4f0472a_scipy.sif}

cd "$SRC_DIR"
if [ -n "$(git status --porcelain -- .)" ]; then
    echo "ERROR: uncommitted changes in $SRC_DIR; commit them so that the image name identifies the source" >&2
    git status --short -- . >&2
    exit 1
fi
commit=$(git log -1 --format=%h -- .)
out="$OUT_DIR/hierarchical_fold_${commit}.sif"
if [ -e "$out" ]; then
    echo "ERROR: $out exists already" >&2
    exit 1
fi

export APPTAINER_TMPDIR=/tmp/$USER/apptainer_tmp_$SLURM_JOB_ID APPTAINER_CACHEDIR=/tmp/$USER/apptainer_cache_$SLURM_JOB_ID
mkdir -p "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"
trap 'rm -rf "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"' EXIT

hostname; apptainer --version; echo "source $SRC_DIR at commit $commit, base $BASE_IMAGE -> $out"
apptainer build --fakeroot \
    --build-arg BASE_IMAGE="$BASE_IMAGE" \
    --build-arg GIT_COMMIT="$commit" \
    "$out" hierarchical_fold.def

# checks on the finished image (no GPU on short.q: the binary only prints its usage)
apptainer inspect --labels "$out"
apptainer exec "$out" bash -c 'ls -la /usr/local/bin/resample_timeseries; resample_timeseries 2>&1 | head -1'
apptainer exec "$out" python3 -c "import numpy, scipy, threadpoolctl; print('numpy', numpy.__version__, 'scipy', scipy.__version__)"
echo "built $out"
