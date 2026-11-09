#!/bin/bash
# Build an image of the pipeline on a hercules compute node, from the repository root:
#   sbatch src/build_sif.sbatch peasoup             # src/peasoup/peasoup.def                     -> peasoup_<commit>.sif
#   sbatch src/build_sif.sbatch hierarchical_fold   # src/hierarchical_fold/hierarchical_fold.def -> hierarchical_fold_<commit>.sif
# <commit> is the last commit that changed src/<name>, so a new source version gives a new image name (and a new
# Nextflow task hash). The source must be committed. Both images are compiled in DEVEL_IMAGE (nvcc, dedisp) and
# shipped on plain Ubuntu without the CUDA toolkit; the build needs internet (ubuntu:22.04, pip).
#SBATCH -J build_sif
#SBATCH -p short.q
#SBATCH -c 8
#SBATCH --mem=32G
#SBATCH -t 02:00:00
#SBATCH -o build_sif_%x_%j.log
set -euo pipefail

name=${1:?usage: sbatch src/build_sif.sbatch <peasoup|hierarchical_fold>}
REPO=${REPO:-$SLURM_SUBMIT_DIR}
OUT_DIR=${OUT_DIR:-/hercules/u/jijin/singularity_images}
DEVEL_IMAGE=${DEVEL_IMAGE:-/hercules/u/jijin/singularity_images/peasoup_4f0472a_scipy.sif}

src_dir="$REPO/src/$name"
def="$src_dir/$name.def"
[ -f "$def" ] || { echo "ERROR: no definition file $def" >&2; exit 1; }
cd "$src_dir"
if [ -n "$(git status --porcelain -- .)" ]; then
    echo "ERROR: uncommitted changes in $src_dir; commit them so that the image name identifies the source" >&2
    git status --short -- . >&2
    exit 1
fi
commit=$(git log -1 --format=%h -- .)
out="$OUT_DIR/${name}_${commit}.sif"
[ -e "$out" ] && { echo "ERROR: $out exists already" >&2; exit 1; }

export APPTAINER_TMPDIR=/tmp/$USER/apptainer_tmp_$SLURM_JOB_ID APPTAINER_CACHEDIR=/tmp/$USER/apptainer_cache_$SLURM_JOB_ID
mkdir -p "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"
trap 'rm -rf "$APPTAINER_TMPDIR" "$APPTAINER_CACHEDIR"' EXIT

hostname; apptainer --version; echo "src/$name at commit $commit, devel image $DEVEL_IMAGE -> $out"
apptainer build --fakeroot \
    --build-arg DEVEL_IMAGE="$DEVEL_IMAGE" \
    --build-arg GIT_COMMIT="$commit" \
    "$out" "$name.def"

# checks on the finished image (no GPU on short.q)
ls -la "$out"
apptainer inspect --labels "$out"
case "$name" in
    peasoup)
        apptainer exec "$out" bash -c 'ldd /usr/local/bin/peasoup | grep -E "cudart|cufft|dedisp"; peasoup --help 2>&1 | grep -E "normalise_dump|deredden_post_resample"; python -c "import numpy; print(\"numpy\", numpy.__version__)"; command -v uuidgen' ;;
    hierarchical_fold)
        apptainer exec "$out" bash -c 'resample_timeseries 2>&1 | head -1; python -c "import numpy, scipy, threadpoolctl; print(\"numpy\", numpy.__version__, \"scipy\", scipy.__version__)"' ;;
esac
echo "built $out"
