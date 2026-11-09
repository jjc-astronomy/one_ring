# Peasoup

**Peasoup** is a fast, GPU-accelerated time-domain pulsar search pipeline designed to discover compact binary pulsars in high time resolution radio observations. It operates on filterbank files and outputs XML files of candidate detections. It uses the [`dedisp`](https://github.com/vishnubk/dedisp) library (originally written by [`Ben Barsdell`](https://github.com/benbarsdell)) to perform incoherent dedispersion.

> ⚠️ Peasoup does **not** fold candidates. Use tools like [`psrfold_fil`](https://github.com/ypmen/PulsarX) (recommended), [`prepfold`](https://github.com/scottransom/presto), or [`dspsr`](https://dspsr.sourceforge.net/current/) to fold using candidate outputs.

---

## Table of Contents

* [New Features](#new-features-in-the-latest-version)
* [Installation](#installation)
* [Acceleration Search](#acceleration-search)

  * [Basic Usage for Full Length Acceleration Search](#basic-usage-for-full-length-acceleration-search)
  * [Segmented Acceleration Search](#segmented-acceleration-search)
  * [Folding Acceleration Candidates](#folding-acceleration-candidates)
* [Template Bank Searches](#template-bank-searches)

  * [Generating a Template-Bank](#generating-a-template-bank)
  * [Basic Usage](#basic-usage-template-bank-peasoup)
  * [Folding Keplerian Parameter Search Candidates](#folding-keplerian-parameter-search-candidates)
* [Acknowledgements](#acknowledgements)

---

## New Features in the Latest Version

* **Template Bank Searches**: `Peasoup` now supports going beyond acceleration and jerk searches to perform coherent Keplerian binary parameter searches using a template bank. Circular orbit templates use three parameters: orbital angular frequency (ω), projected semi-major axis (τ), and orbital phase (ϕ). Elliptical templates add eccentricity (e) and longitude of periastron (ωₚ), enabling detection of short-period compact orbit binaries.

* **CUDA 12.6 Compatibility**: Now builds and runs with CUDA versions up to 12.6. Previous issues with legacy texture memory in `dedisp` are fixed.

* **Segmented Acceleration Search Support**: Allows you to process specific chunks of a filterbank file using:

  * `--start_sample`: Starting sample index.
  * `--fft_size`: FFT window length.
  * `--nsamples` (optional): Explicit cap on number of real samples.

  **What’s the difference?**

  * `--fft_size`: Total number of samples to use for FFT. 
  * `--nsamples`: Upper bound on how many real samples.

  Recommended usage: use `--start_sample` and `--fft_size`. Use `--nsamples` only when you're intentionally truncating the read segment.

  Example scenarios:
  * `--start_sample=0.25*total`,  `--fft_size=0.5*total`: Peasoup reads `25%` to `75%` of the filterbank time samples.
  * `--start_sample=0.25*total`, `--nsamples=0.25*total`, `--fft_size=0.5*total`: Peasoup reads only `25%` to `50%` of the filterbank samples, and pads an additional `0.25*total` of zeros before searching.

* **Coherent DM Correction (`--cdm`)**: If your filterbank file has been coherently dedispersed to a non-zero DM, you can now inform Peasoup using the `--cdm` flag. This modifies the acceleration plan, making the search step size finer near the coherent DM value and improving sensitivity.

* **Single Precision Harmonic Sums**: Enabled via 
`--single_precision_harmonic_sums`. Reduces GPU memory and speeds up harmonic summing.

* **Presto-Compatible Dedispersed Time Series**: Run Peasoup in "dedispersion-only" mode with `-d` and `--nosearch`. This dumps `.dat` files compatible with PRESTO. No barycentric correction is applied. Equivalent to PRESTO’s `prepdata -nobary -dm $dm $input_file`.

* **Live Progress Bar**: Useful for multi-day or multi-week searches. Activate it with the `-p` flag.

---

## Installation

### Docker

```bash
docker pull vishnubk/peasoup
```

### Singularity / Apptainer

```bash
apptainer pull docker://vishnubk/peasoup
```

### Build from Source (Advanced)

```bash
git clone https://github.com/vishnubk/dedisp.git
cd dedisp
make
make install

cd ..
git clone https://github.com/vishnubk/peasoup.git
cd peasoup
make
make install
```

Peasoup supports GPU architectures from `sm_60` to `sm_90`. If you're using a different architecture, edit `Makefile.inc` and add the appropriate `-gencode` flags.

---

## Acceleration Search

Use `--acc_start`, `--acc_end`, and either `--dm_file` or the `--dm_start`/`--dm_end` range.

### Basic Usage for Full Length Acceleration Search

```bash
peasoup -i data.fil \
        --fft_size 67108864 \
        --limit 100000 \
        -m 7.0 \
        -o output_dir \
        -t 1 \
        --acc_start -50.0 \
        --acc_end 50.0 \
        --dm_file my_dm_trials.txt \
        --ram_limit_gb 180.0 \
        -n 4 \
        -p 
```

> ⚠️ Always specify `--fft_size`. Peasoup defaults to the next lowest power-of-two if it's missing. While `cuFFT` supports efficient FFTs for sizes composed of 2, 3, 5, or 7 as prime factors, performance can vary across GPUs. For large-scale surveys, we recommend benchmarking different FFT sizes and explicitly setting `--fft_size` to avoid issues from observations that fall slightly short of the next optimal size.

Peasoup still supports specifying a DM range using `--dm_start` and `--dm_end`, allowing dedisp to generate internal trial steps. However, we strongly recommend using the `--dm_file` flag to provide full control over dispersion trials.
You can generate `dm_file` using `DDplan.py` from PRESTO. An example file is available [here](https://github.com/vishnubk/peasoup/blob/master/examples/sample_input_files/dm_file.txt).

---

### Segmented Acceleration Search

```bash
peasoup -i data.fil \
        --start_sample 4194304 \
        --fft_size 33554432 \
        --acc_start -100.0 \
        --acc_end 100.0 \
        --dm_file dm_trials.txt \
        --cdm 33.0 \
        -n 4 -t 1 -p -o output_dir
```

---

### Folding Acceleration Candidates

#### Using PulsarX

Use the `segment_pepoch` from Peasoup’s XML output (example [here](https://github.com/vishnubk/peasoup/blob/master/examples/sample_output/acceleration_search.xml)) as the reference epoch.

```bash
psrfold_fil -v -t 12 --candfile pulsarx.candfile \
    -n 64 -b 64 --template meerkat_fold.template \
    -f data.fil --pepoch ${segment_pepoch} -o results
```

Example `pulsarx.candfile`:

```
#id DM accel F0 F1 F2 S/N
0 46.840000 -38.537781 275.478302 0 0 8.938008
1 46.750000 548.444214 162.808395 0 0 8.425230
```

Bonus Tip: Use `psrfold_fil2` for faster dedispersion+folding of hundreds of candidates.

#### Using prepfold (PRESTO)

Convert acceleration to Ṗ and shift period to the start of the FFT:

```python
def a_to_pdot(P_s, acc_ms2):
    c = 2.99792458e8
    return P_s * acc_ms2 / c

def period_correction_for_prepfold(p0, pdot, tsamp, fft_size):
    return p0 - pdot * fft_size * tsamp / 2
```

Then fold:

```bash
prepfold -topo -noxwin -p ${corrected_period} -pd ${pdot} -dm ${dm} data.fil
```

#### Using DSPSR

Example predictor file:

```
SOURCE: J1546-5431
EPOCH: 55739.5399653
PERIOD: 1.466892342 s
DM: 316.2835
ACC: 1.25571819897
RA: 15:46:48.00
DEC: -54:31:00.216
```

```bash
dspsr -P predictor.txt -O folded_output data.fil
```

---

## Template Bank Searches

### Generating a Template-Bank

Use the [`template_bank_generator`](https://github.com/erc-compact/template_bank_generator) repository to create a `.txt` file with one template per line. Each line should contain Keplerian parameters.

Examples are available [here](https://github.com/vishnubk/peasoup/blob/master/examples/sample_input_files).

---

### Basic Usage: Template Bank Peasoup

```bash
peasoup -i data.fil \
        --start_sample 4194304 \
        --fft_size 33554432 \
        -K circular_orbit_template_bank.txt \
        --dm_file dm_trials.txt \
        --cdm 33.0 \
        -n 4 -t 1 -p -o output_dir
```

### Segmented template bank Searches

Segmented template bank searches follow the same logic as segmented acceleration searches described above. Just pass the appropriate template bank file for the observation time corresponding to your search segment using the `-K` flag.

---

### Folding Keplerian Parameter Search Candidates

The `segment_pepoch` corresponds to the start of the FFT segment. No conversion required.

#### Using PulsarX

```bash
psrfold_fil -v -t 12 --candfile keplerian_pulsarx.candfile \
    -n 64 -b 64 --template meerkat_fold.template \
    -f data.fil --pepoch ${segment_pepoch} -o results
```

Example `keplerian_pulsarx.candfile`:

```
#id DM accel F0 F1 F2 Pb A1 T0 OM ECC S/N
0 20.0 0 399.9958801269538 0 0 0.04166666671427778 2.32141701093094 50000.02083333336 0 0 11.1497869491577
```

#### Using prepfold

```bash
prepfold -topo -noxwin -p ${xml_period} -dm ${dm} -bin \
         -pb ${xml_pb} -x ${xml_a1} -To ${xml_t0} -w ${xml_omega} -e ${xml_ecc} data.fil
```

Warning: prepfold's `-bin` option evaluates the orbital delay at the arrival time
rather than the emission time, which smears the profile of compact binaries
(the error grows as f0*a1^2/Pb). For short orbital periods fold with PulsarX,
or with `prepfold -par` using a tempo par file, instead.

Note: peasoup searches topocentric data, so the reported spin frequency, T0 and
orbital phase are topocentric. Folding the same filterbank with PulsarX or
prepfold needs no conversion, but tempo par files are barycentric by convention,
so if you build a par file from these values you must barycentre them first.

---

### Optional Features

* `--exact_resampler`: Use linear interpolation instead of nearest-neighbor. Both resamplers solve for the emission time, so results agree to ~1%; the nearest-neighbor gather is much faster and is the recommended default. You can use this flag if required as an independent cross-check.
* `--distill_circular_orbit_cands`: Enables candidate filtering in template bank mode (currently off by default).
* `--no_deredden`: Skip the running-median whitening of the Fourier spectrum altogether. Without whitening the
  red noise below ~1 Hz produces very many spurious candidates (raise `--min_freq` in that case).
* `--deredden_post_resample`: Whiten the spectrum of every *resampled* time series instead of once before resampling.
  By default peasoup whitens the unresampled spectrum (running medians of 5/25/125 bins, see `--boundary_5_freq`,
  `--boundary_25_freq`) and then resamples the whitened time series for every template. A binary pulsar is smeared
  over many bins in the unresampled spectrum, so the running median rides up onto it and the division removes a
  large part of its power. With this flag the pre-resampling whitening is skipped (birdie zapping is kept) and,
  for every trial, the power spectrum of the resampled series is divided by its own running-median envelope and
  normalised with the statistics of the whitened spectrum. A signal matched by the template is a narrow peak and
  survives the median. The extra cost per trial is a few memory-bound passes over the spectrum (measured: +10 %
  on a 25M-point FFT). Recorded in the XML as `deredden_post_resample`.
* `--boundary_125_freq <Hz>`, `--boundary_625_freq <Hz>`: use 625-bin running medians above the first frequency and
  3125-bin medians above the second (default: off, i.e. 125-bin medians everywhere above `--boundary_25_freq`).
  Recommended together with `--deredden_post_resample` (e.g. `--boundary_125_freq 5 --boundary_625_freq 50`): a
  template that is slightly mismatched leaves the signal smeared over tens to hundreds of bins, which would still
  lift a 125-bin median; red noise varies slowly at these frequencies, so the wider window costs nothing there.
  With the three options above, a synthetic binary smeared over ~900 bins keeps its full S/N (86 -> 91 vs 73 with
  the default whitening before resampling), while the sub-hertz red-noise candidates are removed.


## Benchmarks on A100 GPUs

Peasoup’s performance scales with FFT size and the number of harmonic sums. The table below shows the measured runtimes (in seconds) per binary trial, per DM trial, per beam on an A100 80 GB GPU. 

| FFT Size | 8 Harmonics (-n 3) | 16 Harmonics (-n 4)| 32 Harmonics (-n 5)|
| -------- | ----------- | ------------ | ------------ |
| 2²⁴      | 0.00087     | 0.00126      | 0.00139      |
| 2²⁵      | 0.00173     | 0.00252      | 0.00277      |
| 2²⁶      | 0.00327     | 0.00477      | 0.00525      |

These numbers are useful baselines when estimating total search runtime. Actual performance may vary depending on your GPU model and the amount of RFI in your data.


## Acknowledgements

Peasoup was developed by Ewan Barr with contributions from Vishnu, Vivek, Prajwal, Yunpeng, and Jiri. It has been used to discover over 200 pulsars. If you use Peasoup in your research, please cite this repository.


