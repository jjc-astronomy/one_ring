/*
 * resample_timeseries
 *
 * Resample a dedispersed time series (PRESTO-style .dat + .inf, as written by `peasoup -d`) into the
 * emission frame of a circular orbit, so that a pulsar in that orbit becomes a constant-frequency signal.
 * Used by the hierarchical search: the series is resampled once with the stage-1 peak template, folded into
 * sub-integrations, and the fine grid of templates is then searched by rotating the sub-integration profiles.
 *
 * The GPU kernels are copied verbatim from peasoup (https://github.com/vishnubk/peasoup, Apache License 2.0,
 * src/kernels.cu): getCircular_orbit_index, resample_circular_kernel, get_roemer_delay_circular,
 * subtract_roemer_delay_circular_kernel, bsearch_range and resample_using_1D_lerp_kernel. The host drivers
 * follow device_circular_orbit_resampler and exact_resampler_circular (src/pipeline_multi.cu). The same
 * arithmetic is also provided on the CPU for machines without a CUDA device (or with --cpu).
 *
 * Conventions (peasoup): t = idx * tsamp from the first sample of the series, mean anomaly = n * t - phi,
 * Roemer delay R = a1 * sin(n * t - phi) [s]; nearest neighbour: out[idx] = in[idx + R(t)/tsamp].
 *
 * Modes (--mode):
 *   nearest  peasoup's default: out[idx] = in[round(idx + R/tsamp)]  (identical to the stage-1 search)
 *   linear   same index mapping, but linear interpolation at the fractional index (same time origin and
 *            conventions as 'nearest'; note that interpolation correlates the noise of neighbouring samples,
 *            which biases the boxcar S/N upwards when the profile has more bins than samples per period)
 *   exact    peasoup's --exact_resampler path (inverts t = tau + R(tau), uniform grid from the minimum
 *            telescope time, linear interpolation); note that its output grid starts at a different time
 *
 * Usage:
 *   resample_timeseries -i in.dat -o out_prefix --n <rad/s> --a1 <lt-s> --phi <rad> [--mode M] [--cpu]
 *   resample_timeseries -i in.dat --templates list.txt --outdir dir [--mode M] [--cpu]
 *       list.txt: one template per line "n a1 phi" -> dir/<stem>_tmpl<k>.dat/.inf
 *   resample_timeseries -i in.dat --fold list.txt --nsub NSUB --nbins NBINS --out profiles.bin [--mode M] [--cpu]
 *   resample_timeseries -i in.dat --search list.txt --nsub NSUB --nbins NBINS --df_halfrange B --df_step S
 *                       --ducy_max D --out results.txt [--mode M] [--cpu]
 *       as --fold, but the folded profiles stay on the device: for every template the sub-integrations are
 *       shifted by the linear phase ramp of each frequency offset (-B..B Fourier bins in steps of S, i.e.
 *       offsets in units of 1/tobs), summed, normalised to unit noise (variances propagated through the
 *       interpolation weights) and scored with pyloki's boxcar matched filter (widths 1 .. D*NBINS);
 *       results.txt has one line per template: "snr width df_bins".
 *       list.txt: one line per template "n a1 phi f0": the series is resampled with the template and folded
 *       at f0 (phase = f0 * idx * tsamp) into NSUB sub-integrations of NBINS bins, in one pass per template.
 *       profiles.bin: for every template float32 [NSUB][NBINS] summed intensities followed by float32 [NSUB][NBINS]
 *       noise variances of the bins (1 per sample for nearest, (1-w)^2+w^2 per interpolated sample), so that
 *       profile/sqrt(variance) has unit noise. The series is normalised (median, MAD) before folding, unless its
 *       .inf carries the marker written by 'hierarchical_search.py normalise' (normalised once after the dump).
 *
 * Build (inside the peasoup container):  nvcc -O3 -o resample_timeseries resample_timeseries.cu
 */

#include <cuda_runtime.h>
#include <thrust/device_vector.h>
#include <thrust/host_vector.h>
#include <thrust/extrema.h>
#include <thrust/sequence.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// ------------------------------------------------------------------------------------------------------
// peasoup kernels (verbatim)
// ------------------------------------------------------------------------------------------------------

__device__ unsigned long getCircular_orbit_index(unsigned long idx,
                                  double n, double a1, double phi, double zero_offset,
                                  double tsamp, double inverse_tsamp)

{

    double t = idx * tsamp;
    //double mean_anomaly = n * t + phi;
    double mean_anomaly = n * t - phi;
    double sine_mean_anomaly = sin(mean_anomaly);
    //return __double2ull_rn(idx - (a1 * sine_mean_anomaly * inverse_tsamp - zero_offset));
    return __double2ull_rn(idx + (a1 * sine_mean_anomaly * inverse_tsamp - zero_offset));

}


__global__ void resample_circular_kernel(float* input_d,
                                  float* output_d,
                                  double n, double a1, double phi, double zero_offset,
                                  double tsamp, double inverse_tsamp,
                                  double size)

{
  for( unsigned long idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
  {
    unsigned long out_idx = getCircular_orbit_index(idx, n, a1, phi, zero_offset,
                                  tsamp, inverse_tsamp);
    if (out_idx <= size - 1){
        output_d[idx] = input_d[out_idx];
    }
    else {
        output_d[idx] = 0.0; // Zero padding if resampled data needs a bin above fft_size
    }
  }
}

__device__ double get_roemer_delay_circular(unsigned long idx,
    double n, double a1, double phi, double tsamp)

{

double t = idx * tsamp;
/* A pulse emitted at tau arrives at t = tau + R(tau). Evaluating the
   delay at the arrival time instead drops the R*Rdot retardation term
   and smears compact binaries. Invert t = tau + R(tau) by fixed-point
   iteration: converges geometrically (ratio a1*n << 1), 5 iterations
   give residuals below 1e-12 s. */
double tau = t;
for (int iteration = 0; iteration < 5; iteration++)
{
    tau = t - a1 * sin(n * tau - phi);
}

return t - tau;
}


__global__ void subtract_roemer_delay_circular_kernel(double* d_t_binary_grid_ptr, double* d_t_telescope_nonuniform_ptr,
    double n, double a1, double phi, double tsamp, double size)

{
for( unsigned long idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
{
    double roemer_delay = get_roemer_delay_circular(idx, n, a1, phi, tsamp);
    //Subtracting the roemer delay from the binary grid time to get the telescope non-uniform time
    d_t_telescope_nonuniform_ptr[idx] = d_t_binary_grid_ptr[idx] - roemer_delay;


}
}

__device__ void bsearch_range(double *a, double key, unsigned long len_a, unsigned long *idx){
    unsigned long lower = 0;
    unsigned long upper = len_a;
    unsigned long midpt;
    while (lower < upper){
      // '>>1' is the right bitshift operator which is equivalent to dividing by 2 for unsigned numbers.
      midpt = (lower + upper)>>1;
      if (a[midpt] < key) lower = midpt +1;
      else upper = midpt;
      }
    *idx = lower;
    return;
    }

__global__ void resample_using_1D_lerp_kernel(double *d_t_telescope_nonuniform_ptr, float  *input_d, unsigned long size, double *d_t_binary_target_ptr, float *output_d){
    for (unsigned long i = threadIdx.x+blockDim.x*blockIdx.x; i < size; i+=gridDim.x*blockDim.x){
      double target_val = d_t_binary_target_ptr[i];
      if ((target_val > d_t_telescope_nonuniform_ptr[0]) && (target_val <= d_t_telescope_nonuniform_ptr[size - 1])){
        unsigned long idx;
        bsearch_range(d_t_telescope_nonuniform_ptr, target_val, size, &idx);
        double xlv = d_t_telescope_nonuniform_ptr[idx - 1];
        double xrv = d_t_telescope_nonuniform_ptr[idx];
        double ylv = input_d[idx - 1];
        double yrv = input_d[idx];
       // y  =      m                *   x       + b
       output_d[i] = ((yrv-ylv)/(xrv-xlv)) * (target_val-xlv) + ylv;
      }
    }
  }

/* linear interpolation at the fractional index of getCircular_orbit_index (same grid and conventions) */
__global__ void resample_circular_linear_kernel(float* input_d, float* output_d, double n, double a1, double phi,
                                                double tsamp, double inverse_tsamp, double size)
{
  for( unsigned long idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
  {
    double t = idx * tsamp;
    double pos = idx + a1 * sin(n * t - phi) * inverse_tsamp;
    double fl = floor(pos);
    double w = pos - fl;
    long long i0 = (long long) fl;
    if (i0 >= 0 && i0 + 1 <= (long long) size - 1) {
        output_d[idx] = (float) ((1.0 - w) * input_d[i0] + w * input_d[i0 + 1]);
    } else if (i0 >= 0 && i0 <= (long long) size - 1) {
        output_d[idx] = input_d[i0];
    } else {
        output_d[idx] = 0.0;
    }
  }
}

/* resample (nearest or linear) and fold in one pass: one block per sub-integration, shared-memory histogram.
   prof_d accumulates the (unit-variance) samples, cnt_d the noise variance of each bin: 1 per sample for nearest
   neighbour, (1-w)^2 + w^2 for a linearly interpolated sample, so that prof/sqrt(cnt) has unit noise. */
__global__ void fold_template_kernel(const float* __restrict__ input_d, float* __restrict__ prof_d, float* __restrict__ cnt_d,
                                     double n, double a1, double phi, double f0, double tsamp, double inverse_tsamp,
                                     unsigned long size, int nsub, int nbins, int linear)
{
    extern __shared__ float sh[];
    float* hist = sh;                 // nbins
    float* cnt  = sh + nbins;         // nbins
    for (int b = threadIdx.x; b < nbins; b += blockDim.x) { hist[b] = 0.0f; cnt[b] = 0.0f; }
    __syncthreads();
    int isub = blockIdx.x;
    unsigned long start = ((unsigned long) isub * size) / nsub;
    unsigned long end   = ((unsigned long) (isub + 1) * size) / nsub;
    for (unsigned long idx = start + threadIdx.x; idx < end; idx += blockDim.x) {
        double t = idx * tsamp;
        double pos = idx + a1 * sin(n * t - phi) * inverse_tsamp;      // getCircular_orbit_index, fractional
        float v, var; bool ok;
        if (linear) {
            double fl = floor(pos); double w = pos - fl; long long i0 = (long long) fl;
            if (i0 >= 0 && i0 + 1 <= (long long) size - 1) { v = (float) ((1.0 - w) * input_d[i0] + w * input_d[i0 + 1]); var = (float) ((1.0 - w) * (1.0 - w) + w * w); ok = true; }
            else if (i0 >= 0 && i0 <= (long long) size - 1) { v = input_d[i0]; var = 1.0f; ok = true; }
            else { v = 0.0f; var = 0.0f; ok = false; }
        } else {
            long long i0 = (long long) nearbyint(pos);
            ok = (i0 >= 0 && i0 <= (long long) size - 1);
            v = ok ? input_d[i0] : 0.0f; var = 1.0f;
        }
        if (!ok) continue;
        double ph = f0 * t;
        ph -= floor(ph);
        int b = (int) (ph * nbins); if (b >= nbins) b = nbins - 1;
        atomicAdd(&hist[b], v);
        atomicAdd(&cnt[b], var);
    }
    __syncthreads();
    for (int b = threadIdx.x; b < nbins; b += blockDim.x) {
        prof_d[(size_t) isub * nbins + b] = hist[b];
        cnt_d[(size_t) isub * nbins + b]  = cnt[b];
    }
}

/* frequency search + scoring of folded profiles: one block per template.
   prof: [nt][nsub][nbins] summed intensities, var: [nt][nsub][nbins] noise variances of the bins.
   For each offset df (in Fourier bins of 1/tobs): sub-integration j is rotated by s_j = df * t_j/tobs turns,
   rounded to whole bins (p_rot[b] = p[b - round(s)], as pyloki's real-valued shift_add: no interpolation, so the
   noise of neighbouring bins stays independent), summed over j, normalised to unit noise and scored with the
   boxcar widths (pyloki statistic, stdnoise = 1); the best (snr, width, df) over offsets and widths is kept.
   nbins <= 1024 (one thread per bin). */
__device__ float block_max(float v, float* red) {
    red[threadIdx.x] = v; __syncthreads();
    for (int s = blockDim.x / 2; s > 0; s >>= 1) { if (threadIdx.x < s) red[threadIdx.x] = fmaxf(red[threadIdx.x], red[threadIdx.x + s]); __syncthreads(); }
    float r = red[0]; __syncthreads(); return r;
}

__global__ void search_profiles_kernel(const float* __restrict__ prof, const float* __restrict__ var, int nsub, int nbins,
                                       const double* __restrict__ t_frac, const double* __restrict__ dfs, int ndf,
                                       const int* __restrict__ widths, int nw,
                                       float* best_snr, int* best_w, int* best_idf)
{
    extern __shared__ float shm[];
    float* sum   = shm;                  // nbins
    float* pre   = shm + nbins;          // 2*nbins+1 prefix sums
    float* red   = shm + 3 * nbins + 1;  // blockDim.x reduction
    int t = blockIdx.x; int b = threadIdx.x;
    const float* P = prof + (size_t) t * nsub * nbins;
    const float* V = var + (size_t) t * nsub * nbins;
    float bsnr = -1e30f; int bw = 0, bidf = 0;
    for (int idf = 0; idf < ndf; ++idf) {
        if (b < nbins) {
            float acc = 0.f, vacc = 0.f;
            for (int j = 0; j < nsub; ++j) {
                int sh = (int) nearbyint(dfs[idf] * t_frac[j] * nbins);  // whole-bin shift
                int i0 = ((b - sh) % nbins + nbins) % nbins;              // p_rot[b] = p[b - s]
                acc  += P[j * nbins + i0];
                vacc += V[j * nbins + i0];
            }
            sum[b] = vacc > 0.f ? acc * rsqrtf(vacc) : 0.f;             // unit noise per bin
        }
        __syncthreads();
        if (b == 0) { pre[0] = 0.f; for (int i = 0; i < 2 * nbins; ++i) pre[i + 1] = pre[i] + sum[i % nbins]; }
        __syncthreads();
        float total = pre[nbins];
        for (int iw = 0; iw < nw; ++iw) {
            int w = widths[iw];
            float v = (b < nbins) ? (pre[b + w] - pre[b]) : -1e30f;
            float dmax = block_max(v, red);
            float height = sqrtf((float) (nbins - w) / ((float) nbins * w));
            float bb = w * height / (nbins - w);
            float snr = (height + bb) * dmax - bb * total;
            if (snr > bsnr) { bsnr = snr; bw = w; bidf = idf; }
        }
        __syncthreads();
    }
    if (b == 0) { best_snr[t] = bsnr; best_w[t] = bw; best_idf[t] = bidf; }
}

static void cpu_search_profile(const std::vector<float>& prof, const std::vector<float>& var, int nsub, int nbins,
                               const std::vector<double>& t_frac, const std::vector<double>& dfs, const std::vector<int>& widths,
                               float& best_snr, int& best_w, int& best_idf)
{
    std::vector<float> sum(nbins), pre(2 * nbins + 1);
    best_snr = -1e30f; best_w = 0; best_idf = 0;
    for (size_t idf = 0; idf < dfs.size(); ++idf) {
        for (int b = 0; b < nbins; ++b) {
            float acc = 0.f, vacc = 0.f;
            for (int j = 0; j < nsub; ++j) {
                int sh = (int) nearbyint(dfs[idf] * t_frac[j] * nbins);
                int i0 = ((b - sh) % nbins + nbins) % nbins;
                acc  += prof[j * nbins + i0];
                vacc += var[j * nbins + i0];
            }
            sum[b] = vacc > 0.f ? acc / sqrtf(vacc) : 0.f;
        }
        pre[0] = 0; for (int i = 0; i < 2 * nbins; ++i) pre[i + 1] = pre[i] + sum[i % nbins];
        float total = pre[nbins];
        for (size_t iw = 0; iw < widths.size(); ++iw) {
            int w = widths[iw]; float dmax = -1e30f;
            for (int b = 0; b < nbins; ++b) dmax = std::max(dmax, pre[b + w] - pre[b]);
            float height = sqrtf((float) (nbins - w) / ((float) nbins * w)); float bb = w * height / (nbins - w);
            float snr = (height + bb) * dmax - bb * total;
            if (snr > best_snr) { best_snr = snr; best_w = w; best_idf = (int) idf; }
        }
    }
}

// ------------------------------------------------------------------------------------------------------
// host side
// ------------------------------------------------------------------------------------------------------

static const unsigned int MAX_THREADS = 256;
static const unsigned int MAX_BLOCKS  = 65535;

static void check_cuda(const char* what) {
    cudaError_t err = cudaGetLastError();
    if (err == cudaSuccess) err = cudaDeviceSynchronize();
    if (err != cudaSuccess) {
        std::cerr << "CUDA error (" << what << "): " << cudaGetErrorString(err) << std::endl;
        std::exit(2);
    }
}

static unsigned int nblocks(size_t size) {
    unsigned int blocks = size / MAX_THREADS + 1;
    return blocks > MAX_BLOCKS ? MAX_BLOCKS : blocks;
}

struct Template { double n, a1, phi; };

/* nearest-neighbour resampling, as device_circular_orbit_resampler (zero_offset = 0) */
static void gpu_resample_nearest(float* d_in, float* d_out, size_t size, double tsamp, const Template& t) {
    double zero_offset = 0.0;
    resample_circular_kernel<<< nblocks(size), MAX_THREADS >>>(d_in, d_out, t.n, t.a1, t.phi, zero_offset,
                                                              tsamp, 1.0 / tsamp, (double) size);
    check_cuda("resample_circular_kernel");
}

static void gpu_resample_linear(float* d_in, float* d_out, size_t size, double tsamp, const Template& t) {
    resample_circular_linear_kernel<<< nblocks(size), MAX_THREADS >>>(d_in, d_out, t.n, t.a1, t.phi, tsamp, 1.0 / tsamp, (double) size);
    check_cuda("resample_circular_linear_kernel");
}

/* linear-interpolation resampling, as exact_resampler_circular */
static void gpu_resample_exact(float* d_in, float* d_out, size_t size, double tsamp, const Template& t,
                               thrust::device_vector<double>& d_grid, thrust::device_vector<double>& d_tel,
                               thrust::device_vector<double>& d_target) {
    double binary_start_time = t.phi / t.n;
    thrust::sequence(d_grid.begin(), d_grid.end(), binary_start_time, tsamp);
    subtract_roemer_delay_circular_kernel<<< nblocks(size), MAX_THREADS >>>(
        thrust::raw_pointer_cast(d_grid.data()), thrust::raw_pointer_cast(d_tel.data()),
        t.n, t.a1, t.phi, tsamp, (double) size);
    check_cuda("subtract_roemer_delay_circular_kernel");
    double minele = *thrust::min_element(d_tel.begin(), d_tel.end());
    thrust::sequence(d_target.begin(), d_target.end(), minele, tsamp);
    cudaMemset(d_out, 0, size * sizeof(float));
    resample_using_1D_lerp_kernel<<< nblocks(size), MAX_THREADS >>>(
        thrust::raw_pointer_cast(d_tel.data()), d_in, size, thrust::raw_pointer_cast(d_target.data()), d_out);
    check_cuda("resample_using_1D_lerp_kernel");
}

/* CPU versions of the same arithmetic */
static void cpu_resample_nearest(const std::vector<float>& in, std::vector<float>& out, double tsamp, const Template& t) {
    size_t size = in.size();
    double inverse_tsamp = 1.0 / tsamp;
    for (size_t idx = 0; idx < size; ++idx) {
        double tt = idx * tsamp;
        double sine_mean_anomaly = sin(t.n * tt - t.phi);
        double shifted = idx + (t.a1 * sine_mean_anomaly * inverse_tsamp);
        long long out_idx = (long long) nearbyint(shifted);          // __double2ull_rn: round to nearest even
        out[idx] = (out_idx >= 0 && (size_t) out_idx <= size - 1) ? in[(size_t) out_idx] : 0.0f;
    }
}

static void cpu_resample_linear(const std::vector<float>& in, std::vector<float>& out, double tsamp, const Template& t) {
    size_t size = in.size();
    double inverse_tsamp = 1.0 / tsamp;
    for (size_t idx = 0; idx < size; ++idx) {
        double tt = idx * tsamp;
        double pos = idx + t.a1 * sin(t.n * tt - t.phi) * inverse_tsamp;
        double fl = floor(pos), w = pos - fl;
        long long i0 = (long long) fl;
        if (i0 >= 0 && i0 + 1 <= (long long) size - 1) out[idx] = (float) ((1.0 - w) * in[i0] + w * in[i0 + 1]);
        else if (i0 >= 0 && i0 <= (long long) size - 1) out[idx] = in[i0];
        else out[idx] = 0.0f;
    }
}

static void cpu_resample_exact(const std::vector<float>& in, std::vector<float>& out, double tsamp, const Template& t) {
    size_t size = in.size();
    std::vector<double> tel(size);
    double binary_start_time = t.phi / t.n;
    for (size_t idx = 0; idx < size; ++idx) {
        double tt = idx * tsamp;
        double tau = tt;
        for (int it = 0; it < 5; ++it) tau = tt - t.a1 * sin(t.n * tau - t.phi);
        tel[idx] = (binary_start_time + idx * tsamp) - (tt - tau);
    }
    double minele = tel[0];
    for (size_t i = 1; i < size; ++i) if (tel[i] < minele) minele = tel[i];
    std::fill(out.begin(), out.end(), 0.0f);
    for (size_t i = 0; i < size; ++i) {
        double target_val = minele + i * tsamp;
        if (target_val > tel[0] && target_val <= tel[size - 1]) {
            size_t lower = 0, upper = size;
            while (lower < upper) { size_t mid = (lower + upper) >> 1; if (tel[mid] < target_val) lower = mid + 1; else upper = mid; }
            size_t idx = lower;
            double xlv = tel[idx - 1], xrv = tel[idx];
            double ylv = in[idx - 1], yrv = in[idx];
            out[i] = (float) (((yrv - ylv) / (xrv - xlv)) * (target_val - xlv) + ylv);
        }
    }
}

struct FoldTemplate { double n, a1, phi, f0; };

static void cpu_fold_template(const std::vector<float>& in, std::vector<float>& prof, std::vector<float>& cnt,
                              double tsamp, const FoldTemplate& t, int nsub, int nbins, bool linear) {
    size_t size = in.size();
    double inverse_tsamp = 1.0 / tsamp;
    std::fill(prof.begin(), prof.end(), 0.0f); std::fill(cnt.begin(), cnt.end(), 0.0f);
    for (size_t idx = 0; idx < size; ++idx) {
        double tt = idx * tsamp;
        double pos = idx + t.a1 * sin(t.n * tt - t.phi) * inverse_tsamp;
        float v, var; bool ok;
        if (linear) {
            double fl = floor(pos), w = pos - fl; long long i0 = (long long) fl;
            if (i0 >= 0 && i0 + 1 <= (long long) size - 1) { v = (float) ((1.0 - w) * in[i0] + w * in[i0 + 1]); var = (float) ((1.0 - w) * (1.0 - w) + w * w); ok = true; }
            else if (i0 >= 0 && i0 <= (long long) size - 1) { v = in[i0]; var = 1.0f; ok = true; }
            else { v = 0.0f; var = 0.0f; ok = false; }
        } else {
            long long i0 = (long long) nearbyint(pos);
            ok = (i0 >= 0 && i0 <= (long long) size - 1); v = ok ? in[i0] : 0.0f; var = 1.0f;
        }
        if (!ok) continue;
        double ph = t.f0 * tt; ph -= floor(ph);
        int b = (int) (ph * nbins); if (b >= nbins) b = nbins - 1;
        int isub = (int) ((idx * (unsigned long) nsub) / size);
        prof[(size_t) isub * nbins + b] += v; cnt[(size_t) isub * nbins + b] += var;
    }
}

static void normalise_series(std::vector<float>& x) {
    std::vector<float> tmp(x);
    std::nth_element(tmp.begin(), tmp.begin() + tmp.size() / 2, tmp.end());
    float med = tmp[tmp.size() / 2];
    for (size_t i = 0; i < tmp.size(); ++i) tmp[i] = fabsf(x[i] - med);
    std::nth_element(tmp.begin(), tmp.begin() + tmp.size() / 2, tmp.end());
    float mad = tmp[tmp.size() / 2] * 1.4826f;
    if (mad <= 0) mad = 1.0f;
    for (size_t i = 0; i < x.size(); ++i) x[i] = (x[i] - med) / mad;
}

// ------------------------------------------------------------------------------------------------------
// files
// ------------------------------------------------------------------------------------------------------

static std::string stem_of(const std::string& path) {
    size_t slash = path.find_last_of('/');
    std::string base = slash == std::string::npos ? path : path.substr(slash + 1);
    size_t dot = base.find_last_of('.');
    return dot == std::string::npos ? base : base.substr(0, dot);
}

static std::string inf_of(const std::string& dat) {
    size_t dot = dat.find_last_of('.');
    return (dot == std::string::npos ? dat : dat.substr(0, dot)) + ".inf";
}

static bool read_inf(const std::string& inf, double& tsamp, size_t& nsamples, std::string& text) {
    std::ifstream f(inf);
    if (!f) return false;
    std::stringstream buf; buf << f.rdbuf(); text = buf.str();
    tsamp = 0; nsamples = 0;
    std::istringstream lines(text); std::string line;
    while (std::getline(lines, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq), val = line.substr(eq + 1);
        if (key.find("Width of each time series bin") != std::string::npos) tsamp = atof(val.c_str());
        else if (key.find("Number of bins in the time series") != std::string::npos) nsamples = (size_t) atoll(val.c_str());
    }
    return tsamp > 0;
}

static void write_output(const std::string& prefix, const std::vector<float>& out, const std::string& inf_text, const Template& t, const std::string& mode) {
    std::ofstream d(prefix + ".dat", std::ios::binary);
    d.write(reinterpret_cast<const char*>(out.data()), out.size() * sizeof(float));
    std::ofstream i(prefix + ".inf");
    i << inf_text;
    if (!inf_text.empty() && inf_text.back() != '\n') i << "\n";
    i << " Resampled with circular orbit (peasoup convention): n = " << t.n << " rad/s, a1 = " << t.a1
      << " lt-s, phi = " << t.phi << " rad, method = " << mode << "\n";
}

static void usage() {
    std::cerr << "usage: resample_timeseries -i in.dat (-o out_prefix --n N --a1 A1 --phi PHI | --templates list.txt --outdir dir | --fold list.txt --nsub NSUB --nbins NBINS --out profiles.bin | --search list.txt --nsub NSUB --nbins NBINS --df_halfrange B --df_step S --ducy_max D --out results.txt) [--mode nearest|linear|exact] [--cpu]\n";
    std::exit(1);
}

int main(int argc, char** argv) {
    std::string input, out_prefix, templates_file, outdir = ".";
    Template single{0, 0, 0}; bool have_single = false, force_cpu = false; std::string mode = "nearest";
    std::string fold_file, fold_out = "profiles.bin"; int nsub = 128, nbins = 64;
    bool do_search = false; double df_halfrange = 0.6, df_step = 0.05, ducy_max = 0.3;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&](void) -> std::string { if (i + 1 >= argc) usage(); return argv[++i]; };
        if (a == "-i") input = next();
        else if (a == "-o") out_prefix = next();
        else if (a == "--n") { single.n = atof(next().c_str()); have_single = true; }
        else if (a == "--a1") { single.a1 = atof(next().c_str()); have_single = true; }
        else if (a == "--phi") { single.phi = atof(next().c_str()); have_single = true; }
        else if (a == "--templates") templates_file = next();
        else if (a == "--outdir") outdir = next();
        else if (a == "--exact") mode = "exact";
        else if (a == "--mode") mode = next();
        else if (a == "--fold") fold_file = next();
        else if (a == "--search") { fold_file = next(); do_search = true; }
        else if (a == "--df_halfrange") df_halfrange = atof(next().c_str());
        else if (a == "--df_step") df_step = atof(next().c_str());
        else if (a == "--ducy_max") ducy_max = atof(next().c_str());
        else if (a == "--nsub") nsub = atoi(next().c_str());
        else if (a == "--nbins") nbins = atoi(next().c_str());
        else if (a == "--out") fold_out = next();
        else if (a == "--cpu") force_cpu = true;
        else usage();
    }
    if (input.empty() || (!have_single && templates_file.empty() && fold_file.empty())) usage();
    if (mode != "nearest" && mode != "linear" && mode != "exact") usage();
    bool exact = (mode == "exact");
    if (!fold_file.empty() && exact) { std::cerr << "--fold supports --mode nearest or linear\n"; return 1; }

    double tsamp; size_t nsamples; std::string inf_text;
    if (!read_inf(inf_of(input), tsamp, nsamples, inf_text)) { std::cerr << "cannot read " << inf_of(input) << std::endl; return 1; }
    std::ifstream f(input, std::ios::binary);
    if (!f) { std::cerr << "cannot read " << input << std::endl; return 1; }
    f.seekg(0, std::ios::end); size_t nfile = (size_t) f.tellg() / sizeof(float); f.seekg(0);
    size_t size = (nsamples > 0 && nsamples < nfile) ? nsamples : nfile;
    std::vector<float> in(size), out(size);
    f.read(reinterpret_cast<char*>(in.data()), size * sizeof(float));

    int ndev0 = 0;
    bool gpu0 = !force_cpu && cudaGetDeviceCount(&ndev0) == cudaSuccess && ndev0 > 0;
    cudaGetLastError();

    if (!fold_file.empty()) {
        // ---------------- fused resample + fold of every template ----------------
        std::vector<FoldTemplate> fts;
        std::ifstream tf(fold_file); std::string line;
        while (std::getline(tf, line)) {
            if (line.empty() || line[0] == '#') continue;
            FoldTemplate t; std::istringstream ls(line);
            if (ls >> t.n >> t.a1 >> t.phi >> t.f0) fts.push_back(t);
        }
        // series normalised once after the dump (hierarchical_search.py normalise) carry a marker in their .inf
        bool prenormalised = inf_text.find("Normalised in place: (x - median) / (1.4826 * MAD)") != std::string::npos;
        if (!prenormalised) normalise_series(in);
        bool linear = (mode == "linear");
        // frequency offsets (Fourier bins of 1/tobs) and boxcar widths for --search
        std::vector<double> dfs; for (double d = -df_halfrange; d <= df_halfrange + 1e-9; d += df_step) dfs.push_back(d);
        std::vector<int> widths; { int w = 1; int wmax = std::max(1, (int) (ducy_max * nbins)); while (w <= wmax) { widths.push_back(w); int nxt = std::max(w + 1, (int) lround(w * 1.5)); w = nxt; } }
        std::vector<double> t_frac(nsub); for (int j = 0; j < nsub; ++j) t_frac[j] = (j + 0.5) / nsub;   // t_j / tobs
        if (nbins > 1024) { std::cerr << "nbins must be <= 1024\n"; return 1; }
        std::cerr << "resample_timeseries " << (do_search ? "search" : "fold") << ": " << size << " samples, " << fts.size()
                  << " templates, nsub " << nsub << ", nbins " << nbins << ", " << mode << ", " << (gpu0 ? "GPU" : "CPU")
                  << (prenormalised ? ", input already normalised" : ", input normalised here");
        if (do_search) std::cerr << ", " << dfs.size() << " frequency offsets, " << widths.size() << " widths";
        std::cerr << std::endl;
        std::ofstream of(fold_out, do_search ? std::ios::out : std::ios::binary);
        std::vector<float> prof((size_t) nsub * nbins), cnt((size_t) nsub * nbins);
        if (gpu0) {
            const int batch = 256;
            float *d_in, *d_prof, *d_cnt, *d_snr; int *d_w, *d_idf; double *d_tfrac, *d_dfs; int *d_widths;
            cudaMalloc(&d_in, size * sizeof(float)); cudaMalloc(&d_prof, (size_t) batch * prof.size() * sizeof(float)); cudaMalloc(&d_cnt, (size_t) batch * cnt.size() * sizeof(float));
            cudaMalloc(&d_snr, batch * sizeof(float)); cudaMalloc(&d_w, batch * sizeof(int)); cudaMalloc(&d_idf, batch * sizeof(int));
            cudaMalloc(&d_tfrac, nsub * sizeof(double)); cudaMalloc(&d_dfs, dfs.size() * sizeof(double)); cudaMalloc(&d_widths, widths.size() * sizeof(int));
            cudaMemcpy(d_in, in.data(), size * sizeof(float), cudaMemcpyHostToDevice);
            cudaMemcpy(d_tfrac, t_frac.data(), nsub * sizeof(double), cudaMemcpyHostToDevice);
            cudaMemcpy(d_dfs, dfs.data(), dfs.size() * sizeof(double), cudaMemcpyHostToDevice);
            cudaMemcpy(d_widths, widths.data(), widths.size() * sizeof(int), cudaMemcpyHostToDevice);
            check_cuda("upload");
            size_t shmem_fold = 2 * (size_t) nbins * sizeof(float);
            int sthreads = 1; while (sthreads < nbins) sthreads <<= 1; if (sthreads < 32) sthreads = 32;
            size_t shmem_search = (3 * (size_t) nbins + 1 + sthreads) * sizeof(float);
            std::vector<float> bsnr(batch); std::vector<int> bw(batch), bidf(batch);
            for (size_t k0 = 0; k0 < fts.size(); k0 += batch) {
                int nb = (int) std::min((size_t) batch, fts.size() - k0);
                for (int k = 0; k < nb; ++k) {
                    fold_template_kernel<<< nsub, MAX_THREADS, shmem_fold >>>(d_in, d_prof + (size_t) k * prof.size(), d_cnt + (size_t) k * cnt.size(),
                        fts[k0 + k].n, fts[k0 + k].a1, fts[k0 + k].phi, fts[k0 + k].f0, tsamp, 1.0 / tsamp, size, nsub, nbins, linear ? 1 : 0);
                }
                check_cuda("fold_template_kernel");
                if (do_search) {
                    search_profiles_kernel<<< nb, sthreads, shmem_search >>>(d_prof, d_cnt, nsub, nbins, d_tfrac, d_dfs, (int) dfs.size(),
                                                                            d_widths, (int) widths.size(), d_snr, d_w, d_idf);
                    check_cuda("search_profiles_kernel");
                    cudaMemcpy(bsnr.data(), d_snr, nb * sizeof(float), cudaMemcpyDeviceToHost);
                    cudaMemcpy(bw.data(), d_w, nb * sizeof(int), cudaMemcpyDeviceToHost);
                    cudaMemcpy(bidf.data(), d_idf, nb * sizeof(int), cudaMemcpyDeviceToHost);
                    for (int k = 0; k < nb; ++k) { char line[96]; snprintf(line, sizeof line, "%.6f %d %.6f\n", bsnr[k], bw[k], dfs[bidf[k]]); of << line; }
                } else {
                    for (int k = 0; k < nb; ++k) {
                        cudaMemcpy(prof.data(), d_prof + (size_t) k * prof.size(), prof.size() * sizeof(float), cudaMemcpyDeviceToHost);
                        cudaMemcpy(cnt.data(), d_cnt + (size_t) k * cnt.size(), cnt.size() * sizeof(float), cudaMemcpyDeviceToHost);
                        of.write(reinterpret_cast<const char*>(prof.data()), prof.size() * sizeof(float));
                        of.write(reinterpret_cast<const char*>(cnt.data()), cnt.size() * sizeof(float));
                    }
                }
            }
            check_cuda("download");
            cudaFree(d_in); cudaFree(d_prof); cudaFree(d_cnt); cudaFree(d_snr); cudaFree(d_w); cudaFree(d_idf); cudaFree(d_tfrac); cudaFree(d_dfs); cudaFree(d_widths);
        } else {
            for (size_t k = 0; k < fts.size(); ++k) {
                cpu_fold_template(in, prof, cnt, tsamp, fts[k], nsub, nbins, linear);
                if (do_search) {
                    float bsnr; int bw, bidf;
                    cpu_search_profile(prof, cnt, nsub, nbins, t_frac, dfs, widths, bsnr, bw, bidf);
                    char line[96]; snprintf(line, sizeof line, "%.6f %d %.6f\n", bsnr, bw, dfs[bidf]); of << line;
                } else {
                    of.write(reinterpret_cast<const char*>(prof.data()), prof.size() * sizeof(float));
                    of.write(reinterpret_cast<const char*>(cnt.data()), cnt.size() * sizeof(float));
                }
            }
        }
        std::cout << fold_out << std::endl;
        return 0;
    }

    std::vector<Template> templates; std::vector<std::string> prefixes;
    if (have_single) { templates.push_back(single); prefixes.push_back(out_prefix.empty() ? stem_of(input) + "_resampled" : out_prefix); }
    else {
        std::ifstream tf(templates_file); std::string line; int k = 0;
        while (std::getline(tf, line)) {
            if (line.empty() || line[0] == '#') continue;
            Template t; std::istringstream ls(line);
            if (!(ls >> t.n >> t.a1 >> t.phi)) continue;
            templates.push_back(t);
            char buf[64]; snprintf(buf, sizeof buf, "_tmpl%05d", k++);
            prefixes.push_back(outdir + "/" + stem_of(input) + buf);
        }
    }

    bool use_gpu = gpu0;
    std::cerr << "resample_timeseries: " << size << " samples, tsamp " << tsamp << " s, " << templates.size()
              << " template(s), " << mode << ", " << (use_gpu ? "GPU" : "CPU") << std::endl;

    if (use_gpu) {
        float *d_in, *d_out;
        cudaMalloc(&d_in, size * sizeof(float)); cudaMalloc(&d_out, size * sizeof(float));
        cudaMemcpy(d_in, in.data(), size * sizeof(float), cudaMemcpyHostToDevice);
        check_cuda("upload");
        thrust::device_vector<double> d_grid, d_tel, d_target;
        if (exact) { d_grid.resize(size); d_tel.resize(size); d_target.resize(size); }
        for (size_t k = 0; k < templates.size(); ++k) {
            if (exact) gpu_resample_exact(d_in, d_out, size, tsamp, templates[k], d_grid, d_tel, d_target);
            else if (mode == "linear") gpu_resample_linear(d_in, d_out, size, tsamp, templates[k]);
            else gpu_resample_nearest(d_in, d_out, size, tsamp, templates[k]);
            cudaMemcpy(out.data(), d_out, size * sizeof(float), cudaMemcpyDeviceToHost);
            check_cuda("download");
            write_output(prefixes[k], out, inf_text, templates[k], mode);
        }
        cudaFree(d_in); cudaFree(d_out);
    } else {
        for (size_t k = 0; k < templates.size(); ++k) {
            if (exact) cpu_resample_exact(in, out, tsamp, templates[k]);
            else if (mode == "linear") cpu_resample_linear(in, out, tsamp, templates[k]);
            else cpu_resample_nearest(in, out, tsamp, templates[k]);
            write_output(prefixes[k], out, inf_text, templates[k], mode);
        }
    }
    for (size_t k = 0; k < prefixes.size(); ++k) std::cout << prefixes[k] << ".dat" << std::endl;
    return 0;
}
