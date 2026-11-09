#include <iostream>
#include "cuda.h"
#include "cufft.h"
#include <thrust/transform.h>
#include <thrust/iterator/constant_iterator.h>
#include <thrust/transform_reduce.h>
#include <thrust/reduce.h>
#include <thrust/sort.h>
#include <thrust/iterator/zip_iterator.h>
#include <thrust/iterator/counting_iterator.h>
#include <thrust/device_vector.h>
#include <thrust/extrema.h>
#include <thrust/device_ptr.h>
#include <thrust/distance.h>
#include <thrust/copy.h>
#include <thrust/tuple.h>
#include <thrust/fill.h>
#include <kernels/defaults.h>
#include <kernels/kernels.h>
#include <utils/exceptions.hpp>
#include <utils/utils.hpp>
#include <thrust/adjacent_difference.h>
#include <math.h>
#include <thrust/system/cuda/vector.h>
#include <thrust/system/cuda/execution_policy.h>
#include <thrust/functional.h>
#include <map>

#define SQRT2 1.4142135623730951f

//--------------Harmonic summing----------------//

/* Unwrapped for 3x speed increase */
__global__
void harmonic_sum_kernel(float *d_idata, float **d_odata,
			 size_t size, unsigned nharms)

{
  for( int idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
    {
      float val = d_idata[idx];

      if (nharms>0)
	{
      	  val += d_idata[(int) (idx*0.5 + 0.5)];
	  d_odata[0][idx] = val*rsqrt(2.0);
	}

      if (nharms>1)
	{
	  val += d_idata[(int) (idx * 0.75 + 0.5)];
	  val += d_idata[(int) (idx * 0.25 + 0.5)];
	  d_odata[1][idx] = val*0.5;
	}

      if (nharms>2)
	{
	  val += d_idata[(int) (idx * 0.125 + 0.5)];
	  val += d_idata[(int) (idx * 0.375 + 0.5)];
	  val += d_idata[(int) (idx * 0.625 + 0.5)];
	  val += d_idata[(int) (idx * 0.875 + 0.5)];
	  d_odata[2][idx] = val*rsqrt(8.0);
	}

      if (nharms>3)
	{
	  val += d_idata[(int) (idx * 0.0625 + 0.5)];
	  val += d_idata[(int) (idx * 0.1875 + 0.5)];
	  val += d_idata[(int) (idx * 0.3125 + 0.5)];
	  val += d_idata[(int) (idx * 0.4375 + 0.5)];
	  val += d_idata[(int) (idx * 0.5625 + 0.5)];
	  val += d_idata[(int) (idx * 0.6875 + 0.5)];
	  val += d_idata[(int) (idx * 0.8125 + 0.5)];
	  val += d_idata[(int) (idx * 0.9375 + 0.5)];
	  d_odata[3][idx] = val*0.25;
	}

      if (nharms>4)
	{
	  val += d_idata[(int) (idx * 0.03125 + 0.5)];
	  val += d_idata[(int) (idx * 0.09375 + 0.5)];
	  val += d_idata[(int) (idx * 0.15625 + 0.5)];
	  val += d_idata[(int) (idx * 0.21875 + 0.5)];
	  val += d_idata[(int) (idx * 0.28125 + 0.5)];
	  val += d_idata[(int) (idx * 0.34375 + 0.5)];
	  val += d_idata[(int) (idx * 0.40625 + 0.5)];
	  val += d_idata[(int) (idx * 0.46875 + 0.5)];
	  val += d_idata[(int) (idx * 0.53125 + 0.5)];
	  val += d_idata[(int) (idx * 0.59375 + 0.5)];
	  val += d_idata[(int) (idx * 0.65625 + 0.5)];
	  val += d_idata[(int) (idx * 0.71875 + 0.5)];
	  val += d_idata[(int) (idx * 0.78125 + 0.5)];
	  val += d_idata[(int) (idx * 0.84375 + 0.5)];
	  val += d_idata[(int) (idx * 0.90625 + 0.5)];
	  val += d_idata[(int) (idx * 0.96875 + 0.5)];
	  d_odata[4][idx] = val*rsqrt(32.0);
	}
    }
  return;
}

__global__
void harmonic_sum_kernel_single_precision(float *d_idata, float **d_odata,
             size_t size, unsigned nharms)

{
  for( int idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
    {
      float val = d_idata[idx];

      if (nharms>0)
    {
      val += d_idata[__float2int_rn(idx*0.5f)];
      d_odata[0][idx] = val * 0.7071067811865475f;
    }

      if (nharms>1)
    {
      val += d_idata[__float2int_rn(idx * 0.75f)];
      val += d_idata[__float2int_rn(idx * 0.25f)];
      d_odata[1][idx] = val * 0.5f;
    }

      if (nharms>2)
    {
      val += d_idata[__float2int_rn(idx * 0.125f)];
      val += d_idata[__float2int_rn(idx * 0.375f)];
      val += d_idata[__float2int_rn(idx * 0.625f)];
      val += d_idata[__float2int_rn(idx * 0.875f)];
      d_odata[2][idx] = val * 0.35355339059327373f;
    }

      if (nharms>3)
    {
      val += d_idata[__float2int_rn(idx * 0.0625f)];
      val += d_idata[__float2int_rn(idx * 0.1875f)];
      val += d_idata[__float2int_rn(idx * 0.3125f)];
      val += d_idata[__float2int_rn(idx * 0.4375f)];
      val += d_idata[__float2int_rn(idx * 0.5625f)];
      val += d_idata[__float2int_rn(idx * 0.6875f)];
      val += d_idata[__float2int_rn(idx * 0.8125f)];
      val += d_idata[__float2int_rn(idx * 0.9375f)];
      d_odata[3][idx] = val * 0.25f;
    }

      if (nharms>4)
    {
      val += d_idata[__float2int_rn(idx * 0.03125f)];
      val += d_idata[__float2int_rn(idx * 0.09375f)];
      val += d_idata[__float2int_rn(idx * 0.15625f)];
      val += d_idata[__float2int_rn(idx * 0.21875f)];
      val += d_idata[__float2int_rn(idx * 0.28125f)];
      val += d_idata[__float2int_rn(idx * 0.34375f)];
      val += d_idata[__float2int_rn(idx * 0.40625f)];
      val += d_idata[__float2int_rn(idx * 0.46875f)];
      val += d_idata[__float2int_rn(idx * 0.53125f)];
      val += d_idata[__float2int_rn(idx * 0.59375f)];
      val += d_idata[__float2int_rn(idx * 0.65625f)];
      val += d_idata[__float2int_rn(idx * 0.71875f)];
      val += d_idata[__float2int_rn(idx * 0.78125f)];
      val += d_idata[__float2int_rn(idx * 0.84375f)];
      val += d_idata[__float2int_rn(idx * 0.90625f)];
      val += d_idata[__float2int_rn(idx * 0.96875f)];
      d_odata[4][idx] = val * 0.17677669529663687f;
    }
    }
  return;
}

/*
__global__
void harmonic_sum_kernel_wshared(float *d_idata, float **d_odata,
                         size_t size, unsigned nharms)

{

  __shared__ float buffer [sizeof(float)*512];


  for( int idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
    {
      float val = d_idata[idx];

      int thread_by_fold;
      int blockdim_by_fold;

      if (nharms>0)
        {

	  thread_by_fold = threadIdx.x/2;
	  if (threadIdx.x % 2 == 0)
	    {
	      buffer[thread_by_fold] = d_idata[(int) (idx*0.5)];
	    }
	  //__syncthreads();

	  val += buffer[thread_by_fold];
          d_odata[0][idx] = val*rsqrt(2.0);
        }

      if (nharms>1)
        {
	  thread_by_fold = threadIdx.x/4;
	  blockdim_by_fold = blockDim.x/4;
	  if (threadIdx.x % 4 == 0)
            {
              buffer[thread_by_fold]                    = d_idata[(int) (idx*0.75)];
	      buffer[thread_by_fold + blockdim_by_fold] = d_idata[(int) (idx*0.25)];
            }
          //__syncthreads();
	  val += buffer[thread_by_fold];
	  val += buffer[thread_by_fold + blockdim_by_fold];
	  d_odata[1][idx] = val*0.5;
        }

      if (nharms>2)
        {
	  thread_by_fold = threadIdx.x/8;
          blockdim_by_fold = blockDim.x/8;
	  if (threadIdx.x % 8 == 0)
            {
              buffer[thread_by_fold]                     = d_idata[(int) (idx*0.125)];
              buffer[thread_by_fold+ blockdim_by_fold]   = d_idata[(int) (idx*0.375)];
	      buffer[thread_by_fold+ 2*blockdim_by_fold] = d_idata[(int) (idx*0.625)];
	      buffer[thread_by_fold+ 3*blockdim_by_fold] = d_idata[(int) (idx*0.875)];
            }
          //__syncthreads();

	  val += buffer[thread_by_fold];
	  val += buffer[thread_by_fold + blockdim_by_fold];
	  val += buffer[thread_by_fold + 2*blockdim_by_fold];
	  val += buffer[thread_by_fold + 3*blockdim_by_fold];
          d_odata[2][idx] = val*rsqrt(8.0);
        }

      if (nharms>3)
        {
	  thread_by_fold = threadIdx.x/16;
          blockdim_by_fold = blockDim.x/16;
          if (threadIdx.x % 16 == 0)
            {
              buffer[thread_by_fold]                     = d_idata[(int) (idx*0.0625)];
              buffer[thread_by_fold+ blockdim_by_fold]   = d_idata[(int) (idx*0.1875)];
              buffer[thread_by_fold+ 2*blockdim_by_fold] = d_idata[(int) (idx*0.3125)];
              buffer[thread_by_fold+ 3*blockdim_by_fold] = d_idata[(int) (idx*0.4375)];
	      buffer[thread_by_fold+ 4*blockdim_by_fold] = d_idata[(int) (idx*0.5625)];
	      buffer[thread_by_fold+ 5*blockdim_by_fold] = d_idata[(int) (idx*0.6875)];
	      buffer[thread_by_fold+ 6*blockdim_by_fold] = d_idata[(int) (idx*0.8125)];
	      buffer[thread_by_fold+ 7*blockdim_by_fold] = d_idata[(int) (idx*0.9375)];
            }
          //__syncthreads();

	  val += buffer[thread_by_fold];
	  val += buffer[thread_by_fold+ blockdim_by_fold];
	  val += buffer[thread_by_fold+ 2*blockdim_by_fold];
	  val += buffer[thread_by_fold+ 3*blockdim_by_fold];
	  val += buffer[thread_by_fold+ 4*blockdim_by_fold];
	  val += buffer[thread_by_fold+ 5*blockdim_by_fold];
	  val += buffer[thread_by_fold+ 6*blockdim_by_fold];
	  val += buffer[thread_by_fold+ 7*blockdim_by_fold];

          d_odata[3][idx] = val*0.25;
        }
    }
  return;
  }*/

void device_harmonic_sum(float* d_input_array, float** d_output_array,
			 size_t size, unsigned nharms, bool single_precision,
			 unsigned int max_blocks, unsigned int max_threads)
{
  unsigned blocks = size/max_threads + 1;
  if (blocks > max_blocks)
    blocks = max_blocks;

    if (single_precision) {
    // Use single precision kernel for harmonic sum
    harmonic_sum_kernel_single_precision<<<blocks, max_threads>>>(
        d_input_array, d_output_array, size, nharms);
    }else {
    harmonic_sum_kernel<<<blocks,max_threads>>>(d_input_array,d_output_array,size,nharms);
    }
  ErrorChecker::check_cuda_error("Error from device_harmonic_sum");
}

//------------spectrum forming--------------//


//Could be optimised with shared memory

__global__
void power_series_kernel(cufftComplex *d_idata, float* d_odata,
			 size_t size, size_t gulp_index)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_index;
  cufftComplex& x = d_idata[idx];
  if(idx<size)
    {
      float z = x.x*x.x+x.y*x.y;
      if (z==0) {
        //printf("zero at %d\n", idx);
        d_odata[idx] = 0;
      }
      else{
        d_odata[idx] = z*rsqrtf(z);
      }
    }
  return;
}

//Could be optimised with shared memory

__global__ void bin_interbin_series_kernel(cufftComplex *d_idata,float* d_odata,
					   size_t size, size_t gulp_index)
{
  float* d_idata_float = (float*)d_idata;
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_index;
  float re_l =0.0;
  float im_l =0.0;
  if (idx>0 && idx<size) {
    re_l = d_idata_float[2*idx-2];
    im_l = d_idata_float[2*idx-1];
  }
  if(idx<size)
    {
      float re = d_idata_float[2*idx];
      float im = d_idata_float[2*idx+1];
      float ampsq = re*re+im*im;
      float ampsq_diff = 0.5*((re-re_l)*(re-re_l) +
                              (im-im_l)*(im-im_l));
      d_odata[idx] = sqrtf(fmaxf(ampsq,ampsq_diff));
    }
  return;
}

 /*
__global__ void bin_interbin_series_kernel(cufftComplex *d_idata,float* d_odata, int size)
{
  int idx = blockIdx.x * (blockDim.x-1) + threadIdx.x;

  if (idx>=size-1)
    return;

  extern __shared__ cufftComplex s[];

  //blockIdx accounts for backshift by 1 sample to keep single write coalesence

  if (idx!=0)
    s[threadIdx.x] = d_idata[idx-1];
  else
    s[threadIdx.x] = make_cuComplex(0.0,0.0);
  __syncthreads();

  if (threadIdx.x+1 == blockDim.x)
    return;

  cufftComplex x = s[threadIdx.x+1];
  cufftComplex y = s[threadIdx.x];
  float ampsq = x.x*x.x+x.y*x.y;
  float ampsq_diff = 0.5*((x.x-y.x)*(x.x-y.x) +
			  (x.y-y.y)*(x.y-y.y));
  float val = max(ampsq,ampsq_diff);
  d_odata[idx] = val*rsqrtf(val);

  return;
}
 */

void device_form_power_series(cufftComplex* d_array_in,
			      float* d_array_out,
			      size_t size, int way,
			      unsigned int max_blocks,
			      unsigned int max_threads)
{
  BlockCalculator calc(size,max_blocks,max_threads);
  for (int ii=0;ii<calc.size();ii++){
    if (way == 1)
      bin_interbin_series_kernel<<<calc[ii].blocks,max_threads>>>
        (d_array_in, d_array_out, size, calc[ii].data_idx);
    else
      power_series_kernel<<<calc[ii].blocks,max_threads>>>
	(d_array_in, d_array_out, size, calc[ii].data_idx);
  }
  ErrorChecker::check_cuda_error("Error from device_form_power_series");
  return;
}

//-----------------time domain resampling---------------//

inline __device__ unsigned long getAcceleratedIndex(double accel_fact, double size_by_2,
						    unsigned long id){
  return __double2ull_rn(id + accel_fact*( ((id-size_by_2)*(id-size_by_2)) - (size_by_2*size_by_2)));
}


inline __device__ unsigned long getAcceleratedIndexII(double accel_fact, double size,
						      unsigned long id){
  return __double2ull_rn(id + id*accel_fact*(id-size));
}


__global__ void resample_kernel(float* input_d,
				float* output_d,
				double accel_fact,
				size_t size,
				double size_by_2,
				size_t start_idx)
{
  unsigned long idx = threadIdx.x + blockIdx.x * blockDim.x + start_idx;
  if (idx>=size)
    return;
  unsigned long idx_read = getAcceleratedIndex(accel_fact,size_by_2,idx);
  output_d[idx] = input_d[idx_read];
}


__global__ void resample_kernelII(float* input_d,
				  float* output_d,
				  double accel_fact,
				  double size)

{
  for( unsigned long idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
  {
    unsigned long out_idx = getAcceleratedIndexII(accel_fact,size,idx);
    output_d[idx] = input_d[out_idx];
  }
}

void device_resampleII(float * d_idata, float * d_odata,
                     size_t size, float a,
                     float tsamp, unsigned int max_threads,
                     unsigned int max_blocks)
{

  double accel_fact = ((a*tsamp) / (2 * 299792458.0));
  unsigned blocks = size/max_threads + 1;
  if (blocks > max_blocks)
    blocks = max_blocks;
  resample_kernelII<<< blocks,max_threads >>>(d_idata, d_odata,
					      accel_fact,
					      (double) size);
  ErrorChecker::check_cuda_error("Error from device_resampleII");
}

void device_resample(float * d_idata, float * d_odata,
		     size_t size, float a,
		     float tsamp, unsigned int max_threads,
		     unsigned int max_blocks)
{
  double accel_fact = ((a*tsamp) / (2 * 299792458.0));
  double size_by_2  = (double)size/2.0;
  BlockCalculator calc(size,max_blocks,max_threads);
  for (int ii=0;ii<calc.size();ii++)
    resample_kernel<<< calc[ii].blocks,max_threads >>>(d_idata, d_odata,
						       accel_fact,
						       size,
						       size_by_2,
						       calc[ii].data_idx);
  ErrorChecker::check_cuda_error("Error from device_resample");
}

//------------------Circular Orbit Search Template Bank Resampler-----------------//

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
void device_circular_orbit_resampler(float * d_idata, float * d_odata, double n, double a1, double phi, double tsamp, double inverse_tsamp,  size_t size, unsigned int max_threads, unsigned int max_blocks)
{
 
    /* Forcing out[0] = in[0] rotates the recovered orbital phase by n*R(0)
       and shifts the reported T0 by ~R(0). Without the offset,
       out[j] = in[j + R(j)] resamples exactly to the emission frame. */
  double zero_offset = 0.0;

  unsigned blocks = size/max_threads + 1;
  if (blocks > max_blocks)
    blocks = max_blocks;
  resample_circular_kernel<<< blocks,max_threads >>>(d_idata, d_odata, n,
  a1, phi, zero_offset, tsamp, inverse_tsamp, (double) size);
  ErrorChecker::check_cuda_error("Error from device_circular_orbit_resampler");
}

//--------------------Elliptical Orbit Search Template Bank Resampler-----------------//

__device__ unsigned long get_ell8_resampler_idx(unsigned long idx, double n, double a1, double phi, 
    double omega, double ecc, double zero_offset,
    double tsamp, double inverse_tsamp,  double c0, double c1, double c2, double c3, double c4, double c5, double c6, double c7, 
    double s1, double s2, double s3, double s4, double s5, double s6, double s7)

{

    double t = idx * tsamp;
    double mean_anomaly = n * t - phi;

    double cosE = c0 + c1 * cos(mean_anomaly) + c2 * cos(2 * mean_anomaly) + c3 * cos(3 * mean_anomaly) + c4 * cos(4 * mean_anomaly) + c5 * cos(5 * mean_anomaly) + c6 * cos(6 * mean_anomaly) + c7 * cos(7 * mean_anomaly);

    //The variable below is called sinE for brevity. It is actually sqrt(1-e**2) * sinE Power expansion
    double sinE = s1 * sin(mean_anomaly) + s2 * sin(2 * mean_anomaly) + s3 * sin(3 * mean_anomaly) + s4 * sin(4 * mean_anomaly) + s5 * sin(5 * mean_anomaly) + s6 * sin(6 * mean_anomaly) + s7 * sin(7 * mean_anomaly);
   
    double bin_offset = a1 * (sin(omega) * cosE + cos(omega) * sinE) * inverse_tsamp  - zero_offset;

 
    return __double2ull_rn(idx + bin_offset);
}

__global__ void ell8_resampler_kernel(float* input_d, float* output_d,
    double n, double a1, double phi, double omega, double ecc, double zero_offset,  
    double tsamp, double inverse_tsamp, double c0, double c1, double c2, double c3, double c4, double c5, double c6, double c7, 
    double s1, double s2, double s3, double s4, double s5, double s6, double s7, double size)


{
  for( unsigned long idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
  {

    unsigned long out_idx = get_ell8_resampler_idx(idx, n, a1, phi, omega, ecc, zero_offset,
    tsamp, inverse_tsamp, c0, c1, c2, c3, c4, c5, c6, c7, s1, s2, s3, s4, s5, s6, s7);

    if (out_idx <= size - 1){
        output_d[idx] = input_d[out_idx];
    }
   /* Zero padding if resampled data needs a bin above fft_size */
    else{
        output_d[idx] = 0.0;
    }
  }
}

/* The fn. below is an implementation of the fast ell8 5-D resampler!*/

void device_ell8_resampler(float * d_idata, float * d_odata, double n, double a1, double phi, double omega, double ecc, double tsamp, double inverse_tsamp, size_t size, unsigned int max_threads, unsigned int max_blocks)
{
    /* These values were taken from Dhurandhar et al. 2000. Original derivation can be found in 
     L. G. Taff, Celestial Mechanics (John Wiley and Sons, Inc., New York 1985), pp. 58-61.
     Here we expand the taylor series to 7th order which should cover upto ecc 0.8. If your system is more eccentric, consider
     adding higher order terms. Future versions of the software will expand the order depending on the ecc' */
   
   /* Original expression is given below. To make the code faster, we will get rid of the division terms. 
    c0 = -0.5 * ecc
    c1 = 1 - (3/8) * ecc**2 + (5/192) * ecc**4 - (7/9216) * ecc**6
    c2 = 0.5 * ecc - (1/3) * ecc**3 + (1/16) * ecc**5
    c3 = (3/8) * ecc**2 - (45/128) * ecc**4 + (567/5120) * ecc**6
    c4 = (1/3) * ecc**3 - (2/5) * ecc**5
    c5 = (125/384) * ecc**4 - (4375/9216) * ecc**6
    c6 = (27/80) * ecc**5
    c7 = (16807/46080) * ecc**6

    s1 = 1 - (5/8) * ecc**2 - (11/192) * ecc**4 - (457/9216) * ecc**6
    s2 = (1/2) * ecc - (5/12) * ecc**3 + (1/24) * ecc**5
    s3 = (3/8) * ecc**2 - (51/128) * ecc**4 + (543/5120) * ecc**6
    s4 = (1/3) * ecc**3 - (13/30) * ecc**5
    s5 = (125/384) * ecc**4 - (4625/9216) * ecc**6
    s6 = (27/80) * ecc**5
    s7 = (16807/46080) * ecc**6 */

                                          
    double c0 = -0.5 * ecc;
    double c1 = 1 - 0.375 * pow(ecc, 2) + 0.02604166666 * pow(ecc, 4) - 0.00075954861 * pow(ecc, 6);
    double c2 = 0.5 * ecc - 0.33333333 * pow(ecc, 3) + 0.0625 * pow(ecc, 5);
    double c3 = 0.375 * pow(ecc, 2) - 0.3515625 * pow(ecc, 4) + 0.1107421875 * pow(ecc, 6);
    double c4 = 0.33333333 * pow(ecc, 3) - 0.4 * pow(ecc, 5);
    double c5 = 0.32552083333 * pow(ecc, 4) - 0.47471788194 * pow(ecc, 6);
    double c6 = 0.3375 * pow(ecc, 5);
    double c7 = 0.36473524305 * pow(ecc, 6);

    double s1 = 1 - 0.625 * pow(ecc, 2) - 0.05729166666 * pow(ecc, 4) - 0.04958767361 * pow(ecc, 6);
    double s2 = 0.5 * ecc - 0.41666666666 * pow(ecc, 3) + 0.04166666666 * pow(ecc, 5);
    double s3 = 0.375 * pow(ecc, 2) - 0.3984375 * pow(ecc, 4) + 0.1060546875 * pow(ecc, 6);
    double s4 = 0.33333333 * pow(ecc, 3) - 0.43333333333 * pow(ecc, 5);
    double s5 = 0.32552083333 * pow(ecc, 4) - 0.50184461805 * pow(ecc, 6);
    double s6 = 0.3375 * pow(ecc, 5);
    double s7 = 0.36473524305 * pow(ecc, 6);

     /* At t = 0, the n * t term vanishes */
    /* cos(-phi) = cos(phi) and sin(-phi) = -sin(phi), so all sinE gets terms get multiplied by -1. */
    double cosE = c0 + c1 * cos(phi) + c2 * cos(2 * phi) + c3 * cos(3 * phi) + c4 * cos(4 * phi) + c5 * cos(5 * phi) + c6 * cos(6 * phi) + c7 * cos(7 * phi);

    //variable name is sinE for brevity. Mathematically it is sqrt(1-e**2) * sin(E).
    double sinE = s1 * -1 * sin(phi) + s2 * -1 * sin(2 * phi) + s3 * -1 * sin(3 * phi) + s4 * -1 * sin(4 * phi) + s5 * -1 * sin(5 * phi) + s6 * -1 * sin(6 * phi) + s7 * -1 * sin(7 * phi);

    /* Forcing out[0] = in[0] rotates the recovered orbital phase by n*R(0)
       and shifts the reported T0 by ~R(0). Without the offset,
       out[j] = in[j + R(j)] resamples exactly to the emission frame. */
    double zero_offset = 0.0;
    unsigned blocks = size/max_threads + 1;
  
    if (blocks > max_blocks)
        blocks = max_blocks;
        
        ell8_resampler_kernel<<< blocks,max_threads >>>(d_idata, d_odata,
                         n, a1, phi, omega, ecc, zero_offset, tsamp, inverse_tsamp, 
                         c0, c1, c2, c3, c4, c5, c6, c7, s1, s2, s3, s4, s5, s6, s7, (double) size);

    ErrorChecker::check_cuda_error("Error from device_ell8_resampler");
}

//-----Elliptical Orbit Exact Resampler-----//

__device__ double get_bt_model_resampler_idx(unsigned long idx, double n, double a1, 
    double phi, double omega, double ecc, double tsamp, double inverse_tsamp, double zero_offset)

{

double t = idx * tsamp;
double mean_anomaly = n * t - phi;
double eccentric_anomaly = mean_anomaly + ecc * sin(mean_anomaly) * (1. + ecc * cos(mean_anomaly));

//Computing eccentric anomaly by iterating kepler's equation
// initializing to large value
double du = 1.;
while(abs(du) > 1.0e-13)
{
    du = (mean_anomaly - (eccentric_anomaly - ecc * sin(eccentric_anomaly)))/(1.0 - ecc * cos(eccentric_anomaly));
    eccentric_anomaly+= du;
}

double roemer_delay = a1  * ((cos(eccentric_anomaly) - ecc) * sin(omega) + sqrt(1 - pow(ecc,2)) * sin(eccentric_anomaly) * cos(omega)) * inverse_tsamp;

double roemer_delay_final = roemer_delay - zero_offset;

return __double2ull_rn(idx + roemer_delay_final);
}


__global__ void bt_model_resampler_kernel(float* d_idata, 
    float* d_odata,
    double n, double a1, double phi, double omega, double ecc,
    double tsamp, double inverse_tsamp, double size, double zero_offset)

{
for( unsigned long idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
{
unsigned long out_idx = get_bt_model_resampler_idx(idx, n, a1, phi, omega, ecc, tsamp, inverse_tsamp, zero_offset);

    if (out_idx < size)
{
    d_odata[idx] = d_idata[out_idx];

}
else
{
    d_odata[idx] = 0.0; // Zero padding if resampled data needs a bin above fft_size

}

}
}

void device_bt_model_resampler(float* d_idata, float* d_odata,
    double n, double a1, double phi, double omega, double ecc, 
    double tsamp, double inverse_tsamp, unsigned int size, unsigned int max_threads, unsigned int max_blocks)
{

//Calculating the zero offset
double mean_anomaly_t0 = -1 * phi;
double eccentric_anomaly_t0 = mean_anomaly_t0 + ecc * sin(mean_anomaly_t0) * (1. + ecc * cos(mean_anomaly_t0));
//Computing eccentric anomaly at t=0 by iterating kepler's equation
double du_t0 = 1.;
while(abs(du_t0) > 1.0e-8)
{
    du_t0 = (mean_anomaly_t0 - (eccentric_anomaly_t0 - ecc * sin(eccentric_anomaly_t0)))/(1.0 - ecc * cos(eccentric_anomaly_t0));
    eccentric_anomaly_t0+= du_t0;
}
/* Forcing out[0] = in[0] rotates the recovered orbital phase by n*R(0)
   and shifts the reported T0 by ~R(0). Without the offset,
   out[j] = in[j + R(j)] resamples exactly to the emission frame. */
double zero_offset = 0.0;

 unsigned blocks = size/max_threads + 1;
 if (blocks > max_blocks)
   blocks = max_blocks;

 bt_model_resampler_kernel<<< blocks,max_threads >>>(d_idata, d_odata, n, a1, phi, omega, ecc, tsamp, inverse_tsamp, (double) size, zero_offset);

 ErrorChecker::check_cuda_error("Error from device_bt_model_resampler");
}

//---Interpolator resampler---------------------//

//Evaluates the BT-model roemer delay R(t) at a single time t
__device__ double evaluate_roemer_delay_bt_model_elliptical(double t, double n, double a1,
    double phi, double omega, double ecc)

{

double mean_anomaly = n * t - phi;
double eccentric_anomaly = mean_anomaly + ecc * sin(mean_anomaly) * (1. + ecc * cos(mean_anomaly));

//Computing eccentric anomaly by iterating kepler's equation
// initializing to large value
double du = 1.;
while(abs(du) > 1.0e-13)
{
    du = (mean_anomaly - (eccentric_anomaly - ecc * sin(eccentric_anomaly)))/(1.0 - ecc * cos(eccentric_anomaly));
    eccentric_anomaly+= du;
}

double roemer_delay = a1  * ((cos(eccentric_anomaly) - ecc) * sin(omega) + sqrt(1 - pow(ecc,2)) * sin(eccentric_anomaly) * cos(omega));

return roemer_delay;
}

__device__ double get_roemer_delay_bt_model_elliptical(unsigned long idx, double n, double a1,
    double phi, double omega, double ecc, double tsamp)

{

double t = idx * tsamp;
/* Same emission-time inversion as the circular case, evaluating the
   full BT-model delay at each iteration. */
double tau = t;
for (int iteration = 0; iteration < 5; iteration++)
{
    tau = t - evaluate_roemer_delay_bt_model_elliptical(tau, n, a1, phi, omega, ecc);
}

return t - tau;
}

__global__ void subtract_roemer_delay_bt_model_elliptical_kernel(double* d_t_binary_grid_ptr, double* d_t_telescope_nonuniform_ptr,
    double n, double a1, double phi, double omega, double ecc,
    double tsamp, double size)

{
for( unsigned long idx = blockIdx.x*blockDim.x + threadIdx.x ; idx < size ; idx += blockDim.x*gridDim.x )
{
    double roemer_delay = get_roemer_delay_bt_model_elliptical(idx, n, a1, phi, omega, ecc, tsamp);
    //Subtracting the roemer delay from the binary grid time to get the telescope non-uniform time
    d_t_telescope_nonuniform_ptr[idx] = d_t_binary_grid_ptr[idx] - roemer_delay; 


}
}

void device_subtract_roemer_delay_elliptical_bt_model(double* d_t_binary_grid_ptr, double* d_t_telescope_nonuniform_ptr,
    double n, double a1, double phi, double omega, double ecc, 
    double tsamp, unsigned int size, unsigned int max_threads, unsigned int max_blocks)
{

 unsigned blocks = size/max_threads + 1;
 if (blocks > max_blocks)
   blocks = max_blocks;

   subtract_roemer_delay_bt_model_elliptical_kernel<<< blocks,max_threads >>>(d_t_binary_grid_ptr, d_t_telescope_nonuniform_ptr, n,
 a1, phi, omega, ecc, tsamp, (double) size);

 ErrorChecker::check_cuda_error("Error from device_subtract_roemer_delay_elliptical_bt_model");
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

void device_subtract_roemer_delay_circular(double* d_t_binary_grid_ptr, double* d_t_telescope_nonuniform_ptr,
    double n, double a1, double phi, double tsamp, unsigned int size, unsigned int max_threads, unsigned int max_blocks)
{

 unsigned blocks = size/max_threads + 1;
 if (blocks > max_blocks)
   blocks = max_blocks;

   subtract_roemer_delay_circular_kernel<<< blocks,max_threads >>>(d_t_binary_grid_ptr, d_t_telescope_nonuniform_ptr, n,
 a1, phi, tsamp, (double) size);

 ErrorChecker::check_cuda_error("Error from device_subtract_roemer_delay_circular");
}


//------------ 1D LERP RESAMPLER----------------//

/* 1. Lerp Algorithm equivalent to np.interp and scipy.interpolate.interp1d in python

Definitions:
xp --> xarray of data --> device_roemer_delay_removed_timeseries
yp --> yarray of data --> input_d
x ----> xarray where we want to evaulate the interpolated values ---> output_samples_array. For our case size == x_len == size
y ----> yarray we want to calculate ---> output_d
size ---> len(xp) == len(yp)
x_size ---> len(x) 

Assume xp is sorted in ascending order.
1. For each value of x, find the segment/interval in xp that contains x. Use a binary search algorithm. Scales as O(logn)
2. Use equation y=mx+b to calculate interpolated value based on the segment chosen from step 1.
*/


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


void device_resample_using_1D_lerp(double *d_t_telescope_nonuniform_ptr, float  *input_d, 
    unsigned long size, double *d_t_binary_target_ptr, float *output_d,
    unsigned int max_threads, unsigned int max_blocks)
{

unsigned blocks = size/max_threads + 1;
if (blocks > max_blocks)
  blocks = max_blocks;
resample_using_1D_lerp_kernel<<< blocks,max_threads >>>(d_t_telescope_nonuniform_ptr, input_d, size, d_t_binary_target_ptr, output_d);

ErrorChecker::check_cuda_error("Error from device_resample_using_1D_lerp");
}
//------------------peak finding-----------------//
//defined here as (although Thrust based) requires CUDA functors

struct greater_than_threshold : thrust::unary_function<thrust::tuple<int,float>,bool>
{
  float threshold;
  __device__ bool operator()(thrust::tuple<int,float> t) { return thrust::get<1>(t) > threshold; }
  greater_than_threshold(float thresh):threshold(thresh){}
};

int device_find_peaks(int n, int start_index, float * d_dat,
		      float thresh, int * indexes, float * snrs,
		      thrust::device_vector<int>& d_index,
		      thrust::device_vector<float>& d_snrs,
		      cached_allocator& policy)
{

  using thrust::tuple;
  using thrust::counting_iterator;
  using thrust::zip_iterator;
  // Wrap the device pointer to let Thrust know
  thrust::device_ptr<float> dptr_dat(d_dat + start_index);
  typedef thrust::device_vector<float>::iterator snr_iterator;
  typedef thrust::device_vector<int>::iterator indices_iterator;
  thrust::counting_iterator<int> iter(start_index);
  zip_iterator<tuple<counting_iterator<int>,thrust::device_ptr<float> > > zipped_iter = make_zip_iterator(make_tuple(iter,dptr_dat));
  zip_iterator<tuple<indices_iterator,snr_iterator> > zipped_out_iter = make_zip_iterator(make_tuple(d_index.begin(),d_snrs.begin()));

  //apply execution policy to get some speed up
  int num_copied = thrust::copy_if(thrust::cuda::par(policy), zipped_iter, zipped_iter+n-start_index,
				   zipped_out_iter,greater_than_threshold(thresh)) - zipped_out_iter;
  thrust::copy(d_index.begin(),d_index.begin()+num_copied,indexes);
  thrust::copy(d_snrs.begin(),d_snrs.begin()+num_copied,snrs);
  ErrorChecker::check_cuda_error("Error from device_find_peaks;");
  return(num_copied);
}

//------------------rednoise----------------//

template<typename T>
struct square {
    __host__ __device__ inline
    T operator()(const T& x) { return x*x; }
};

template<typename T>
float GPU_rms(T* d_collection,int nsamps, int min_bin)
{
  T rms_sum;
  float rms;

  using thrust::device_ptr;
  rms_sum = thrust::transform_reduce(device_ptr<T>(d_collection)+min_bin,
				     device_ptr<T>(d_collection)+nsamps,
				     square<T>(),T(0),thrust::plus<T>());
  rms = sqrt(float(rms_sum)/float(nsamps-min_bin));
  return rms;
}

template<typename T>
float GPU_mean(T* d_collection,int nsamps, int min_bin)
{
  float mean;
  T m_sum;

  using thrust::device_ptr;
  m_sum = thrust::reduce(device_ptr<T>(d_collection)+min_bin,
			 device_ptr<T>(d_collection)+nsamps);

  cudaDeviceSynchronize();
  mean = float(m_sum)/float(nsamps-min_bin);

  return mean;
}

template<typename T>
void GPU_remove_baseline(T* d_collection, int nsamps){
  float mean  = 0.0;

  int count = 0;
  do{
    mean = GPU_mean(d_collection, nsamps, 0);

    thrust::for_each(thrust::device_ptr<T>(d_collection),
        thrust::device_ptr<T>(d_collection)+nsamps,thrust::placeholders::_1 -= mean);


    if(count++ > 100) break;

  }while(abs(mean) > 5e-7 * nsamps);

}


template<typename T>
void GPU_fill(T* start, T* end, T value){
  thrust::device_ptr<T> ar_start(start);
  thrust::device_ptr<T> ar_end(end);
  thrust::fill(ar_start,ar_end,value);
  ErrorChecker::check_cuda_error("Error in GPU_fill");
}

template void GPU_fill<float>(float*, float*, float);
template float GPU_rms<float>(float*,int,int);
template float GPU_mean<float>(float*,int,int);

template void GPU_remove_baseline<float>(float*,int);

__global__
void normalisation_kernel(float*d_powers, float mean, float sigma,
			  size_t size, size_t gulp_idx)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_idx;
  if (idx>=size)
    return;
  float val = d_powers[idx];
  val-=mean;
  val/=sigma;
  d_powers[idx] = val;
}

void device_normalise(float* d_powers,
		      float mean,
		      float sigma,
		      unsigned int size,
		      unsigned int max_blocks,
		      unsigned int max_threads)
{
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++)
    normalisation_kernel<<<calc[ii].blocks,max_threads>>>(d_powers,mean,sigma,size,
							  calc[ii].data_idx);
  ErrorChecker::check_cuda_error("Error from device_normalise");
}


//old normalisation routine used after a different
//rednoise algorithm was applied
void device_normalise_spectrum(int nsamp,
			       float* d_power_spectrum,
			       float* d_normalised_power_spectrum,
			       int min_bin,
			       float * sigma)
{
  float mean;
  float rms;
  float meansquares;

  if (*sigma==0.0) {
    mean = GPU_mean(d_power_spectrum,nsamp,min_bin);
    rms = GPU_rms(d_power_spectrum,nsamp,min_bin);
    meansquares = rms*rms;
    *sigma = sqrt(meansquares - (mean*mean));
  }

  thrust::transform(thrust::device_ptr<float>(d_power_spectrum),
                    thrust::device_ptr<float>(d_power_spectrum)+nsamp,
                    thrust::make_constant_iterator(*sigma),
                    thrust::device_ptr<float>(d_normalised_power_spectrum),
                    thrust::divides<float>());
  ErrorChecker::check_cuda_error("Error from device_normalise_spectrum");
}


//--------------Time series folder----------------//
/*
__global__
void fold_filterbank_kernel(float* input, float* output, unsigned* count,
			    unsigned nchans, float tsamp_by_period,
			    double accel_fact, unsigned nbins,
			    float nrots_per_subint, unsigned nsamps,
			    unsigned offset)
{
  extern __shared__ peasoup_fold_plan plan [];

  unsigned first_samp;
  unsigned samp;
  float rotation;
  float int_part;
  float frac_part;
  unsigned in_idx_partial,in_idx;
  unsigned out_idx_partial,out_idx;

  //Start in time domain and calculate output
  //phasebin an subint for each sample in the block

  first_samp = blockIdx.x*blockDim.x + offset;
  samp = first_samp + threadIdx.x;
  rotation = (samp + samp*accel_fact*(samp-nsamps))*tsamp_by_period;
  frac_part = modf(rotation,&int_part);
  plan[threadIdx.x].subint = __float2uint_rd(rotation/nrots_per_subint);
  plan[threadIdx.x].phasebin = __float2uint_rd(frac_part*nbins);

  //Sync and move to channel domain to preserve
  //memory bandwidth

  __sync_threads();

  for (jj=0; jj<blockDim.x; jj++)
    {
      in_idx_partial = (jj+first_samp)*nchans;

      //These are shared memory broadcasts
      out_idx_partial = nbins*nchans*plan[jj].subint + nchan*plan[jj].bin;

      for (ii=threadIdx.x; ii<nchans; ii+=blockDim.x)
	{
	  in_idx = in_idx_partial+ii;
	  out_idx = out_idx_partial+ii;
	  output[out_idx] += input[in_idx];
	  count[out_idx]++;
	}
    }
}


int device_fold_filterbank(float* input, float* output, unsigned* count,
			   float tsamp, float period, float acceleration,
			   unsigned nsubints, unsigned nbins, unsigned nchans,
			   unsigned total_nsamps, unsigned nsamps, unsigned offset,
			   unsigned max_blocks, unsigned max_threads)
{

  float tobs = total_nsamps*tsamp;
  float nrots = tobs/period;
  float nrots_per_subint = nrots/nsubints;
  float tsamp_by_period = tsamp_by_period;
  double accel_fact = ((acceleration * tsamp) / (2 * 299792458.0));
  unsigned mem_size_bytes = nsamps*sizeof(peasoup_fold_plan);
  fold_filterbank_kernel<<<max_blocks,max_threads,mem_size_bytes>>>
    (input, output, count, nchans, tsamp_by_period, accel_fact, nbins,
     nrots_per_subint, nsamps, offset);

     }*/


__global__
void fold_time_series_kernel(float* input, float* output,
			     size_t nsubints,
			     size_t nbins, size_t nsamps_per_subint,
			     double tsamp_by_period)
{
  extern __shared__ float block [];
  float* soutput = (float*) &block[0];
  int* count = (int*) &block[nbins];

  //one block per subint
  size_t data_idx = nsamps_per_subint*blockIdx.x + threadIdx.x;
  size_t ii,jj;
  int idx;

  if (threadIdx.x>nbins)
    return;

  //zero output shared memory
  for (ii=threadIdx.x; ii<nbins; ii+=blockDim.x){
    soutput[ii] = 0;
    count[ii] = 1;
  }
  //read all data for a subint
  double int_part,float_part;

  for (jj = data_idx; jj < (data_idx + nsamps_per_subint); jj += blockDim.x)
    {
      float_part = modf(jj*tsamp_by_period,&int_part);
      idx = __double2int_rd(float_part * nbins);
      atomicAdd(&soutput[idx], input[jj]);
      atomicAdd(&count[idx], 1);
    }

  for (ii=threadIdx.x; ii<nbins; ii+=blockDim.x)
    output[blockIdx.x * nbins + ii] = soutput[ii]/count[ii];
}

void device_fold_timeseries(float* input, float* output,
			    size_t nsamps, size_t nsubints,
			    double period, double tsamp, int nbins,
			    size_t max_blocks, size_t max_threads)
{
  size_t nsamps_per_subint = nsamps/nsubints;
  double tsamp_by_period = tsamp/period;

  if (nbins*sizeof(float)*2>16384){
    ErrorChecker::throw_error("device_fold_timeseries: nbins must be less than 2048");
    return;
  }

  fold_time_series_kernel<<<nsubints,nbins,2*nbins*nsubints*sizeof(float)>>>
    (input,output,nsubints,nbins,nsamps_per_subint,tsamp_by_period);
  ErrorChecker::check_cuda_error("Error from device_fold_timeseries.");
}

//--------------FoldOptimiser------------//

__device__ inline cuComplex cuCexpf(cuComplex z)
{
  cuComplex res;
  float t = expf(z.x);
  sincosf(z.y, &res.y, &res.x);
  res.x *= t;
  res.y *= t;
  return res;
}

__global__
void shift_array_generator_kernel(cuComplex* shift_ar, unsigned int shift_ar_size,
				  unsigned int nbins, unsigned int nints,
				  unsigned int nshift, float* shifts,
				  float two_pi)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx >= shift_ar_size)
    return;
  float subint = idx/nbins%nints;
  unsigned int shift_idx = idx/(nbins*nints);
  unsigned int bin = idx%nbins;
  float shift = subint/nints * shifts[shift_idx];
  float ramp = bin*two_pi/nbins;
  if (bin>nbins/2)
    ramp-=two_pi;
  cuComplex tmp1 = make_cuComplex(0.0,-1*ramp*shift);
  cuComplex tmp2 = cuCexpf(tmp1);
  shift_ar[idx] = tmp2;
}

__global__
void template_generator_kernel(cuComplex* templates, unsigned int nbins, unsigned int size)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx>=size)
    return;
  unsigned int bin = idx%nbins;
  unsigned int template_idx = idx/nbins;
  float val = (bin<=template_idx);
  templates[idx] = make_cuComplex(val,0.0);
}

__global__
void multiply_by_shift_kernel(cuComplex* input, cuComplex* output,
			      cuComplex* shift_array, unsigned int nbins_by_nints,
			      unsigned int size)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx>=size)
    return;
  unsigned int in_idx = idx%(nbins_by_nints);
  output[idx] = cuCmulf(input[in_idx],shift_array[idx]);
}

__global__
void collapse_subints_kernel(cuComplex* input, cuComplex* output,
			     unsigned int nbins, unsigned int nints,
			     unsigned int nbins_by_nints, unsigned int size)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx>=size)
    return;
  unsigned int bin = idx%nbins;
  unsigned int fold = idx/nbins;
  unsigned int in_idx = (fold*nbins_by_nints)+bin;
  cuComplex val =  make_cuComplex(0.0,0.0);
  for (int ii=0;ii<nints;ii++)
    val = cuCaddf(val,input[in_idx+ii*nbins]);
  output[idx] = val;
}

__global__
void multiply_by_template_kernel(cuComplex* input, cuComplex* output,
				 cuComplex* templates, unsigned int nbins,
				 unsigned int nshifts, unsigned int nbins_by_nshifts,
				 unsigned int size, unsigned int step)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx>=size)
    return;
  unsigned int template_idx = idx/nbins_by_nshifts;
  unsigned int bin = idx%nbins;
  unsigned int shift = idx%nbins_by_nshifts;
  float width = (template_idx+1.0);
  cuComplex normalisation_factor = make_cuComplex(sqrtf(width),0.0);
  if (bin==0)
    output[idx] = make_cuComplex(0.0,0.0);
  else
    output[idx] = cuCdivf(cuCmulf(input[shift],templates[template_idx*nbins+bin]),normalisation_factor);
}

__global__
void cuCabsf_kernel(cuComplex* input, float* output, unsigned int size)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx>=size)
    return;
  output[idx] = cuCabsf(input[idx]);
}

__global__
void real_to_complex_kernel(float* input, cuComplex* output, unsigned int size)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx>=size)
    return;
  output[idx] = make_cuComplex(input[idx],0.0);
}

unsigned int device_argmax(float* input, unsigned int size)
{
  thrust::device_ptr<float> ptr(input);
  thrust::device_ptr<float> max_elem = thrust::max_element(ptr,ptr+size);
  ErrorChecker::check_cuda_error("Error from thrust::max_element in device_argmax");
  return thrust::distance(ptr,max_elem);
}

void device_real_to_complex(float* input, cuComplex* output, unsigned int size,
			    unsigned int max_blocks, unsigned int max_threads)
{
  BlockCalculator calc(size,max_blocks,max_threads);
  for (int ii=0;ii<calc.size();ii++)
    real_to_complex_kernel<<<calc[ii].blocks,max_threads>>>(input,output,size);
  ErrorChecker::check_cuda_error("Error from device_real_to_complex");
  return;
}


void device_get_absolute_value(cuComplex* input, float* output, unsigned int size,
			       unsigned int max_blocks, unsigned int max_threads)
{
  BlockCalculator calc(size,max_blocks,max_threads);
  for (int ii=0;ii<calc.size();ii++)
    cuCabsf_kernel<<<calc[ii].blocks,max_threads>>>(input,output,size);
  ErrorChecker::check_cuda_error("Error from device_get_absolute_value");
  return;
}

void device_generate_shift_array(cuComplex* shifted_ar,
                                 unsigned int shifted_ar_size,
                                 unsigned int nbins, unsigned int nints,
                                 unsigned int nshift, float* shifts,
                                 unsigned int max_blocks, unsigned int max_threads)
{
  float two_pi = 2*3.14159265359;
  BlockCalculator calc(shifted_ar_size,max_blocks,max_threads);
  for (int ii=0;ii<calc.size();ii++)
    shift_array_generator_kernel<<<calc[ii].blocks,max_threads>>>(shifted_ar, shifted_ar_size, nbins,
								  nints, nshift, shifts, two_pi);
  ErrorChecker::check_cuda_error("Error from device_generate_shift_array");
  return;
}

void device_generate_template_array(cuComplex* templates, unsigned int nbins,
				    unsigned int size, unsigned int max_blocks,
				    unsigned int max_threads)
{
  BlockCalculator calc(size,max_blocks,max_threads);
  for (int ii=0;ii<calc.size();ii++){
    template_generator_kernel<<<calc[ii].blocks,max_threads>>>(templates, nbins, size);
  }
  ErrorChecker::check_cuda_error("Error from device_generate_template_array");
  return;
}

void device_multiply_by_shift(cuComplex* input, cuComplex* output,
                              cuComplex* shift_array, unsigned int size,
			      unsigned int nbins_by_nints,
			      unsigned int max_blocks, unsigned int max_threads)
{
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++){
    multiply_by_shift_kernel<<<calc[ii].blocks,max_threads>>>(input,output,shift_array,
							      nbins_by_nints,size);
  }
  ErrorChecker::check_cuda_error("Error from device_multiply_by_shift");
  return;
}

void device_collapse_subints(cuComplex* input, cuComplex* output,
			     unsigned int nbins, unsigned int nints,
			     unsigned int size, unsigned int max_blocks,
			     unsigned int max_threads)
{
  unsigned int nbins_by_nints = nbins*nints;
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++){
    collapse_subints_kernel<<<calc[ii].blocks,max_threads>>>(input,output,nbins,
							     nints,nbins_by_nints,size);
  }
  ErrorChecker::check_cuda_error("Error from device_collapse_subints");
  return;
}

void device_multiply_by_templates(cuComplex* input, cuComplex* output,
				  cuComplex* templates, unsigned int nbins,
				  unsigned int nshifts,
				  unsigned int size, unsigned int step,
				  unsigned int max_blocks, unsigned int max_threads)
{
  unsigned int nbins_by_nshifts = nbins*nshifts;
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++){
    multiply_by_template_kernel<<<calc[ii].blocks,max_threads>>>(input,output,templates,
								 nbins,nshifts,nbins_by_nshifts,
								 size,step);
  }
  ErrorChecker::check_cuda_error("Error from device_multiply_by_templates");
  return;
}

//--------------Rednoise stuff--------------//

//Ben Barsdells median scrunching algorithm from Heimdall
/*
  Note: The implementations of median3-5 here can be derived from
          'sorting networks'.
*/

inline __host__ __device__
float median3(float a, float b, float c) {
	return a < b ? b < c ? b
	                      : a < c ? c : a
	             : a < c ? a
	                     : b < c ? c : b;
}
inline __host__ __device__
float median4(float a, float b, float c, float d) {
	return a < c ? b < d ? a < b ? c < d ? 0.5f*(b+c) : 0.5f*(b+d)
	                             : c < d ? 0.5f*(a+c) : 0.5f*(a+d)
	                     : a < d ? c < b ? 0.5f*(d+c) : 0.5f*(b+d)
	                             : c < b ? 0.5f*(a+c) : 0.5f*(a+b)
	             : b < d ? c < b ? a < d ? 0.5f*(b+a) : 0.5f*(b+d)
	                             : a < d ? 0.5f*(a+c) : 0.5f*(c+d)
	                     : c < d ? a < b ? 0.5f*(d+a) : 0.5f*(b+d)
	                             : a < b ? 0.5f*(a+c) : 0.5f*(c+b);
}
inline __host__ __device__
float median5(float a, float b, float c, float d, float e) {
	// Note: This wicked code is by 'DRBlaise' and was found here:
	//         http://stackoverflow.com/a/2117018
	return b < a ? d < c ? b < d ? a < e ? a < d ? e < d ? e : d
                                                 : c < a ? c : a
                                         : e < d ? a < d ? a : d
                                                 : c < e ? c : e
                                 : c < e ? b < c ? a < c ? a : c
                                                 : e < b ? e : b
                                         : b < e ? a < e ? a : e
                                                 : c < b ? c : b
                         : b < c ? a < e ? a < c ? e < c ? e : c
                                                 : d < a ? d : a
                                         : e < c ? a < c ? a : c
                                                 : d < e ? d : e
                                 : d < e ? b < d ? a < d ? a : d
                                                 : e < b ? e : b
                                         : b < e ? a < e ? a : e
                                                 : d < b ? d : b
	         : d < c ? a < d ? b < e ? b < d ? e < d ? e : d
                                                 : c < b ? c : b
                                         : e < d ? b < d ? b : d
                                                 : c < e ? c : e
                                 : c < e ? a < c ? b < c ? b : c
                                                 : e < a ? e : a
                                         : a < e ? b < e ? b : e
                                                 : c < a ? c : a
                         : a < c ? b < e ? b < c ? e < c ? e : c
                                                 : d < b ? d : b
                                         : e < c ? b < c ? b : c
                                                 : d < e ? d : e
                                 : d < e ? a < d ? b < d ? b : d
                                                 : e < a ? e : a
                                         : a < e ? b < e ? b : e
	                                         : d < a ? d : a;
}

struct median_scrunch5_kernel
	: public thrust::unary_function<hd_float,hd_float> {
	const hd_float* in;
	median_scrunch5_kernel(const hd_float* in_)
		: in(in_) {}
	inline __host__ __device__
	hd_float operator()(unsigned int i) const {
		hd_float a = in[5*i+0];
		hd_float b = in[5*i+1];
		hd_float c = in[5*i+2];
		hd_float d = in[5*i+3];
		hd_float e = in[5*i+4];
		return median5(a, b, c, d, e);
	}
};

hd_error median_scrunch5(const hd_float* d_in,
                         hd_size         count,
                         hd_float*       d_out)
{
	thrust::device_ptr<const hd_float> d_in_begin(d_in);
	thrust::device_ptr<hd_float>       d_out_begin(d_out);

	if( count == 1 ) {
		*d_out_begin = d_in_begin[0];
	}
	else if( count == 2 ) {
		*d_out_begin = 0.5f*(d_in_begin[0] + d_in_begin[1]);
	}
	else if( count == 3 ) {
		*d_out_begin = median3(d_in_begin[0],
		                       d_in_begin[1],
		                       d_in_begin[2]);
	}
	else if( count == 4 ) {
		*d_out_begin = median4(d_in_begin[0],
		                       d_in_begin[1],
		                       d_in_begin[2],
		                       d_in_begin[3]);
	}
	else {
		// Note: Truncating here is necessary
		hd_size out_count = count / 5;
		using thrust::make_counting_iterator;
		thrust::transform(make_counting_iterator<unsigned int>(0),
		                  make_counting_iterator<unsigned int>(out_count),
		                  d_out_begin,
		                  median_scrunch5_kernel(d_in));
	}
	return HD_NO_ERROR;
}

struct linear_stretch_functor
	: public thrust::unary_function<hd_float,hd_float> {
	const hd_float* in;
	hd_float        step;
	linear_stretch_functor(const hd_float* in_,
	                       hd_size in_count, hd_size out_count)
		: in(in_), step(hd_float(in_count-1)/(out_count-1)) {}
	inline __host__ __device__
	hd_float operator()(unsigned int i) const {
		hd_float     x = i * step;
		unsigned int j = x;
		return in[j] + ((x-j > 1e-5f) ? (x-j)*(in[j+1]-in[j]) : 0.f);
	}
};

hd_error linear_stretch(const hd_float* d_in,
                        hd_size         in_count,
                        hd_float*       d_out,
                        hd_size         out_count)
{
	using thrust::make_counting_iterator;
	thrust::device_ptr<hd_float> d_out_begin(d_out);

	thrust::transform(make_counting_iterator<unsigned int>(0),
	                  make_counting_iterator<unsigned int>(out_count),
	                  d_out_begin,
	                  linear_stretch_functor(d_in, in_count, out_count));
	return HD_NO_ERROR;
}

__global__
void divide_c_by_f_kernel(cuComplex* c, float* f, unsigned int size, unsigned int gulp_idx)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_idx;
  if (idx>=size)
    return;
  if (idx<5)
    c[idx] = make_cuComplex(0.0,0.0);
  else
    c[idx] = cuCdivf(c[idx],make_cuComplex(f[idx],0.0));
}

__global__
void divide_f_by_f_kernel(float* a, float* b, unsigned int size, unsigned int gulp_idx)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_idx;
  if (idx>=size)
    return;
  if (idx<5)
    a[idx] = 0.0f;
  else
    a[idx] = a[idx] / b[idx];
}

/* a[i] /= b[i]; used to whiten a power spectrum with its running-median envelope (first 5 bins zeroed
   like divide_c_by_f_kernel). */
void device_divide_f_by_f(float* a, float* b, unsigned int size,
			    unsigned int max_blocks, unsigned int max_threads)
{
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++){
    divide_f_by_f_kernel<<<calc[ii].blocks,max_threads>>>(a,b,size,ii*max_threads*max_blocks);
  }
  ErrorChecker::check_cuda_error();
  return;
}

void device_divide_c_by_f(cuComplex* c, float* f, unsigned int size,
			    unsigned int max_blocks, unsigned int max_threads)
{
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++){
    divide_c_by_f_kernel<<<calc[ii].blocks,max_threads>>>(c,f,size,ii*max_threads*max_blocks);
  }
  ErrorChecker::check_cuda_error();
  return;
}

__global__
void zap_birdies_kernel(cuComplex* fseries, float* birdies, float* widths,
			float bin_width, unsigned int size,
			unsigned int fseries_size)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  if (idx>=size)
    return;
  int ii;
  float freq = birdies[idx];
  float width = widths[idx];
  int low_bin = __float2int_rd((freq-width)/bin_width);
  int high_bin = __float2int_ru((freq+width)/bin_width);

  if (low_bin<0)
    low_bin = 0;
  if (low_bin>=fseries_size)
    return;
  if (high_bin>=fseries_size)
    high_bin = fseries_size-1;
  for (ii=low_bin;ii<high_bin;ii++)
    fseries[ii] = make_cuComplex(1.0,0.0);
}

void device_zap_birdies(cuComplex* fseries, float* d_birdies, float* d_widths, float bin_width,
			unsigned int birdies_size, unsigned int fseries_size,
			unsigned int max_blocks, unsigned int max_threads)
{
  BlockCalculator calc(birdies_size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++)
    zap_birdies_kernel<<<calc[ii].blocks,max_threads>>>(fseries,d_birdies,d_widths,bin_width,birdies_size,fseries_size);
  ErrorChecker::check_cuda_error("Error from device_zap_birdies");
  return;
}

//--------------coincidence matching--------------//

__global__
void coincidence_kernel(float** arrays, float* out_array,
			int narrays, size_t size,
			float thresh, int beam_thresh)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x;
  int count = 0;
  for (int ii=0;ii<narrays;ii++)
    if (arrays[ii][idx]>thresh)
      count++;
  out_array[idx] = (count<beam_thresh);
}

void device_coincidencer(float** arrays, float* out_array,
			 int narrays, size_t size,
			 float thresh, int beam_thresh,
			 unsigned int max_blocks,
			 unsigned int max_threads)
{

  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++)
    coincidence_kernel<<<calc[ii].blocks,max_threads>>>
      (arrays,out_array,narrays,size,thresh,beam_thresh);
  ErrorChecker::check_cuda_error("Error from device_coincidencer");
  return;

}

//--------Correlation tools--------//

__global__ void conjugate_kernel(cufftComplex* x, unsigned int size,
				 unsigned int gulp_idx){
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_idx;
  if (idx<size)
    x[idx].y *= -1.0;
}

void device_conjugate(cufftComplex* x, unsigned int size,
		      unsigned int max_blocks,
		      unsigned int max_threads)
{
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++)
    conjugate_kernel<<<calc[ii].blocks,max_threads>>>(x,size,calc[ii].data_idx);
  ErrorChecker::check_cuda_error("Error from device_conjugate");
  return;
}

__global__ void cuCmulf_inplace_kernel(cufftComplex* x, cufftComplex* y,
						unsigned int size, unsigned int gulp_idx){
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_idx;
  if (idx<size)
    y[idx] = cuCmulf(x[idx],y[idx]);
}

void device_cuCmulf_inplace(cufftComplex* x, cufftComplex* y,
			    unsigned int size,
			    unsigned int max_blocks,
			    unsigned int max_threads)
{
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++)
    cuCmulf_inplace_kernel<<<calc[ii].blocks,max_threads>>>(x,y,size,calc[ii].data_idx);
  ErrorChecker::check_cuda_error("Error from device_cuCmulf_inplace");
  return;
}

//--------type converter--------//
//This is to get around the stupid thrust copy issue

template <class X,class Y> __global__
void conversion_kernel(X* x, Y* y, unsigned int size,
                       unsigned int gulp_idx)
{
  int idx = blockIdx.x * blockDim.x + threadIdx.x + gulp_idx;
  if (idx<size)
    y[idx] = static_cast<Y>(x[idx]);
  return;
}

template __global__ void conversion_kernel<char,float>(char*,float*,unsigned int,unsigned int);
template __global__ void conversion_kernel<unsigned char,float>(unsigned char*,float*,unsigned int,unsigned int);

template <class X,class Y>
void device_conversion(X* x, Y* y, unsigned int size,
                       unsigned int max_blocks,
                       unsigned int max_threads)
{
  BlockCalculator calc(size, max_blocks, max_threads);
  for (int ii=0;ii<calc.size();ii++)
    conversion_kernel<X,Y> <<<calc[ii].blocks,max_threads>>>(x,y,size,calc[ii].data_idx);
  ErrorChecker::check_cuda_error("Error from device_conversion");
  return;
}

template void device_conversion<char,float>(char*, float*, unsigned int, unsigned int, unsigned int);
template void device_conversion<unsigned char,float>(unsigned char*, float*, unsigned int, unsigned int, unsigned int);
template void device_conversion<unsigned int,float>(unsigned int*, float*, unsigned int, unsigned int, unsigned int);
template void device_conversion<unsigned char, double>(unsigned char*, double*, unsigned int, unsigned int, unsigned int);
template void device_conversion<float, float>(float*, float*, unsigned int, unsigned int, unsigned int);




