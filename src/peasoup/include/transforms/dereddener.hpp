#pragma once
#include "transforms/ffter.hpp"
#include "data_types/fourierseries.hpp"
#include "kernels/kernels.h"
#include "kernels/defaults.h"
#include "utils/utils.hpp"
#include "utils/exceptions.hpp"
#include <iostream>
#include <algorithm>

/*
  Running-median envelope of a spectrum, built from block medians of 5, 25, 125 and optionally 625 and 3125
  bins (each level is the 5-bin median of the previous one), linearly stretched back to the full resolution.
  The level used depends on the frequency: 5-bin medians below boundary_5_freq, 25-bin medians below
  boundary_25_freq, 125-bin medians below boundary_125_freq, 625-bin medians below boundary_625_freq and
  3125-bin medians above. The two wide levels are off by default (boundaries at infinity) and are meant for
  whitening after resampling (--deredden_post_resample): a wider window at high frequency keeps the median
  from riding up onto signals that a slightly mismatched template leaves smeared over tens of bins.
*/
class Dereddener {
private:
  unsigned int size;
  float* median_5;
  float* median_25;
  float* median_125;
  float* median_625;
  float* median_3125;
  float* median;
  float* intermediate;
  
public:
  Dereddener(unsigned int size)
    :size(size)
  {
    Utils::device_malloc(&intermediate,size);
    Utils::device_malloc(&median,size);
    Utils::device_malloc(&median_5,size/5);
    Utils::device_malloc(&median_25,size/5/5);
    Utils::device_malloc(&median_125,size/5/5/5);
    Utils::device_malloc(&median_625,std::max(1u,size/5/5/5/5));
    Utils::device_malloc(&median_3125,std::max(1u,size/5/5/5/5/5));
  }
  
  virtual ~Dereddener()
  {
    Utils::device_free(median);
    Utils::device_free(median_5);
    Utils::device_free(median_25);
    Utils::device_free(median_125);
    Utils::device_free(median_625);
    Utils::device_free(median_3125);
    Utils::device_free(intermediate);
  }

  void calculate_median(DevicePowerSpectrum<float>& powers, 
			float boundary_5_freq=0.05,
			float boundary_25_freq=0.5,
			float boundary_125_freq=1e30,
			float boundary_625_freq=1e30)
  {
    if (powers.get_nbins()!=size)
      ErrorChecker::throw_error("Bad data length given to running_median()");
  
    auto clamp_pos = [&](float freq) {
      double p = (double) freq / powers.get_bin_width();
      return (unsigned int) std::min<double>(p < 0 ? 0 : p, (double) size);
    };
    unsigned int pos5   = clamp_pos(boundary_5_freq);
    unsigned int pos25  = clamp_pos(boundary_25_freq);
    unsigned int pos125 = clamp_pos(boundary_125_freq);
    unsigned int pos625 = clamp_pos(boundary_625_freq);
    if (pos25 < pos5) pos25 = pos5;
    if (pos125 < pos25) pos125 = pos25;
    if (pos625 < pos125) pos625 = pos125;
    // the wide levels need at least 5 blocks to be meaningful
    bool use_625  = pos125 < size && size/625  >= 5;
    bool use_3125 = pos625 < size && size/3125 >= 5;
    if (!use_625)  { pos125 = size; pos625 = size; }
    if (!use_3125) { pos625 = size; }

    median_scrunch5(powers.get_data(),size,median_5);
    median_scrunch5(median_5,size/5,median_25);
    median_scrunch5(median_25,size/5/5,median_125);
    if (use_625)  median_scrunch5(median_125,size/5/5/5,median_625);
    if (use_3125) median_scrunch5(median_625,size/5/5/5/5,median_3125);
    
    linear_stretch(median_5,size/5,intermediate,size);
    Utils::d2dcpy(median,intermediate,pos5);
    
    linear_stretch(median_25,size/5/5,intermediate,size);
    Utils::d2dcpy(median+pos5,intermediate+pos5,pos25-pos5);
    
    linear_stretch(median_125,size/5/5/5,intermediate,size);
    Utils::d2dcpy(median+pos25,intermediate+pos25,pos125-pos25);

    if (use_625) {
      linear_stretch(median_625,size/5/5/5/5,intermediate,size);
      Utils::d2dcpy(median+pos125,intermediate+pos125,pos625-pos125);
    }
    if (use_3125) {
      linear_stretch(median_3125,size/5/5/5/5/5,intermediate,size);
      Utils::d2dcpy(median+pos625,intermediate+pos625,size-pos625);
    }
  }
  
  void deredden(DeviceFourierSeries<cufftComplex>& spectrum){
    device_divide_c_by_f(spectrum.get_data(),median,spectrum.get_nbins(),MAX_BLOCKS,MAX_THREADS);
  }

  /* Divide a power spectrum by the running-median envelope computed with calculate_median()
     (used for whitening after resampling, see --deredden_post_resample). */
  void whiten(DevicePowerSpectrum<float>& powers){
    if (powers.get_nbins()!=size)
      ErrorChecker::throw_error("Bad data length given to whiten()");
    device_divide_f_by_f(powers.get_data(),median,size,MAX_BLOCKS,MAX_THREADS);
  }
  
};
