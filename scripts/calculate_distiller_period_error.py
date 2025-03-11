#!/usr/bin/env python3
import argparse

def period_tolerance(pulsar_period=1e-3, freq_tol=0.0001, acc=50, tobs=7200.0, c=3e8):
    """
    Calculate period tolerance for a pulsar given a frequency tolerance.

    For a pulsar with period P:
        f = 1 / P.
    A frequency tolerance freq_tol gives a base frequency window:
        Δf_base = f * freq_tol.
    The corresponding period tolerance is:
        ΔP_base ≈ Δf_base / f^2 = freq_tol / f.

    Additionally, if acceleration is provided (acc != 0), then the acceleration
    introduces an extra frequency shift:
        Δf_acc = |acc| * f * (tobs / c),
    which translates to a period error:
        ΔP_acc ≈ Δf_acc / f^2 = |acc| * (tobs / c) / f.

    Parameters:
      pulsar_period : float
          Pulsar spin period in seconds (default 1e-3 for a 1ms pulsar).
      freq_tol : float
          Fractional frequency tolerance (default 0.0001).
      acc : float
          Acceleration in m/s² (default 50). Set to 0 for no acceleration correction.
      tobs : float
          Observation time in seconds (default 7200 s).
      c : float
          Speed of light in m/s (default 3e8 m/s).

    Returns:
      If acc == 0:
          A single float representing the base period tolerance.
      Else:
          A tuple (ΔP_base, ΔP_acc) where:
              ΔP_base is the period tolerance from freq_tol,
              ΔP_acc is the additional period error due to acceleration.
    """
    f = 1.0 / pulsar_period  # Spin frequency in Hz
    base_delta_f = f * freq_tol  # Frequency tolerance window in Hz
    base_delta_P = freq_tol / f  # Base period tolerance

    if acc == 0:
        return base_delta_P
    else:
        additional_delta_f = abs(acc) * f * (tobs / c)
        additional_delta_P = abs(acc) * (tobs / c) / f
        return base_delta_P, additional_delta_P

def main():
    parser = argparse.ArgumentParser(
        description="Calculate period tolerance for a pulsar given a frequency tolerance and acceleration."
    )
    parser.add_argument(
        "--period",
        type=float,
        default=1e-3,
        help="Pulsar spin period in seconds (default: 1e-3 s)"
    )
    parser.add_argument(
        "--freq_tol",
        type=float,
        default=0.0001,
        help="Fractional frequency tolerance (default: 0.0001)"
    )
    parser.add_argument(
        "--acc",
        type=float,
        default=50,
        help="Acceleration in m/s² (default: 50). Set to 0 for no acceleration."
    )
    parser.add_argument(
        "--tobs",
        type=float,
        default=7200.0,
        help="Observation time in seconds (default: 7200 s)"
    )
    args = parser.parse_args()

    result = period_tolerance(
        pulsar_period=args.period,
        freq_tol=args.freq_tol,
        acc=args.acc,
        tobs=args.tobs
    )

    if isinstance(result, tuple):
        base_tol, acc_tol = result
        print("For a pulsar with period {:.2e} s:".format(args.period))
        print("With Frequency Tolerance: {:.2e}".format(args.freq_tol))
        print("Base period tolerance: {:.2e} s".format(base_tol))
        print("Acceleration-induced period error: {:.2e} s".format(acc_tol))
    else:
        print("For a pulsar with period {:.2e} s:".format(args.period))
        print("Period tolerance (no acceleration): {:.2e} s".format(result))

if __name__ == "__main__":
    main()
