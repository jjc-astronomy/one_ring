/*──────────────────────────  progress_monitor.hpp  ──────────────────────────*
 *      0‒79 %  → cyan bar
 *     80‒99 % → yellow bar
 *      100 %  → green bar
 *  Bar width defaults to 100 glyphs; tune W below if you like.
 *────────────────────────────────────────────────────────────────────────────*/
#pragma once
#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>

class ProgressMonitor {
    using clk = std::chrono::steady_clock;

/* ───────── immutable configuration ───────── */
    const std::size_t total_dm_, total_bin_;
    const double      pct_step_dm_, pct_step_bin_;
    const std::chrono::milliseconds time_step_;

/* ───────── current state (thread-safe) ───── */
    std::atomic<std::size_t> dm_done_{0}, bin_done_{0};
    std::atomic<double>      last_pct_dm_{0}, last_pct_bin_{0};
    std::atomic<clk::time_point> next_time_;
    std::mutex io_;
    bool first_line_ = true;

/* ───────── ANSI colours ───────── */
    static constexpr const char* GRN = "\033[92m";
    static constexpr const char* YEL = "\033[93m";
    static constexpr const char* CYN = "\033[96m";
    static constexpr const char* DIM = "\033[90m";
    static constexpr const char* RST = "\033[0m";

/* ───────── helpers ───────── */
    static std::string utc_now()
    {
        using namespace std::chrono;
        auto t  = system_clock::to_time_t(system_clock::now());
        std::tm g;  gmtime_r(&t, &g);
        std::ostringstream ss;
        ss << std::put_time(&g, "%Y-%m-%dT%H:%M:%SZ");
        return ss.str();
    }

    /* traffic-light fill colour */
    static constexpr const char* colour(double p)
    {
        return (p >= 1.0) ? GRN :
               (p >= 0.8) ? YEL : CYN;
    }

    /* text bar builder */
    static std::string bar(double p, int W = 100)
    {
        int filled = int(p * W + 0.5);
        constexpr const char* FULL  = u8"█";
        constexpr const char* EMPTY = u8"░";

        std::ostringstream ss;
        ss << colour(p);
        for (int i = 0; i < filled; ++i) ss << FULL;
        ss << DIM;
        for (int i = filled; i < W; ++i) ss << EMPTY;
        ss << RST;
        return ss.str();
    }

    void draw(double pdm, double pbin)
    {
        std::lock_guard<std::mutex> lk(io_);

        if (!first_line_) std::cout << "\033[3F";        // move cursor up 3 lines
        first_line_ = false;

        std::cout << DIM << "Last update (UTC): " << RST << utc_now() << '\n'
                  << "DM-loop           [" << bar(pdm)  << "] "
                  << colour(pdm) << std::setw(3) << int(pdm  * 100) << "% " << RST
                  << "(" << dm_done_  << " / " << total_dm_  << ")\n"
                  << "Binary-trial loop [" << bar(pbin) << "] "
                  << colour(pbin) << std::setw(3) << int(pbin * 100) << "% " << RST
                  << "(" << bin_done_ << " / " << total_bin_ << ")\n"
                  << std::flush;
    }

    bool should_draw(double pdm, double pbin) const
    {
        auto now = clk::now();
        return  now >= next_time_.load(std::memory_order_relaxed)                     ||
               pdm  - last_pct_dm_.load(std::memory_order_relaxed)  >= pct_step_dm_   ||
               pbin - last_pct_bin_.load(std::memory_order_relaxed) >= pct_step_bin_;
    }

    void maybe_draw(double pdm, double pbin, bool force = false)
    {
        if (force || should_draw(pdm, pbin)) {
            last_pct_dm_ .store(pdm,  std::memory_order_relaxed);
            last_pct_bin_.store(pbin, std::memory_order_relaxed);
            next_time_   .store(clk::now() + time_step_, std::memory_order_relaxed);
            draw(pdm, pbin);
        }
    }

/* ───────── public API ───────── */
public:
    ProgressMonitor(std::size_t tot_dm, std::size_t tot_bin,
                    double pct_step_dm  = 0.02,
                    double pct_step_bin = 0.02,
                    std::chrono::seconds t_step = std::chrono::seconds(2))
        : total_dm_(tot_dm ? tot_dm : 1)      // avoid div-by-zero
        , total_bin_(tot_bin ? tot_bin : 1)
        , pct_step_dm_(pct_step_dm)
        , pct_step_bin_(pct_step_bin)
        , time_step_(t_step)
        , next_time_(clk::now() + t_step) {}

    ~ProgressMonitor() { finish(); std::cout << std::endl; }

    void tick_dm(std::size_t n = 1)
    {
        auto done = dm_done_.fetch_add(n) + n;
        double pdm  = double(done)      / total_dm_;
        double pbin = double(bin_done_) / total_bin_;
        maybe_draw(pdm, pbin, done == total_dm_ && bin_done_ == total_bin_);
    }

    void tick_bin(std::size_t n = 1)
    {
        auto done = bin_done_.fetch_add(n) + n;
        double pdm  = double(dm_done_)  / total_dm_;
        double pbin = double(done)      / total_bin_;
        maybe_draw(pdm, pbin, pdm == 1.0 && done == total_bin_);
    }

    void finish()  // call once at the very end (dtor already does)
    {
        draw(double(dm_done_) / total_dm_,
             double(bin_done_) / total_bin_);
    }
};
