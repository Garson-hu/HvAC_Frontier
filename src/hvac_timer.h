#pragma once
#include <chrono>
#include <unordered_map>
#include <string>
#include <mutex>
#include <atomic>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <cstring>
namespace hvac {

struct Stat {
    std::atomic<double> total_us{0.0};
    std::atomic<uint64_t> calls{0};

    // Default constructor for unordered_map when key is not found
    Stat() = default;
    // Copy constructor (often needed if std::atomic members are present,
    // though for map insertion, default construction and then assignment is typical)
    // However, atomics are not copyable. We will rely on default construction
    // by the map and then modify members.
    // If we were to copy Stat objects, we'd need to define how to copy atomics (e.g., load values).
    // For this specific use case with map's operator[], default construction is fine.
};

inline std::unordered_map<std::string, Stat>& get_table() {
    static std::unordered_map<std::string, Stat> tbl;
    return tbl;
}

inline std::mutex& get_mutex() {
    static std::mutex m;
    return m;
}

class TimerGuard {
public:
    explicit TimerGuard(const char* tag)
        : tag_(tag), start_(std::chrono::steady_clock::now()) {}

    ~TimerGuard() {
        // Use std::chrono::steady_clock consistently
        uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_).count();

        auto& tbl = get_table();
        {
            std::lock_guard<std::mutex> lk(get_mutex());
            auto& s = tbl[tag_]; // This might create a new Stat if tag_ doesn't exist

            // Atomically add to total_us for double
            double current_total = s.total_us.load(std::memory_order_relaxed);
            double new_total;
            do {
                new_total = current_total + static_cast<double>(us);
            } while (!s.total_us.compare_exchange_weak(current_total, new_total,
                                                      std::memory_order_release, // or acq_rel
                                                      std::memory_order_relaxed));
            // current_total is updated by compare_exchange_weak on failure, so loop continues with fresh value

            s.calls.fetch_add(1, std::memory_order_relaxed);
        }
    }
private:
    const char* tag_;
    std::chrono::steady_clock::time_point start_;
};


inline void print_all_stats(int epoch_num = -1) { // Default to -1 if no epoch num is provided
    std::stringstream ss;
    // Modify the header to include epoch and PID
    ss << "\n=== HVAC timing summary (";
    if (epoch_num >= 0) {
        ss << "Epoch: " << epoch_num;
    }
    ss << ") ===\n";

    // Use constants for column widths for easier adjustment
    const int section_col_width = 45; // Increased width for potentially longer tags from client side
    const int calls_col_width = 12;
    const int total_us_col_width = 18; // Made wider to accommodate larger numbers and precision
    const int avg_us_col_width = 15;
    const int total_line_width = section_col_width + calls_col_width + total_us_col_width + avg_us_col_width;


    ss << std::left << std::setw(section_col_width) << "Section"
       << std::right << std::setw(calls_col_width) << "Calls"
       << std::right << std::setw(total_us_col_width) << "Total(us)" // Align right
       << std::right << std::setw(avg_us_col_width) << "Avg(us)"   // Align right
       << "\n" << std::string(total_line_width, '-') << "\n"; // Use calculated total width

    {
        std::lock_guard<std::mutex> lk(get_mutex());
        for (auto const& [k, v] : get_table()) {
            double tot = v.total_us.load(std::memory_order_acquire);
            uint64_t c = v.calls.load(std::memory_order_acquire);
            ss << std::left  << std::setw(section_col_width) << k
               << std::right << std::setw(calls_col_width) << c
               << std::right << std::setw(total_us_col_width) << std::fixed << std::setprecision(2) << tot
               << std::right << std::setw(avg_us_col_width) << std::fixed << std::setprecision(2) << (c ? tot / static_cast<double>(c) : 0.0)
               << "\n";
        }
    }
    ss << std::string(total_line_width, '=') << "\n";
    std::cout << ss.str() << std::flush; // Ensure immediate output
}

inline void reset_all_stats() {
    std::lock_guard<std::mutex> lk(get_mutex());
    auto& tbl = get_table();
    for (auto& pair_in_map : tbl) {
        pair_in_map.second.total_us.store(0.0, std::memory_order_relaxed);
        pair_in_map.second.calls.store(0, std::memory_order_relaxed);
    }
    std::cout << "! HVAC Timing Stats have been RESET  !" << std::endl;
}


inline void export_all_stats_to_file(const char* filename, int epoch_num = -1) {
    // (Implementation from previous answer: opens ofstream, writes formatted stats, closes)
    // (It includes error checking for filename and file opening)
    if (!filename || strlen(filename) == 0) {
        std::cerr << "Error: export_all_stats_to_file called with invalid filename. Printing to console instead." << std::endl;
        print_all_stats(epoch_num); // Fallback to printing to console
        return;
    }

    std::ofstream outfile(filename);
    if (!outfile.is_open()) {
        std::cerr << "Error: Could not open file '" << filename << "' for writing stats. Printing to console instead." << std::endl;
        print_all_stats(epoch_num); // Fallback
        return;
    }

    // Get current process ID to make logs more distinguishable if multiple servers write logs

    outfile << std::left << std::setw(45) << "Section" // Increased width for potentially longer tags
            << std::right << std::setw(12) << "Calls"
            << std::setw(15) << "Total(us)"
            << std::setw(15) << "Avg(us)"
            << "\n-----------------------------------------------------------------------------------\n"; // Adjusted line

    {
        std::lock_guard<std::mutex> lk(get_mutex());
        for (auto const& [k, v] : get_table()) {
            double tot = v.total_us.load(std::memory_order_acquire);
            uint64_t c = v.calls.load(std::memory_order_acquire);
            outfile << std::left << std::setw(45) << k
                    << std::right << std::setw(12) << c
                    << std::setw(15) << std::fixed << std::setprecision(2) << tot
                    << std::setw(15) << (c ? tot / static_cast<double>(c) : 0.0)
                    << "\n";
        }
    }
    outfile << "===================================================================================\n";
    outfile.close();
    // Optional: Log to server's console that the export was done
    std::cout << "[HVAC Server Timing summary exported to " << filename << std::endl;
}

} // namespace hvac

#define HVAC_TIMING(name) hvac::TimerGuard __hvac_tg__(name)