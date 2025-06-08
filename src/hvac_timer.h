#pragma once
#include <chrono>
#include <unordered_map>
#include <string>
#include <mutex>
#include <atomic>
#include <iomanip>
#include <iostream>
#include <fstream>
#include <vector>
#include <numeric>    // For std::accumulate
#include <algorithm>  // For std::sort
#include <set>        // For std::set to store tags for detailed logging
#include <unistd.h>   // For getpid()
#include <cstring>    // For strlen

namespace hvac {


inline std::mutex& get_mutex() {
    static std::mutex m;
    return m;
}
    

/*
    Used for get the tag that which HVAC_TIMING should print the single call time.
*/
inline std::set<std::string>& get_detailed_log_tags_set()
{
    static std::set<std::string> detailed_tags;
    return detailed_tags;
}

inline void enable_detailed_call_logging_for_tag(const std::string& tag) {
    std::lock_guard<std::mutex> lk(get_mutex());
    get_detailed_log_tags_set().insert(tag);
    // std::cout << "[HVAC Timer] Detailed call logging enabled for tag: " << tag << std::endl;
}

inline void disable_detailed_call_logging_for_tag(const std::string& tag)
{
    std::lock_guard<std::mutex> lk(get_mutex());
    get_detailed_log_tags_set().erase(tag);
    // std::cout << "[HVAC Timer] Detailed call logging disabled for tag: " << tag << std::endl;
}

inline std::unordered_map<std::string, std::vector<uint64_t>>& get_call_history_table()
{
    static std::unordered_map<std::string, std::vector<uint64_t>> call_history_table;
    return call_history_table;
}


struct Stat {
    std::atomic<double> total_us{0.0};
    std::atomic<uint64_t> calls{0};

    // Default constructor for unordered_map when key is not found
    Stat() = default;
};

inline std::unordered_map<std::string, Stat>& get_table() {
    static std::unordered_map<std::string, Stat> tbl;
    return tbl;
}

class TimerGuard {
    public:
        explicit TimerGuard(const char* tag)
            : tag_name_str_(tag), start_(std::chrono::steady_clock::now()) { // Store tag as std::string
              // This constructor is a good place to check if the tag exists in get_detailed_log_tags_set()
              // if many tags are dynamically created, to avoid growing the set unnecessarily.
              // However, for simplicity, we check in the destructor.
          }
    
        ~TimerGuard() {
            uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_).count();
    
            std::lock_guard<std::mutex> lk(get_mutex()); // Acquire lock once for all modifications
    
            // 1. Update cumulative statistics
            auto& cumulative_tbl = get_table();
            auto& s_cumulative = cumulative_tbl[tag_name_str_]; // Use std::string key
    
            double current_total = s_cumulative.total_us.load(std::memory_order_relaxed);
            double new_total;
            do {
                new_total = current_total + static_cast<double>(us);
            } while (!s_cumulative.total_us.compare_exchange_weak(current_total, new_total,
                                                                  std::memory_order_release,
                                                                  std::memory_order_relaxed));
            s_cumulative.calls.fetch_add(1, std::memory_order_relaxed);
    
            // 2. Conditionally log individual call duration for detailed analysis
            const auto& detailed_tags_to_log = get_detailed_log_tags_set();
            if (detailed_tags_to_log.count(tag_name_str_)) {
                auto& call_history_tbl = get_call_history_table();
                call_history_tbl[tag_name_str_].push_back(us);
            }
        }
    private:
        std::string tag_name_str_; // Store tag as std::string to ensure its lifetime and for map key
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

    // Reset detailed call history table for enabled tags
    auto& call_history_tbl = get_call_history_table();
    const auto& detailed_tags_to_reset = get_detailed_log_tags_set(); // Read the set of tags
    for (const std::string& tag : detailed_tags_to_reset) {
        if (call_history_tbl.count(tag)) {
            call_history_tbl[tag].clear();
           
        }
    }

    std::cout << "! HVAC Timing Stats have been RESET  !" << std::endl;
}


/*
    Function to write all the recorded call history for a specific tag to a CSV file.
*/
inline void export_tag_call_history_to_file(const std::string& tag_to_export, const char* filename, int epoch_num = -1) {
    if (!filename || strlen(filename) == 0) {
        std::cerr << "Error: export_tag_call_history_to_file called with invalid or empty filename." << std::endl;
        return;
    }

    // Open in append mode so multiple calls (e.g. per epoch for the same tag) append.
    std::ofstream outfile(filename, std::ios_base::app); 
    if (!outfile.is_open()) {
        std::cerr << "Error: Could not open file '" << filename << "' for writing detailed call history for tag '" << tag_to_export << "'." << std::endl;
        return;
    }
    
    outfile << "\n=== HVAC Detailed Call History for Tag: \"" << tag_to_export << "\" (";
    if (epoch_num >= 0) {
        outfile << ", For Client Epoch: " << epoch_num; // client's epoch context
    }
    outfile << ") ===\n";
    outfile << "Call_Index,Duration_us\n"; // CSV Header

    size_t calls_exported = 0;
    {
        std::lock_guard<std::mutex> lk(get_mutex()); // Protect access to call_history_tbl
        auto& call_history_tbl = get_call_history_table();
        auto it = call_history_tbl.find(tag_to_export);
        // outfile << "tag print" << it->first << std::endl;
        if (it != call_history_tbl.end()) 
        {
            const auto& durations = it->second;
            for (size_t i = 0; i < durations.size(); ++i) {
                outfile << (i + 1) << "," << durations[i] << "\n";
                calls_exported++;
            }
            if (durations.empty()) {
                outfile << "(No calls recorded for this tag in this period/since last reset)\n";
            }
        } else 
        {
            outfile << "(Tag \"" << tag_to_export << "\" not found in detailed history or not enabled for detailed logging)\n";
        }
    }
    outfile << "--- End of detailed history for tag: \"" << tag_to_export << "\" (Exported " << calls_exported << " calls) ---\n";
    outfile << "==============================================================================\n";
    outfile.close();
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