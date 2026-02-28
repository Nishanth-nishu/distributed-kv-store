#pragma once
/// @file logger.h
/// @brief Lightweight, thread-safe logging utility.

#include <iostream>
#include <sstream>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <string>

namespace kvstore {

enum class LogLevel { DEBUG, INFO, WARN, ERROR };

class Logger {
public:
    static Logger& Instance() {
        static Logger instance;
        return instance;
    }

    void SetLevel(LogLevel level) { level_ = level; }
    void SetNodeId(const std::string& id) { node_id_ = id; }

    template <typename... Args>
    void Log(LogLevel level, const char* file, int line, Args&&... args) {
        if (level < level_) return;

        std::ostringstream oss;
        oss << Timestamp() << " "
            << LevelStr(level) << " ";
        if (!node_id_.empty()) oss << "[" << node_id_ << "] ";
        oss << "[" << BaseName(file) << ":" << line << "] ";
        (oss << ... << std::forward<Args>(args));
        oss << "\n";

        std::lock_guard<std::mutex> lock(mutex_);
        std::cerr << oss.str();
    }

private:
    Logger() = default;
    LogLevel    level_   = LogLevel::INFO;
    std::string node_id_;
    std::mutex  mutex_;

    static const char* LevelStr(LogLevel l) {
        switch (l) {
            case LogLevel::DEBUG: return "DEBUG";
            case LogLevel::INFO:  return "INFO ";
            case LogLevel::WARN:  return "WARN ";
            case LogLevel::ERROR: return "ERROR";
        }
        return "?????";
    }

    static std::string BaseName(const char* path) {
        std::string s(path);
        auto pos = s.find_last_of("/\\");
        return pos == std::string::npos ? s : s.substr(pos + 1);
    }

    static std::string Timestamp() {
        auto now  = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        auto ms   = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now.time_since_epoch()) % 1000;
        std::ostringstream oss;
        oss << std::put_time(std::localtime(&time), "%H:%M:%S")
            << '.' << std::setfill('0') << std::setw(3) << ms.count();
        return oss.str();
    }
};

#define LOG_DEBUG(...) kvstore::Logger::Instance().Log(kvstore::LogLevel::DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  kvstore::Logger::Instance().Log(kvstore::LogLevel::INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  kvstore::Logger::Instance().Log(kvstore::LogLevel::WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) kvstore::Logger::Instance().Log(kvstore::LogLevel::ERROR, __FILE__, __LINE__, __VA_ARGS__)

}  // namespace kvstore
