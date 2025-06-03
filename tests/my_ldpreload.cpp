#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <iostream>
#include <stdio.h>
#include <dlfcn.h>
#include <stdarg.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>
#include <set>
#include <string.h>

bool verbose = 0;

extern "C" {
    int (*real_open)(const char *pathname, int flags, ...) = nullptr;
    int (*real_close)(int fd) = nullptr;
    ssize_t (*real_pread)(int fd, void *buf, size_t count, off_t offset) = nullptr;

    
    // int (*real_open64)(const char *pathname, int flags, ...) = nullptr;
    // FILE* (*real_fopen)(const char *pathname, const char *mode) = nullptr;
    //  ssize_t (*real_read)(int fd, void *buf, size_t count) = nullptr;
    // ssize_t (*real_read64)(int fd, void *buf, size_t count) = nullptr;
    // ssize_t (*real_readv)(int fd, const struct iovec *iov, int iovcnt) = nullptr;
    // void (*real_exit)(int status) = nullptr;

    // 统计结构
    struct Stats {
        size_t count = 0;          // 调用次数
        double total_time = 0.0;   // 总运行时间
    };

    // 数据组和系统组
    Stats open_data_stats;         // open 数据组
    Stats open_system_stats;       // open 系统组
    Stats open64_stats;       // open 系统组
    Stats fopen_stats;             // fopen 统计
    Stats close_stats;             // close 统计
    Stats read_stats;              // read 统计
    Stats read64_stats;            // read64 统计
    Stats readv_stats;             // readv 统计
    Stats pread_stats;             // pread 统计

    // open 实现
    int open(const char *pathname, int flags, ...) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);
        if (!real_open) {
            real_open = (int (*)(const char*, int, ...)) dlsym(RTLD_NEXT, "open");
        }

        va_list args;
        va_start(args, flags);
        int fd;

        if (flags & O_CREAT) {
            mode_t mode = va_arg(args, mode_t);
            fd = real_open(pathname, flags, mode);
        } else {
            fd = real_open(pathname, flags);
        }
        va_end(args);
        clock_gettime(CLOCK_MONOTONIC, &end);

        double delta;
        if(end.tv_nsec > start.tv_nsec) {
            delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
        }
        else{
            delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
        }
        
        if(verbose)
            printf("DEBUG_HU: MY_LD_PRELOAD: Open fd %d from pathname %s, delta: %.8f\n", fd, pathname, delta);

        fflush(stdout);
        if (strstr(pathname, "/mnt/beegfs/ghu4/hvac/cosmoUniverse_2019_05_4parE_tf_v2_mini/")) {
            open_data_stats.count++;
            open_data_stats.total_time += delta;
        } else {
            open_system_stats.count++;
            open_system_stats.total_time += delta;
        }

        return fd;
    }

    // open64 实现
    // int open64(const char *pathname, int flags, ...) {
    //     struct timespec start, end;
    //     clock_gettime(CLOCK_MONOTONIC, &start);

    //     if (!real_open64) {
    //         real_open64 = (int (*)(const char *, int, ...)) dlsym(RTLD_NEXT, "open64");
    //     }

    //     va_list args;
    //     va_start(args, flags);
    //     int fd;

    //     if (flags & O_CREAT) {
    //         mode_t mode = va_arg(args, mode_t);
    //         fd = real_open64(pathname, flags, mode);
    //     } else {
    //         fd = real_open64(pathname, flags);
    //     }
    //     va_end(args);
    //     clock_gettime(CLOCK_MONOTONIC, &end);

    //     double delta;
    //     if(end.tv_nsec > start.tv_nsec) {
    //         delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
    //     }
    //     else{
    //         delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
    //     }
    //     if(verbose)
    //         printf("DEBUG_HU: MY_LD_PRELOAD: Open64 fd %d from pathname %s, delta: %.8f\n", fd, pathname, delta);
    //     fflush(stdout);

    //     open64_stats.count++;
    //     open64_stats.total_time += delta;

    //     return fd;
    // }

    // fopen 实现
    // FILE* fopen(const char *pathname, const char *mode) {
    //     struct timespec start, end;
    //     clock_gettime(CLOCK_MONOTONIC, &start);

    //     if (!real_fopen) {
    //         real_fopen = (FILE* (*)(const char*, const char*)) dlsym(RTLD_NEXT, "fopen");
    //     }

    //     FILE* file = real_fopen(pathname, mode);
    //     clock_gettime(CLOCK_MONOTONIC, &end);

    //     double delta;
    //     if(end.tv_nsec > start.tv_nsec) {
    //         delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
    //     }
    //     else{
    //         delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
    //     }

    //     if(verbose)
    //         printf("DEBUG_HU: MY_LD_PRELOAD: Fopen from pathname %s, delta: %.8f\n", pathname, delta);
    //     fflush(stdout);

    //     fopen_stats.count++;
    //     // printf("DEBUG_HU: MY_LD_PRELOAD: fopen_stats.count: %ld \n", fopen_stats.count);

    //     fopen_stats.total_time += delta;

    //     return file;
    // }

    // close 实现
    int close(int fd) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);

        if (!real_close) {
            real_close = (int (*)(int)) dlsym(RTLD_NEXT, "close");
        }

        int ret = real_close(fd);
        clock_gettime(CLOCK_MONOTONIC, &end);

        double delta;
        if(end.tv_nsec > start.tv_nsec) {
            delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
        }
        else{
            delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
        }

        if(verbose)
            printf("DEBUG_HU: MY_LD_PRELOAD: Close fd %d, delta: %.8f\n", fd, delta);
        fflush(stdout);
        
        close_stats.count++;
        close_stats.total_time += delta;

        return ret;
    }

    // read 实现
    // ssize_t read(int fd, void *buf, size_t count) {
    //     struct timespec start, end;
    //     clock_gettime(CLOCK_MONOTONIC, &start);

    //     if (!real_read) {
    //         real_read = (ssize_t (*)(int, void*, size_t)) dlsym(RTLD_NEXT, "read");
    //     }

    //     ssize_t ret = real_read(fd, buf, count);
    //     clock_gettime(CLOCK_MONOTONIC, &end);

    //     double delta;
    //     if(end.tv_nsec > start.tv_nsec) {
    //         delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
    //     }
    //     else{
    //         delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
    //     }
    //     if(verbose)
    //         printf("DEBUG_HU: MY_LD_PRELOAD: Read fd %d , delta: %.8f\n", fd, delta);
    //     fflush(stdout);

    //     read_stats.count++;
    //     read_stats.total_time += delta;
    //     // printf("DEBUG_HU: MY_LD_PRELOAD: fopen_stats.count: %ld \n", fopen_stats.count);
    //     fflush(stdout);
    //     return ret;
    // }

    // read64 实现
    // ssize_t read64(int fd, void *buf, size_t count) {
        
    //     struct timespec start, end;
    //     clock_gettime(CLOCK_MONOTONIC, &start);

    //     if (!real_read64) {
    //         real_read64 = (ssize_t (*)(int, void*, size_t)) dlsym(RTLD_NEXT, "read64");
    //     }

    //     ssize_t ret = real_read64(fd, buf, count);
    //     clock_gettime(CLOCK_MONOTONIC, &end);

    //     double delta;
    //     if(end.tv_nsec > start.tv_nsec) {
    //         delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
    //     }
    //     else{
    //         delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
    //     }

    //     if(verbose)
    //         printf("DEBUG_HU: MY_LD_PRELOAD: Read64 fd %d , delta: %.8f\n", fd, delta);
    //     fflush(stdout);

    //     read64_stats.count++;
    //     read64_stats.total_time += delta;

    //     return ret;
    // }

    // readv 实现
    // ssize_t readv(int fd, const struct iovec *iov, int iovcnt) {
    //     struct timespec start, end;
    //     clock_gettime(CLOCK_MONOTONIC, &start);

    //     if (!real_readv) {
    //         real_readv = (ssize_t (*)(int, const struct iovec*, int)) dlsym(RTLD_NEXT, "readv");
    //     }

    //     ssize_t ret = real_readv(fd, iov, iovcnt);
    //     clock_gettime(CLOCK_MONOTONIC, &end);

    //     double delta;
    //     if(end.tv_nsec > start.tv_nsec) {
    //         delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
    //     }
    //     else{
    //         delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
    //     }

    //     if(verbose)
    //         printf("DEBUG_HU: MY_LD_PRELOAD: Readv fd %d , delta: %.8f\n", fd, delta);
    //     fflush(stdout);

    //     readv_stats.count++;
    //     readv_stats.total_time += delta;

    //     return ret;
    // }

    // pread 实现
    ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
        struct timespec start, end;
        clock_gettime(CLOCK_MONOTONIC, &start);        

        if (!real_pread) {
            real_pread = (ssize_t (*)(int, void*, size_t, off_t)) dlsym(RTLD_NEXT, "pread");
        }

        ssize_t ret = real_pread(fd, buf, count, offset);
        clock_gettime(CLOCK_MONOTONIC, &end);

        double delta;
        if(end.tv_nsec > start.tv_nsec) {
            delta = (end.tv_sec - start.tv_sec)  + (end.tv_nsec - start.tv_nsec) / 1e9;
        }
        else{
            delta = (end.tv_sec - start.tv_sec) - 1  + ((end.tv_nsec - start.tv_nsec) + 1000000000) / 1e9;
        }

        if(verbose)
            printf("DEBUG_HU: MY_LD_PRELOAD: Pread fd %d , delta: %.8f\n", fd, delta);
        fflush(stdout);

        pread_stats.count++;
        pread_stats.total_time += delta;

        return ret;
    }

}
