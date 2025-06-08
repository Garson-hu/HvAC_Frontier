//Starting to use CPP functionality

#pragma once
#include <map>
#include <string>
#include <filesystem>
#include <iostream>
#include <assert.h>
#include <unordered_map>

#include "hvac_internal.h"
#include "hvac_logging.h"
#include "hvac_comm.h"
#include "hvac_timer.h" // ! HVAC TIMING

//* add below

FILE* (*__real_fopen)(const char *path, const char *mode) = NULL;
FILE* (*__real_fopen64)(const char *path, const char *mode) = NULL;
ssize_t (*__real_pread)(int fd, void *buf, size_t count, off_t offset) = NULL;
ssize_t (*__real_readv)(int fd, const struct iovec *iov, int iovcnt) = NULL;
ssize_t (*__real_write)(int fd, const void *buf, size_t count) = NULL;
int (*__real_open)(const char *pathname, int flags, ...) = NULL;
int (*__real_open64)(const char *pathname, int flags, ...) = NULL;
ssize_t (*__real_read)(int fd, void *buf, size_t count) = NULL;
ssize_t (*__real_read64)(int fd, void *buf, size_t count) = NULL;
int (*__real_close)(int fd) = NULL;
off_t (*__real_lseek)(int fd, off_t offset, int whence) = NULL;
off64_t (*__real_lseek64)(int fd, off64_t offset, int whence) = NULL;

// ! HVAC TIMING
extern "C" void hvac_setup_detailed_logging();

#define HVAC_CLIENT 1

__thread bool tl_disable_redirect = false;
bool g_disable_redirect = true;
bool g_hvac_initialized = false;
bool g_hvac_comm_initialized = false;
bool g_mercury_init=false;

uint32_t g_hvac_server_count = 0;
char *hvac_data_dir = NULL;

pthread_mutex_t init_mutex = PTHREAD_MUTEX_INITIALIZER;

std::unordered_map<int,std::string> fd_map;
std::unordered_map<int, int > fd_redir_map;

void Initialize_function() {
    L4C_INFO("Executing Initialize_function");
    {
        // HVAC_TIMING("Initialize_timer");
        // Do some trivial work or sleep for a short time
        for (volatile int i = 0; i < 10000; ++i); // volatile to prevent optimization
    } // TimerGuard for "guaranteed_test_timer" destroyed here
    L4C_INFO("Finished Initialize_timer");
}

/* Devise a way to safely call this and initialize early */
static void __attribute__((constructor)) hvac_client_init()
{	
    pthread_mutex_lock(&init_mutex);
    if (g_hvac_initialized){
        pthread_mutex_unlock(&init_mutex);
        return;
    }
    hvac_init_logging();

    char * hvac_data_dir_c = getenv("HVAC_DATA_DIR");

    if (getenv("HVAC_SERVER_COUNT") != NULL)
    {
        g_hvac_server_count = atoi(getenv("HVAC_SERVER_COUNT"));
    }
    else
    {        
        L4C_FATAL("Please set enviroment variable HVAC_SERVER_COUNT\n");
        exit(-1);
    }


    if (hvac_data_dir_c != NULL)
    {
		hvac_data_dir = (char *)malloc(strlen(hvac_data_dir_c) + 1);
		snprintf(hvac_data_dir, strlen(hvac_data_dir_c) + 1, "%s", hvac_data_dir_c);
    }
    

    g_hvac_initialized = true;

    pthread_mutex_unlock(&init_mutex);
	//! HVAC TIMING
	hvac_setup_detailed_logging();
	g_disable_redirect = false;

	Initialize_function();
}

static void __attribute((destructor)) hvac_client_shutdown()
{
    hvac_shutdown_comm();
}

bool hvac_track_file(const char *path, int flags, int fd)
{   
	// HVAC_TIMING("CLIENT_(hvac_track_file)_total");    
	if (strstr(path, ".ports.cfg.") != NULL)
	{
		return false;
	}
	//Always back out of RDONLY
	bool tracked = false;
	if ((flags & O_ACCMODE) == O_WRONLY) {
		return false;
	}

	if ((flags & O_APPEND)) {
		return false;
	}    

	try {
		std::string ppath = std::filesystem::canonical(path).parent_path();
		// Check if current file exists in HVAC_DATA_DIR
		if (hvac_data_dir != NULL){
			std::string test = std::filesystem::canonical(hvac_data_dir);
			
			if (ppath.find(test) != std::string::npos)
			{
				//L4C_FATAL("Got a file want a stack trace");
				L4C_INFO("Traacking used HV_DD file %s",path);
				fd_map[fd] = std::filesystem::canonical(path);
				tracked = true;
			}		
		}else if (ppath == std::filesystem::current_path()) {       
			L4C_INFO("Traacking used CWD file %s",path);
			fd_map[fd] = std::filesystem::canonical(path);
			tracked = true;
		}
	} catch (...)
	{
		//Need to do something here
	}


	// Send RPC to tell server to open file 
	if (tracked){
		if (!g_mercury_init){
			hvac_init_comm(false);	
			/* I think I only need to do this once */
			hvac_client_comm_register_rpc();
			g_mercury_init = true;
		}
		
		int host = std::hash<std::string>{}(fd_map[fd]) % g_hvac_server_count;	
		L4C_INFO("Remote open - Host %d", host);
		ssize_t open_result;
		{
			// HVAC_TIMING("CLIENT_(comm_gen_open_rpc)_dispatch");
			// HVAC_TIMING("CLIENT_hvac_open_rpc_wait_data"); 
			open_result = hvac_client_comm_gen_open_rpc(host, fd_map[fd], fd);
		}
		
		if (open_result < 0) {
			L4C_ERR("Remote open failed for file %s", path);
			tracked = false;  // If remote open failed, don't track the file
		}
	}


	return tracked;
}

/* Need to clean this up - in theory the RPC should time out if the request hasn't been serviced we'll go to the file-system?
 * Maybe not - we'll roll to another server.
 * For now we return true to keep the good path happy
 */
ssize_t hvac_remote_read(int fd, void *buf, size_t count)
{
	// HVAC_TIMING("CLIENT_(hvac_remote_read)_total");
	/* HVAC Code */
	/* Check the local fd - if it's tracked we pass it to the RPC function
	 * The local FD is converted to the remote FD with the buf and count
	 * We must know the remote FD to avoid collision on the remote side
	 */
	ssize_t bytes_read = -1;
	if (hvac_file_tracked(fd)){
		int host = std::hash<std::string>{}(fd_map[fd]) % g_hvac_server_count;	
		L4C_INFO("Remote read - Host %d", host);		
		bytes_read = hvac_client_comm_gen_read_rpc(host, fd, buf, count, -1);
		return bytes_read;
	}
	/* Non-HVAC Reads come from base */
	return bytes_read;
}

/* Need to clean this up - in theory the RPC should time out if the request hasn't been serviced we'll go to the file-system?
 * Maybe not - we'll roll to another server.
 * For now we return true to keep the good path happy
 */
ssize_t hvac_remote_pread(int fd, void *buf, size_t count, off_t offset)
{
	// HVAC_TIMING("CLIENT_(hvac_remote_pread)_total");
	/* HVAC Code */
	/* Check the local fd - if it's tracked we pass it to the RPC function
	 * The local FD is converted to the remote FD with the buf and count
	 * We must know the remote FD to avoid collision on the remote side
	 */
	ssize_t bytes_read = -1;
	if (hvac_file_tracked(fd) && fd_redir_map[fd] != 0){
		int host = std::hash<std::string>{}(fd_map[fd]) % g_hvac_server_count;	
		L4C_INFO("Remote pread - Host %d", host);	
		{
			// HVAC_TIMING("CLIENT_(hvac_remote_pread)_dispatch");	
			// HVAC_TIMING("CLIENT_(hvac_remote_pread)_wait_data"); 
			bytes_read = hvac_client_comm_gen_read_rpc(host, fd, buf, count, offset);
		}
	}
	/* Non-HVAC Reads come from base */
	return bytes_read;
}

ssize_t hvac_remote_lseek(int fd, int offset, int whence)
{
		/* HVAC Code */
	/* Check the local fd - if it's tracked we pass it to the RPC function
	 * The local FD is converted to the remote FD with the buf and count
	 * We must know the remote FD to avoid collision on the remote side
	 */
	ssize_t bytes_read = -1;
	if (hvac_file_tracked(fd)){
		int host = std::hash<std::string>{}(fd_map[fd]) % g_hvac_server_count;	
		L4C_INFO("Remote seek - Host %d", host);		
		bytes_read = hvac_client_comm_gen_seek_rpc(host, fd, offset, whence);
		return bytes_read;
	}
	/* Non-HVAC Reads come from base */
	return bytes_read;
}

void hvac_remote_close(int fd){
	if (hvac_file_tracked(fd)){
		int host = std::hash<std::string>{}(fd_map[fd]) % g_hvac_server_count;	
		hvac_client_comm_gen_close_rpc(host, fd);             	
	}
}

bool hvac_file_tracked(int fd)
{
	// HVAC_TIMING("CLIENT_(hvac_file_tracked)_total");
	return (fd_map.find(fd) != fd_map.end());
}

const char * hvac_get_path(int fd)
{	
	// HVAC_TIMING("CLIENT_(hvac_get_path)_total");
	if (fd_map.find(fd) != fd_map.end())
	{
		return fd_map[fd].c_str();
	}
	return NULL;
}

bool hvac_remove_fd(int fd)
{
	hvac_remote_close(fd);	
	return fd_map.erase(fd);
}

extern "C" {
    void hvac_trigger_print_all_stats(int epoch_num) {
        hvac::print_all_stats(epoch_num);
    }

	void hvac_trigger_reset_all_stats() {
        hvac::reset_all_stats();
    }
}

// Used for initiate the detailed logs of a specific tag
extern "C" void hvac_setup_detailed_logging() {
	hvac::enable_detailed_call_logging_for_tag("CLIENT_hvac_open_rpc_wait_data");
}

// Used to export detailed call history for a specific tag to a CSV file
extern "C" void hvac_client_export_tag_details(const char* tag_name_c_str, const char* output_filename_c_str, int epoch_num) {
    if (tag_name_c_str && output_filename_c_str) {
        std::string tag_name(tag_name_c_str);
        std::string output_filename(output_filename_c_str);
        hvac::export_tag_call_history_to_file(tag_name, output_filename.c_str(), epoch_num);
    }
}