#include <csignal>   // signal
#include <cstdlib>   // exit
#include <atomic>
#include <iostream>
#include <string.h>
#include<pthread.h>
#include <stdio.h>

#include <stdlib.h>
#include <unistd.h>
#include "hvac_timer.h"  // ! HVAC TIMING
#include "hvac_comm.h"
#include "hvac_data_mover_internal.h"


#define HVAC_SERVER 1

extern "C" {
#include "hvac_logging.h"
}

__thread bool tl_disable_redirect = false;
uint32_t hvac_server_count = 0;
static std::atomic<bool> g_running{true};

struct hvac_lookup_arg {
	hg_class_t *hg_class;
	hg_context_t *context;
	hg_id_t id;
	hg_addr_t addr;
};

void signal_exit(int signum)
{
    std::cerr << "\n[hvac_server] Caught signal " << signum
              << ", exiting...\n";

    g_running = false;

    std::exit(0);
}

int hvac_start_comm_server(void)
{

    // !--- BEGIN: Write PID to file ---
    pid_t current_pid = getpid();
    const char* pid_file_path = "/tmp/hvac_server.pid"; // Fixed path for simplicity
    FILE* pid_f = fopen(pid_file_path, "w");
    if (pid_f) {
        fprintf(pid_f, "%d\n", current_pid);
        fclose(pid_f);
        // L4C_INFO("Server (PID: %d) wrote PID to %s", current_pid, pid_file_path); // Optional log
    } else {
        L4C_ERR("Server (PID: %d) FAILED to write PID to %s. errno: %d", current_pid, pid_file_path, errno);
        // Decide if this is a fatal error
    }
    // !--- END: Write PID to file ---

    /* Start the data mover before anything else */
    pthread_t hvac_data_mover_tid;
    if (pthread_create(&hvac_data_mover_tid, NULL, hvac_data_mover_fn, NULL) != 0){
		L4C_FATAL("Failed to initialized mecury progress thread\n");
	}

    /* True means we're a listener */
    hvac_init_comm(true);

    /* Post our address */
    hvac_comm_list_addr();

    /* Register basic RPC */
    hvac_rpc_register();
    hvac_open_rpc_register();
    hvac_close_rpc_register();
    hvac_seek_rpc_register();

    // ! HVAC TIMING
    hvac_trigger_srv_print_stats_rpc_register(); 

    while (1)
        sleep(1);

    return EXIT_SUCCESS;
}



int main(int argc, char **argv)
{
    std::signal(SIGINT,  signal_exit);  
    std::signal(SIGTERM, signal_exit);  
    std::signal(SIGKILL, signal_exit);  
    int l_error = 0;

    // Quick and dirty for prototype
    // TODO actual arg parser
    if (argc < 2)
    {
        fprintf(stderr, "Please supply server count\n");
        exit(-1);
    }

    hvac_server_count = atoi(argv[1]);

    hvac_init_logging();
    L4C_INFO("Server process starting up");
    //int *pid_server = NULL;
    //PMI_Get_rank(pid_server);
    //L4C_INFO("PMI Rank ID: %d \n", pid_server);  
    hvac_start_comm_server();
    L4C_INFO("HVAC Server process shutting down");
    return (l_error);
}
