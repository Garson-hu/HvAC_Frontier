#include "hvac_comm.h"
#include "hvac_data_mover_internal.h"
#include "hvac_timer.h"

extern "C" {
#include "hvac_logging.h"
#include <fcntl.h>
#include <cassert>
//#include <pmi.h>
#include <unistd.h>
}


#include <string>
#include <iostream>
#include <map>	


static hg_class_t *hg_class = NULL;
static hg_context_t *hg_context = NULL;
static int hvac_progress_thread_shutdown_flags = 0;
static int hvac_server_rank = -1;
static int server_rank = -1;

/* struct used to carry state of overall operation across callbacks */
struct hvac_rpc_state {
    hg_size_t size;
    void *buffer;
    hg_bulk_t bulk_handle;
    hg_handle_t handle;
    hvac_rpc_in_t in;
};


void hvac_init_comm(hg_bool_t listen)
{

	const char *info_string = "ofi+tcp;ofi_rxm://";  
    /* 	int *pid_server = NULL;
        PMI_Get_rank(pid_server);
        L4C_INFO("PMI Rank ID: %d \n", pid_server);  
            
        std::string rankstr_str = std::to_string(*pid_server);

        // Set the environment variable
        if (setenv("PMIX_RANK", rankstr_str.c_str(), 1) != 0) 
        {
        L4C_INFO("Exported PMIX_RANK: %s \n", rankstr_str.c_str());
        }
    */
    char *rank_str = getenv("SLURM_PROCID"); // "0";
	server_rank = atoi(rank_str);
    //	L4C_INFO("PMIX_RANK: %s Server Rank: %d \n", rankstr_str.c_str(), server_rank);
	L4C_INFO("Server Rank: %d \n", server_rank);

	pthread_t hvac_progress_tid;

    HG_Set_log_level("DEBUG");

    /* Initialize Mercury with the desired network abstraction class */
    hg_class = HG_Init(info_string, listen);
	if (hg_class == NULL){
		L4C_FATAL("Failed to initialize HG_CLASS Listen Mode : %d : PMI_RANK %d \n", listen, server_rank);
	}

    /* Create HG context */
    hg_context = HG_Context_create(hg_class);
	if (hg_context == NULL){
		L4C_FATAL("Failed to initialize HG_CONTEXT\n");
	}
	//Only for server processes
	if (listen)
	{
		if (rank_str != NULL){
			hvac_server_rank = atoi(rank_str);
		}else
		{
			L4C_FATAL("Failed to extract rank\n");
		}
	}

	L4C_INFO("Mecury initialized");
//	free(rank_str);
	//free(pid_server);
	//TODO The engine creates a pthread here to do the listening and progress work
	//I need to understand this better I don't want to create unecessary work for the client
	if (pthread_create(&hvac_progress_tid, NULL, hvac_progress_fn, NULL) != 0){
		L4C_FATAL("Failed to initialized mecury progress thread\n");
	}

}

void hvac_shutdown_comm()
{
    hvac_progress_thread_shutdown_flags = true;

    if (hg_context == NULL)

	return;
}

void *hvac_progress_fn(void *args)
{
	hg_return_t ret;
	unsigned int actual_count = 0;
	while (!hvac_progress_thread_shutdown_flags){
		do{
			ret = HG_Trigger(hg_context, 0, 1, &actual_count);
		} while (
			(ret == HG_SUCCESS) && actual_count && !hvac_progress_thread_shutdown_flags);
		if (!hvac_progress_thread_shutdown_flags)
			HG_Progress(hg_context, 100);
	}
	
	return NULL;
}

/* I think only servers need to post their addresses. */
/* There is an expectation that the server will be started in 
 * advance of the clients. Should the servers be started with an
 * argument regarding the number of servers? */
void hvac_comm_list_addr()
{
	char self_addr_string[PATH_MAX];
	char filename[PATH_MAX];
        hg_addr_t self_addr;
	FILE *na_config = NULL;
	hg_size_t self_addr_string_size = PATH_MAX;
//	char *stepid = getenv("PMIX_NAMESPACE");
	char *jobid =  getenv("SLURM_JOBID");
        L4C_INFO("JOB_ID: %s\n", jobid); 
	sprintf(filename, "./.ports.cfg.%s", jobid);
	/* Get self addr to tell client about */
    	HG_Addr_self(hg_class, &self_addr);
    	HG_Addr_to_string(
        hg_class, self_addr_string, &self_addr_string_size, self_addr);
    	HG_Addr_free(hg_class, self_addr);
    

    /* Write addr to a file */
    na_config = fopen(filename, "a+");
    if (!na_config) {
        L4C_ERR("Could not open config file from: %s\n",
            filename);
        exit(0);
    }
    fprintf(na_config, "%d %s\n", hvac_server_rank, self_addr_string);
    fclose(na_config);
}



/* callback triggered upon completion of bulk transfer */
static hg_return_t
hvac_rpc_handler_bulk_cb(const struct hg_cb_info *info)
{
    struct hvac_rpc_state *hvac_rpc_state_p = (struct hvac_rpc_state*)info->arg;
    int ret;
    hvac_rpc_out_t out;
    out.ret = hvac_rpc_state_p->size;

    assert(info->ret == 0);

    ret = HG_Respond(hvac_rpc_state_p->handle, NULL, NULL, &out);
    assert(ret == HG_SUCCESS);        

    HG_Bulk_free(hvac_rpc_state_p->bulk_handle);
    L4C_INFO("Info Server: Freeing Bulk Handle\n");
    HG_Destroy(hvac_rpc_state_p->handle);
    free(hvac_rpc_state_p->buffer);
    free(hvac_rpc_state_p);
    return (hg_return_t)0;
}



static hg_return_t
hvac_rpc_handler(hg_handle_t handle)
{
    int ret;
    struct hvac_rpc_state *hvac_rpc_state_p;
    const struct hg_info *hgi;
    ssize_t readbytes;

    hvac_rpc_state_p = (struct hvac_rpc_state*)malloc(sizeof(*hvac_rpc_state_p));

    /* decode input */
    HG_Get_input(handle, &hvac_rpc_state_p->in);   
    
    /* This includes allocating a target buffer for bulk transfer */
    hvac_rpc_state_p->buffer = calloc(1, hvac_rpc_state_p->in.input_val);
    assert(hvac_rpc_state_p->buffer);

    hvac_rpc_state_p->size = hvac_rpc_state_p->in.input_val;
    hvac_rpc_state_p->handle = handle;



    /* register local target buffer for bulk access */

    hgi = HG_Get_info(handle);
    assert(hgi);
    ret = HG_Bulk_create(hgi->hg_class, 1, &hvac_rpc_state_p->buffer,
        &hvac_rpc_state_p->size, HG_BULK_READ_ONLY,
        &hvac_rpc_state_p->bulk_handle);
    assert(ret == 0);

    if (hvac_rpc_state_p->in.offset == -1){
        readbytes = read(hvac_rpc_state_p->in.accessfd, hvac_rpc_state_p->buffer, hvac_rpc_state_p->size);
        L4C_DEBUG("Server Rank %d : Read %ld bytes from file %s", server_rank,readbytes, fd_to_path[hvac_rpc_state_p->in.accessfd].c_str());
    }else
    {
        readbytes = pread(hvac_rpc_state_p->in.accessfd, hvac_rpc_state_p->buffer, hvac_rpc_state_p->size, hvac_rpc_state_p->in.offset);
        L4C_DEBUG("Server Rank %d : PRead %ld bytes from file %s at offset %ld", server_rank,readbytes, fd_to_path[hvac_rpc_state_p->in.accessfd].c_str(),hvac_rpc_state_p->in.offset );
    }

    //Reduce size of transfer to what was actually read 
    //We may need to revisit this.
    hvac_rpc_state_p->size = readbytes;

    /* initiate bulk transfer from client to server */
    ret = HG_Bulk_transfer(hgi->context, hvac_rpc_handler_bulk_cb, hvac_rpc_state_p,
        HG_BULK_PUSH, hgi->addr, hvac_rpc_state_p->in.bulk_handle, 0,
        hvac_rpc_state_p->bulk_handle, 0, hvac_rpc_state_p->size, HG_OP_ID_IGNORE);
    
    assert(ret == 0);
    (void) ret;

    return (hg_return_t)ret;
}




static hg_return_t
hvac_open_rpc_handler(hg_handle_t handle)
{
    hvac_open_in_t in;
    hvac_open_out_t out;    
    int ret = HG_Get_input(handle, &in);
    assert(ret == 0);
    string redir_path = in.path;
    if (path_cache_map.find(redir_path) != path_cache_map.end())
    {
        L4C_INFO("Server Rank %d : Successful Redirection %s to %s", server_rank, redir_path.c_str(), path_cache_map[redir_path].c_str());
        redir_path = path_cache_map[redir_path];
    }
    L4C_INFO("Server Rank %d : Successful Open %s", server_rank, in.path);    
    out.ret_status = open(redir_path.c_str(),O_RDONLY);  
    fd_to_path[out.ret_status] = in.path;  
    HG_Respond(handle,NULL,NULL,&out);

    return (hg_return_t)ret;

}

static hg_return_t
hvac_close_rpc_handler(hg_handle_t handle)
{
    hvac_close_in_t in;
    int ret = HG_Get_input(handle, &in);
    assert(ret == HG_SUCCESS);

    L4C_INFO("Closing File %d\n",in.fd);
    ret = close(in.fd);
    assert(ret == 0);

    //Signal to the data mover to copy the file
    if (path_cache_map.find(fd_to_path[in.fd]) == path_cache_map.end())
    {
        L4C_INFO("Caching %s",fd_to_path[in.fd].c_str());
        pthread_mutex_lock(&data_mutex);
        pthread_cond_signal(&data_cond);
        data_queue.push(fd_to_path[in.fd]);
        pthread_mutex_unlock(&data_mutex);
    }

	fd_to_path.erase(in.fd);
    return (hg_return_t)ret;
}

static hg_return_t
hvac_seek_rpc_handler(hg_handle_t handle)
{
    hvac_seek_in_t in;
    hvac_seek_out_t out;    
    int ret = HG_Get_input(handle, &in);
    assert(ret == 0);

    out.ret = lseek64(in.fd, in.offset, in.whence);

    HG_Respond(handle,NULL,NULL,&out);

    return (hg_return_t)ret;
}


/* register this particular rpc type with Mercury */
hg_id_t
hvac_rpc_register(void)
{
    hg_id_t tmp;

    tmp = MERCURY_REGISTER(
        hg_class, "hvac_base_rpc", hvac_rpc_in_t, hvac_rpc_out_t, hvac_rpc_handler);

    return tmp;
}

hg_id_t
hvac_open_rpc_register(void)
{
    hg_id_t tmp;

    tmp = MERCURY_REGISTER(
        hg_class, "hvac_open_rpc", hvac_open_in_t, hvac_open_out_t, hvac_open_rpc_handler);

    return tmp;
}

hg_id_t
hvac_close_rpc_register(void)
{
    hg_id_t tmp;

    tmp = MERCURY_REGISTER(
        hg_class, "hvac_close_rpc", hvac_close_in_t, void, hvac_close_rpc_handler);
    

    int ret =  HG_Registered_disable_response(hg_class, tmp,
                                           HG_TRUE);                        
    assert(ret == HG_SUCCESS);

    return tmp;
}

/* register this particular rpc type with Mercury */
hg_id_t
hvac_seek_rpc_register(void)
{
    hg_id_t tmp;

    tmp = MERCURY_REGISTER(
        hg_class, "hvac_seek_rpc", hvac_seek_in_t, hvac_seek_out_t, hvac_seek_rpc_handler);

    return tmp;
}

/* Create context even for client */
void
hvac_comm_create_handle(hg_addr_t addr, hg_id_t id, hg_handle_t *handle)
{    
    hg_return_t ret = HG_Create(hg_context, addr, id, handle);

    assert(ret==HG_SUCCESS);    
}

/*Free the addr */
void 
hvac_comm_free_addr(hg_addr_t addr)
{
    hg_return_t ret = HG_Addr_free(hg_class,addr);
    assert(ret==HG_SUCCESS);
}

hg_class_t *hvac_comm_get_class()
{
    return hg_class;
}

hg_context_t *hvac_comm_get_context()
{
    return hg_context;
}


// --- RPC Handler for Server-side Print Stats ---
static hg_return_t
hvac_trigger_srv_print_stats_handler(hg_handle_t handle) {
    // This handler is for the RPC that requests the server to print its stats.

    hvac_rpc_trigger_srv_print_stats_in_t in_struct; // Input struct
    hvac_rpc_trigger_srv_print_stats_out_t out_struct;
    hg_return_t hg_status = HG_SUCCESS;

    // Get input (even if it's just a dummy argument, this step is usually needed)
    hg_status = HG_Get_input(handle, &in_struct);
    if (hg_status != HG_SUCCESS) {
        L4C_ERR("HvacComm_Handle_SrvPrintStatsRPC: HG_Get_input() failed with %d", hg_status);
        out_struct.status = -1; // Indicate error
        // Still attempt to respond
    } else {
        L4C_INFO("HvacComm_Handle_SrvPrintStatsRPC: Request received. Printing server-side timing stats.");
        hvac::print_all_stats(); // THE ACTUAL ACTION: Call the existing print function
        out_struct.status = 0;   // Indicate success
    }

    // Send response back to the client
    hg_status = HG_Respond(handle, NULL, NULL, &out_struct);
    if (hg_status != HG_SUCCESS) {
        L4C_ERR("HvacComm_Handle_SrvPrintStatsRPC: HG_Respond() failed with %d", hg_status);
    }

    // Clean up resources
    HG_Free_input(handle, &in_struct); // Free input struct resources if any were allocated by Mercury
    // HG_Destroy(handle); // Manage handle lifecycle. For synchronous server handlers,
                          // destroying after respond is common, but verify with Mercury docs.

    return hg_status; // Return status of RPC processing
}

// --- RPC Registration Function (Server-side) ---
hg_id_t
hvac_trigger_srv_print_stats_rpc_register(void) {
    // This function registers the RPC that allows clients to tell the server to print its stats.
    hg_id_t rpc_id;
    rpc_id = MERCURY_REGISTER(
        hg_class,                                 // Mercury class (should be initialized and accessible)
        "hvac_rpc_trigger_srv_print_stats",       // Unique RPC name string
        hvac_rpc_trigger_srv_print_stats_in_t,    // Input struct type
        hvac_rpc_trigger_srv_print_stats_out_t,   // Output struct type
        hvac_trigger_srv_print_stats_handler      // Handler function for this RPC
    );
    L4C_INFO("HvacComm: Registered RPC 'hvac_rpc_trigger_srv_print_stats' with ID: %u", rpc_id);
    return rpc_id;
}