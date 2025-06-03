
#include <string>
#include <iostream>
#include <map>	

#include "hvac_timer.h"
#include "hvac_comm.h"
#include "hvac_data_mover_internal.h"

extern "C" {
#include "hvac_logging.h"
#include <fcntl.h>
#include <cassert>
#include <unistd.h>
}

/* RPC Block Constructs */
static hg_bool_t done = HG_FALSE;
static pthread_cond_t done_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t done_mutex = PTHREAD_MUTEX_INITIALIZER;


/* RPC Globals */
static hg_id_t hvac_client_rpc_id;
static hg_id_t hvac_client_open_id;
static hg_id_t hvac_client_close_id;
static hg_id_t hvac_client_seek_id;
static hg_id_t hvac_client_trigger_srv_print_stats_rpc_id;
ssize_t read_ret = -1;

/* Mercury Data Caching */
std::map<int, std::string> address_cache;
extern std::map<int, int > fd_redir_map;

extern std::map<int, std::string > fd_map;
extern "C" bool hvac_file_tracked(int fd);
extern "C" bool hvac_track_file(const char* path, int flags, int fd);
/* struct used to carry state of overall operation across callbacks */
struct hvac_rpc_state {
    uint32_t value;
    hg_size_t size;
    void *buffer;
    hg_bulk_t bulk_handle;
    hg_handle_t handle;
};

// Carry CB Information for CB
struct hvac_open_state{
    uint32_t local_fd;
};

static hg_return_t
hvac_seek_cb(const struct hg_cb_info *info)
{
    hvac_seek_out_t out;
    ssize_t bytes_read = -1;

    HG_Get_output(info->info.forward.handle, &out);    
    //Set the SEEK OUTPUT
    bytes_read = out.ret;
    HG_Free_output(info->info.forward.handle, &out);
    HG_Destroy(info->info.forward.handle);

    /* signal to main() that we are done */
    pthread_mutex_lock(&done_mutex);
    done++;
    read_ret = bytes_read;
    pthread_cond_signal(&done_cond);
    pthread_mutex_unlock(&done_mutex);  
    return HG_SUCCESS;    
}

static hg_return_t
hvac_open_cb(const struct hg_cb_info *info)
{
    hvac_open_out_t out;
    struct hvac_open_state *open_state = (struct hvac_open_state *)info->arg;    
    assert(info->ret == HG_SUCCESS);
    HG_Get_output(info->info.forward.handle, &out);    
    fd_redir_map[open_state->local_fd] = out.ret_status;
    L4C_INFO("Open RPC Returned FD %d\n",out.ret_status);
    HG_Free_output(info->info.forward.handle, &out);
    HG_Destroy(info->info.forward.handle);

    /* signal to main() that we are done */
    pthread_mutex_lock(&done_mutex);
    done++;
    pthread_cond_signal(&done_cond);
    pthread_mutex_unlock(&done_mutex);  
    return HG_SUCCESS;
}

/* callback triggered upon receipt of rpc response */
/* In this case there is no response since that call was response less */
static hg_return_t
hvac_read_cb(const struct hg_cb_info *info)
{
    hg_return_t ret;
    hvac_rpc_out_t out;
    ssize_t bytes_read = -1;
    struct hvac_rpc_state *hvac_rpc_state_p = (hvac_rpc_state *)info->arg;
    assert(info->ret == HG_SUCCESS);

    /* decode response */
    HG_Get_output(info->info.forward.handle, &out);
    bytes_read = out.ret;
    /* clean up resources consumed by this rpc */
    ret = HG_Bulk_free(hvac_rpc_state_p->bulk_handle);
	assert(ret == HG_SUCCESS);
	L4C_INFO("INFO: Freeing Bulk Handle"); //Does this deregister memory?

	ret = HG_Free_output(info->info.forward.handle, &out);
	assert(ret == HG_SUCCESS);
    
	ret = HG_Destroy(info->info.forward.handle);
	assert(ret == HG_SUCCESS);
    
	free(hvac_rpc_state_p);

    /* signal to main() that we are done */
    pthread_mutex_lock(&done_mutex);
    done++;
    read_ret=bytes_read;
    pthread_cond_signal(&done_cond);
    pthread_mutex_unlock(&done_mutex);  
    return HG_SUCCESS;
}

void hvac_client_comm_register_rpc()
{   
    hvac_client_open_id = hvac_open_rpc_register();
    hvac_client_rpc_id = hvac_rpc_register();    
    hvac_client_close_id = hvac_close_rpc_register();
    hvac_client_seek_id = hvac_seek_rpc_register();

    // ! TIMING for RPC in server
    hvac_client_trigger_srv_print_stats_rpc_id = hvac_trigger_srv_print_stats_rpc_register();
}

void hvac_client_block()
{
    // HVAC_TIMING("HvacCommClient_(hvac_client_block)_total");
    /* wait for callbacks to finish */
    pthread_mutex_lock(&done_mutex);
    while (done != HG_TRUE)
        pthread_cond_wait(&done_cond, &done_mutex);
    pthread_mutex_unlock(&done_mutex);
}

ssize_t hvac_read_block()
{
    // HVAC_TIMING("HvacCommClient_(hvac_read_block)_total");
    ssize_t bytes_read;
    /* wait for callbacks to finish */
    
    pthread_mutex_lock(&done_mutex);
    while (done != HG_TRUE)
        pthread_cond_wait(&done_cond, &done_mutex);
    bytes_read = read_ret;
    pthread_mutex_unlock(&done_mutex);
    return bytes_read;
}


ssize_t hvac_seek_block()
{
    size_t bytes_read;
    /* wait for callbacks to finish */
    pthread_mutex_lock(&done_mutex);
    while (done != HG_TRUE)
        pthread_cond_wait(&done_cond, &done_mutex);
    bytes_read = read_ret;
    pthread_mutex_unlock(&done_mutex);
    return bytes_read;
}


void hvac_client_comm_gen_close_rpc(uint32_t svr_hash, int fd)
{   
    hg_addr_t svr_addr; 
    hvac_close_in_t in;
    hg_handle_t handle; 
    int ret;

    /* Get address */
    svr_addr = hvac_client_comm_lookup_addr(svr_hash);        

    /* create create handle to represent this rpc operation */
    hvac_comm_create_handle(svr_addr, hvac_client_close_id, &handle);

    in.fd = fd_redir_map[fd];

    ret = HG_Forward(handle, NULL, NULL, &in);
    assert(ret == 0);

    fd_redir_map.erase(fd);

    HG_Destroy(handle);
    hvac_comm_free_addr(svr_addr);

    return;

}

void hvac_client_comm_gen_open_rpc(uint32_t svr_hash, string path, int fd)
{
    hg_addr_t svr_addr;
    hvac_open_in_t in;
    hg_handle_t handle;
    struct hvac_open_state *hvac_open_state_p;
    int ret;
    done = HG_FALSE;

    /* Get address */
    svr_addr = hvac_client_comm_lookup_addr(svr_hash);    

    /* Allocate args for callback pass through */
    hvac_open_state_p = (struct hvac_open_state *)malloc(sizeof(*hvac_open_state_p));
    hvac_open_state_p->local_fd = fd;

    /* create create handle to represent this rpc operation */    
    hvac_comm_create_handle(svr_addr, hvac_client_open_id, &handle);  

    in.path = (hg_string_t)malloc(strlen(path.c_str()) + 1 );
    sprintf(in.path,"%s",path.c_str());
    
    ret = HG_Forward(handle, hvac_open_cb, hvac_open_state_p, &in);
    assert(ret == 0);

    hvac_comm_free_addr(svr_addr);

    return;

}

void hvac_client_comm_gen_read_rpc(uint32_t svr_hash, int localfd, void *buffer, ssize_t count, off_t offset)
{
    hg_addr_t svr_addr;
    hvac_rpc_in_t in;
    const struct hg_info *hgi;
    int ret;
    struct hvac_rpc_state *hvac_rpc_state_p;
    done = HG_FALSE;
    read_ret = -1;

    /* Get address */
    svr_addr = hvac_client_comm_lookup_addr(svr_hash);

    /* set up state structure */
    hvac_rpc_state_p = (struct hvac_rpc_state *)malloc(sizeof(*hvac_rpc_state_p));
    hvac_rpc_state_p->size = count;


    /* This includes allocating a src buffer for bulk transfer */
    hvac_rpc_state_p->buffer = buffer;
    assert(hvac_rpc_state_p->buffer);
    //hvac_rpc_state_p->value = 5;

    /* create create handle to represent this rpc operation */
    hvac_comm_create_handle(svr_addr, hvac_client_rpc_id, &(hvac_rpc_state_p->handle));

    /* register buffer for rdma/bulk access by server */
    hgi = HG_Get_info(hvac_rpc_state_p->handle);
    assert(hgi);
    ret = HG_Bulk_create(hgi->hg_class, 1, (void**) &(buffer),
       &(hvac_rpc_state_p->size), HG_BULK_WRITE_ONLY, &(in.bulk_handle));

    hvac_rpc_state_p->bulk_handle = in.bulk_handle;
    assert(ret == HG_SUCCESS);

    /* Send rpc. Note that we are also transmitting the bulk handle in the
     * input struct.  It was set above.
     */
    in.input_val = count;
    //Convert FD to remote FD
    in.accessfd = fd_redir_map[localfd];
    /*
    if(in.accessfd == 0) {
	fprintf(stderr, "accessfd is 0, localfd is %d\n", localfd);
	fprintf(stderr, "input_val is %ld and size is %ld\n", count, hvac_rpc_state_p->size);
	bool tracked = hvac_file_tracked(localfd);
	fprintf(stderr, "hvac file tracked is %s\n", tracked ? "true" : "false");
	fprintf(stderr, "hvac file tracked : %s\n", fd_map[localfd].c_str());
	//while (!fd_redir_map[localfd]);
	in.accessfd = fd_redir_map[localfd];
	fprintf(stderr, "redirection map filled\n");
	}
     */    
	in.offset = offset;
    
    
    ret = HG_Forward(hvac_rpc_state_p->handle, hvac_read_cb, hvac_rpc_state_p, &in);
    assert(ret == 0);

    hvac_comm_free_addr(svr_addr);

    return;
}

void hvac_client_comm_gen_seek_rpc(uint32_t svr_hash, int fd, int offset, int whence)
{
    hg_addr_t svr_addr;
    hvac_seek_in_t in;
    hg_handle_t handle;
    read_ret = -1;
    int ret;
    done = HG_FALSE;

    /* Get address */
    svr_addr = hvac_client_comm_lookup_addr(svr_hash);    

    /* Allocate args for callback pass through */    
    /* create create handle to represent this rpc operation */    
    hvac_comm_create_handle(svr_addr, hvac_client_seek_id, &handle);  

    in.fd = fd_redir_map[fd];
    in.offset = offset;
    in.whence = whence;
    

    ret = HG_Forward(handle, hvac_seek_cb, NULL, &in);
    assert(ret == 0);

    
    hvac_comm_free_addr(svr_addr);

    return;

}


//We've converted the filename to a rank
//Using standard c++ hashing modulo servers
//Find the address
hg_addr_t hvac_client_comm_lookup_addr(int rank)
{
	L4C_INFO("AWAIS RANK %d", rank);
	if (address_cache.find(rank) != address_cache.end())
	{
        hg_addr_t target_server;
        HG_Addr_lookup2(hvac_comm_get_class(), address_cache[rank].c_str(), &target_server);
		return target_server;
	}

	/* The hardway */
	char filename[PATH_MAX];
	char svr_str[PATH_MAX];
	int svr_rank = -1;
	char *jobid = getenv("SLURM_JOBID");
	hg_addr_t target_server = nullptr;
	bool svr_found = false;
	FILE *na_config = NULL;
	sprintf(filename, "./.ports.cfg.%s", jobid);
	na_config = fopen(filename,"r+");
    

	while (fscanf(na_config, "%d %s\n",&svr_rank, svr_str) == 2)
	{
		if (svr_rank == rank){
			L4C_INFO("Connecting to %s %d\n", svr_str, svr_rank);            
			svr_found = true;
            break;
		}
	}

	if (svr_found){
		//Do something
        address_cache[rank] = svr_str;
        HG_Addr_lookup2(hvac_comm_get_class(),svr_str,&target_server);		
	}

	return target_server;
}

// Callback for the client after the server responds to the print stats request
static hg_return_t
hvac_client_srv_print_stats_cb(const struct hg_cb_info *callback_info) {
    // This callback is executed when the client receives the response from the server
    // after requesting the server to print its stats.
    hvac_rpc_trigger_srv_print_stats_out_t out_struct;
    int* client_side_status_flag = (int*)callback_info->arg; // User argument to store status

    if (callback_info->ret != HG_SUCCESS) {
        L4C_ERR("HvacCommClient_SrvPrintStatsCb: RPC request failed (transport error %d).", callback_info->ret);
        if (client_side_status_flag) *client_side_status_flag = -2; // Indicate transport or Mercury error
    } else {
        // Get the output from the server's response
        HG_Get_output(callback_info->info.forward.handle, &out_struct);
        if (out_struct.status == 0) {
            L4C_INFO("HvacCommClient_SrvPrintStatsCb: Server acknowledged print stats request successfully.");
            if (client_side_status_flag) *client_side_status_flag = 0; // Success
        } else {
            L4C_ERR("HvacCommClient_SrvPrintStatsCb: Server indicated failure (status %d) for print stats request.", out_struct.status);
            if (client_side_status_flag) *client_side_status_flag = -1; // Server-side error
        }
        HG_Free_output(callback_info->info.forward.handle, &out_struct); // Free the output struct
    }

    HG_Destroy(callback_info->info.forward.handle); // Destroy the client's RPC handle

    // Signal the calling thread that the RPC and callback are complete
    pthread_mutex_lock(&done_mutex);
    done = HG_TRUE;
    pthread_cond_signal(&done_cond);
    pthread_mutex_unlock(&done_mutex);

    return HG_SUCCESS;
}

// C function callable from Python via ctypes
extern "C" int hvac_client_request_server_to_print_stats(const char* server_rank_identifier) {
    // This function sends an RPC to the specified server, asking it to print its timing statistics.

    if (!server_rank_identifier) {
        L4C_ERR("hvac_client_request_server_to_print_stats: server_rank_identifier is NULL.");
        return -3; // Invalid argument error code
    }

    int server_rank_int = atoi(server_rank_identifier); // Convert string rank to int
    hg_addr_t server_address = hvac_client_comm_lookup_addr(server_rank_int); // Get server's Mercury address

    if (server_address == HG_ADDR_NULL) {
        L4C_ERR("hvac_client_request_server_to_print_stats: Could not find address for server rank %d.", server_rank_int);
        return -4; // Server not found error code
    }

    hvac_rpc_trigger_srv_print_stats_in_t in_payload;
    in_payload.dummy_arg = 0; // Set the dummy argument

    hg_handle_t rpc_handle;
    hg_return_t hg_status;
    int operation_status = -10; // Default status: uninitialized/failed to complete

    // Create an RPC handle
    hvac_comm_create_handle(server_address, hvac_client_trigger_srv_print_stats_rpc_id, &rpc_handle);

    done = HG_FALSE; // Reset the global 'done' flag for this blocking call

    // Send the RPC request (HG_Forward)
    hg_status = HG_Forward(rpc_handle,                        // RPC handle
                           hvac_client_srv_print_stats_cb,    // Callback function
                           &operation_status,                 // Argument for the callback (pointer to status flag)
                           &in_payload);                      // Input payload

    if (hg_status != HG_SUCCESS) {
        L4C_ERR("hvac_client_request_server_to_print_stats: HG_Forward() failed with error %d.", hg_status);
        HG_Destroy(rpc_handle);         // Clean up handle on failure
        hvac_comm_free_addr(server_address); // Free looked-up address
        return -5; // HG_Forward call failed
    }

    // Wait for the callback to signal completion (blocking call)
    pthread_mutex_lock(&done_mutex);
    while (done != HG_TRUE) {
        pthread_cond_wait(&done_cond, &done_mutex);
    }
    pthread_mutex_unlock(&done_mutex);

    hvac_comm_free_addr(server_address); // Free the server address resource

    return operation_status; // Return the status set by the callback (0 for success)
}
