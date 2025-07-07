#include <string>
#include <iostream>
#include <map>	
#include <unordered_map>
#include <memory>
#include <pthread.h>
#include <time.h>
#include <errno.h>

#include "hvac_timer.h"  // ! HVAC TIMING
#include "hvac_comm.h"
#include "hvac_data_mover_internal.h"

extern "C" {
#include "hvac_logging.h"
#include <fcntl.h>
#include <cassert>
#include <unistd.h>
}

/* RPC Block Constructs - Now using individual sync mechanisms */
struct hvac_sync_context {
    hg_bool_t done;
    ssize_t result;
    pthread_cond_t done_cond;
    pthread_mutex_t done_mutex;
    
    hvac_sync_context() : done(HG_FALSE), result(-1) {
        pthread_cond_init(&done_cond, NULL);
        pthread_mutex_init(&done_mutex, NULL);
    }
    
    ~hvac_sync_context() {
        pthread_cond_destroy(&done_cond);
        pthread_mutex_destroy(&done_mutex);
    }
};

/* Per-file synchronization mechanism per teacher's recommendation */
/* Each file gets its own condition variable shared by open and subsequent reads */

enum hvac_fd_state {
    HVAC_FD_OPENING = 1,    // Open RPC in progress
    HVAC_FD_READY = 2,      // Open completed, ready for I/O
    HVAC_FD_ERROR = 3       // Open failed
};

struct hvac_fd_status {
    enum hvac_fd_state state;
    pthread_mutex_t state_mutex;
    pthread_cond_t ready_cond;
    
    hvac_fd_status() : state(HVAC_FD_OPENING) {
        pthread_mutex_init(&state_mutex, NULL);
        pthread_cond_init(&ready_cond, NULL);
    }
    
    ~hvac_fd_status() {
        pthread_mutex_destroy(&state_mutex);
        pthread_cond_destroy(&ready_cond);
    }
};

/* Per-file synchronization context - shared by all operations on the same file */
struct hvac_file_sync_context {
    hg_bool_t operation_done;
    ssize_t operation_result;
    pthread_cond_t operation_cond;
    pthread_mutex_t operation_mutex;
    int ref_count;  // Reference count for cleanup
    
    hvac_file_sync_context() : operation_done(HG_FALSE), operation_result(-1), ref_count(1) {
        pthread_cond_init(&operation_cond, NULL);
        pthread_mutex_init(&operation_mutex, NULL);
    }
    
    ~hvac_file_sync_context() {
        pthread_cond_destroy(&operation_cond);
        pthread_mutex_destroy(&operation_mutex);
    }
};

// Global file descriptor state management with sharding to reduce lock contention
#define FD_STATE_SHARDS 64
static std::unordered_map<int, std::shared_ptr<hvac_fd_status>> fd_state_map;
static pthread_rwlock_t fd_state_map_rwlocks[FD_STATE_SHARDS];
static bool fd_state_rwlocks_initialized = false;

// Per-file synchronization mapping - filename to sync context
static std::unordered_map<std::string, std::shared_ptr<hvac_file_sync_context>> file_sync_map;
static pthread_rwlock_t file_sync_map_rwlock = PTHREAD_RWLOCK_INITIALIZER;

// Initialize shard rwlocks once
void init_fd_state_rwlocks() {
    if (!fd_state_rwlocks_initialized) {
        for (int i = 0; i < FD_STATE_SHARDS; i++) {
            pthread_rwlock_init(&fd_state_map_rwlocks[i], NULL);
        }
        fd_state_rwlocks_initialized = true;
    }
}

// Get appropriate rwlock for a file descriptor
pthread_rwlock_t* get_fd_rwlock(int fd) {
    init_fd_state_rwlocks();
    return &fd_state_map_rwlocks[fd % FD_STATE_SHARDS];
}

std::shared_ptr<hvac_fd_status> hvac_get_fd_status(int fd) {
    pthread_rwlock_t* rwlock = get_fd_rwlock(fd);
    pthread_rwlock_rdlock(rwlock);  // Read lock for lookup
    auto it = fd_state_map.find(fd);
    std::shared_ptr<hvac_fd_status> status = nullptr;
    if (it != fd_state_map.end()) {
        status = it->second;
    }
    pthread_rwlock_unlock(rwlock);
    return status;
}

void hvac_set_fd_opening(int fd) {
    HVAC_TIMING("HvacCommClient_(hvac_set_fd_opening)_total");
    pthread_rwlock_t* rwlock = get_fd_rwlock(fd);
    pthread_rwlock_wrlock(rwlock);  // Write lock for modification
    auto status = std::make_shared<hvac_fd_status>();
    status->state = HVAC_FD_OPENING;
    fd_state_map[fd] = status;
    pthread_rwlock_unlock(rwlock);
}

void hvac_set_fd_ready(int fd) {
    HVAC_TIMING("HvacCommClient_(hvac_set_fd_ready)_total");
    auto status = hvac_get_fd_status(fd);
    if (status) {
        pthread_mutex_lock(&status->state_mutex);
        status->state = HVAC_FD_READY;
        pthread_cond_broadcast(&status->ready_cond);  // Wake up all waiting threads
        pthread_mutex_unlock(&status->state_mutex);
    }
}

void hvac_set_fd_error(int fd) {
    auto status = hvac_get_fd_status(fd);
    if (status) {
        pthread_mutex_lock(&status->state_mutex);
        status->state = HVAC_FD_ERROR;
        pthread_cond_broadcast(&status->ready_cond);  // Wake up all waiting threads
        pthread_mutex_unlock(&status->state_mutex);
    }
}

bool hvac_wait_fd_ready(int fd, int timeout_ms = 5000) {
    HVAC_TIMING("HvacCommClient_(hvac_wait_fd_ready)_total");
    auto status = hvac_get_fd_status(fd);
    if (!status) return false;
    
    pthread_mutex_lock(&status->state_mutex);
    
    // If already ready or error, return immediately
    if (status->state == HVAC_FD_READY || status->state == HVAC_FD_ERROR) {
        bool ready = (status->state == HVAC_FD_READY);
        pthread_mutex_unlock(&status->state_mutex);
        return ready;
    }
    
    // Wait for the state to change with timeout
    struct timespec timeout_ts;
    clock_gettime(CLOCK_REALTIME, &timeout_ts);
    timeout_ts.tv_sec += timeout_ms / 1000;
    timeout_ts.tv_nsec += (timeout_ms % 1000) * 1000000;
    if (timeout_ts.tv_nsec >= 1000000000) {
        timeout_ts.tv_sec++;
        timeout_ts.tv_nsec -= 1000000000;
    }
    
    while (status->state == HVAC_FD_OPENING) {
        int ret = pthread_cond_timedwait(&status->ready_cond, &status->state_mutex, &timeout_ts);
        if (ret == ETIMEDOUT) {
            L4C_ERR("Timeout waiting for fd %d to be ready", fd);
            pthread_mutex_unlock(&status->state_mutex);
            return false;
        }
    }
    
    bool ready = (status->state == HVAC_FD_READY);
    pthread_mutex_unlock(&status->state_mutex);
    return ready;
}

void hvac_cleanup_fd_state(int fd) {
    pthread_rwlock_t* rwlock = get_fd_rwlock(fd);
    pthread_rwlock_wrlock(rwlock);  // Write lock for deletion
    fd_state_map.erase(fd);
    pthread_rwlock_unlock(rwlock);
}

/* Per-file synchronization context management functions */

// Get or create file sync context for a given filename
std::shared_ptr<hvac_file_sync_context> hvac_get_file_sync_context(const std::string& filename) {
    pthread_rwlock_rdlock(&file_sync_map_rwlock);
    auto it = file_sync_map.find(filename);
    if (it != file_sync_map.end()) {
        auto context = it->second;
        // Increment reference count
        pthread_mutex_lock(&context->operation_mutex);
        context->ref_count++;
        pthread_mutex_unlock(&context->operation_mutex);
        pthread_rwlock_unlock(&file_sync_map_rwlock);
        return context;
    }
    pthread_rwlock_unlock(&file_sync_map_rwlock);
    
    // Need to create new context
    pthread_rwlock_wrlock(&file_sync_map_rwlock);
    // Double-check in case another thread created it
    it = file_sync_map.find(filename);
    if (it != file_sync_map.end()) {
        auto context = it->second;
        pthread_mutex_lock(&context->operation_mutex);
        context->ref_count++;
        pthread_mutex_unlock(&context->operation_mutex);
        pthread_rwlock_unlock(&file_sync_map_rwlock);
        return context;
    }
    
    // Create new context
    auto context = std::make_shared<hvac_file_sync_context>();
    file_sync_map[filename] = context;
    pthread_rwlock_unlock(&file_sync_map_rwlock);
    return context;
}

// Release file sync context (decrement ref count and cleanup if needed)
void hvac_release_file_sync_context(const std::string& filename, std::shared_ptr<hvac_file_sync_context> context) {
    pthread_mutex_lock(&context->operation_mutex);
    context->ref_count--;
    bool should_cleanup = (context->ref_count <= 0);
    pthread_mutex_unlock(&context->operation_mutex);
    
    if (should_cleanup) {
        pthread_rwlock_wrlock(&file_sync_map_rwlock);
        // Double-check ref count under write lock
        pthread_mutex_lock(&context->operation_mutex);
        if (context->ref_count <= 0) {
            file_sync_map.erase(filename);
        }
        pthread_mutex_unlock(&context->operation_mutex);
        pthread_rwlock_unlock(&file_sync_map_rwlock);
    }
}

// Wait for file operation to complete using per-file sync context
ssize_t hvac_wait_for_file_operation(std::shared_ptr<hvac_file_sync_context> context, const char* operation_name) {
    pthread_mutex_lock(&context->operation_mutex);
    
    while (!context->operation_done) {
        struct timespec timeout_ts;
        clock_gettime(CLOCK_REALTIME, &timeout_ts);
        timeout_ts.tv_sec += 10;  // 10 second timeout
        
        int ret = pthread_cond_timedwait(&context->operation_cond, &context->operation_mutex, &timeout_ts);
        if (ret == ETIMEDOUT) {
            L4C_ERR("SYNC_DEBUG: Timeout waiting for %s operation", operation_name);
            pthread_mutex_unlock(&context->operation_mutex);
            return -1;
        }
    }
    
    ssize_t result = context->operation_result;
    
    // Reset for next operation
    context->operation_done = HG_FALSE;
    context->operation_result = -1;
    
    pthread_mutex_unlock(&context->operation_mutex);
    return result;
}

// Signal file operation completion
void hvac_signal_file_operation_done(std::shared_ptr<hvac_file_sync_context> context, ssize_t result) {
    pthread_mutex_lock(&context->operation_mutex);
    context->operation_done = HG_TRUE;
    context->operation_result = result;
    pthread_cond_broadcast(&context->operation_cond);
    pthread_mutex_unlock(&context->operation_mutex);
}

/* RPC Globals */
static hg_id_t hvac_client_rpc_id;
static hg_id_t hvac_client_open_id;
static hg_id_t hvac_client_close_id;
static hg_id_t hvac_client_seek_id;
static hg_id_t hvac_client_trigger_srv_print_stats_rpc_id;

/* Mercury Data Caching */
std::unordered_map<int, std::string> address_cache;
extern std::unordered_map<int, int > fd_redir_map;

extern std::unordered_map<int, std::string > fd_map;
extern "C" bool hvac_file_tracked(int fd);
extern "C" bool hvac_track_file(const char* path, int flags, int fd);

/* struct used to carry state of overall operation across callbacks */
struct hvac_rpc_state {
    uint32_t value;
    hg_size_t size;
    void *buffer;
    hg_bulk_t bulk_handle;
    hg_handle_t handle;
    std::shared_ptr<hvac_file_sync_context> file_sync_ctx;  // Per-file sync context
    std::string filename;  // For cleanup purposes
};

// Carry CB Information for CB
struct hvac_open_state{
    uint32_t local_fd;
    std::shared_ptr<hvac_file_sync_context> file_sync_ctx;  // Per-file sync context
    std::string filename;  // For cleanup purposes
};

// Seek state structure
struct hvac_seek_state{
    int fd;
    std::shared_ptr<hvac_file_sync_context> file_sync_ctx;  // Per-file sync context
    std::string filename;  // For cleanup purposes
};

static hg_return_t
hvac_seek_cb(const struct hg_cb_info *info)
{
    hvac_seek_out_t out;
    ssize_t bytes_read = -1;
    struct hvac_seek_state *seek_state = (struct hvac_seek_state *)info->arg;

    HG_Get_output(info->info.forward.handle, &out);    
    //Set the SEEK OUTPUT
    bytes_read = out.ret;
    HG_Free_output(info->info.forward.handle, &out);
    HG_Destroy(info->info.forward.handle);

    /* signal to waiting thread that we are done - using individual sync context */
    pthread_mutex_lock(&seek_state->sync_ctx->done_mutex);
    seek_state->sync_ctx->done = HG_TRUE;
    seek_state->sync_ctx->result = bytes_read;
    pthread_cond_broadcast(&seek_state->sync_ctx->done_cond);
    pthread_mutex_unlock(&seek_state->sync_ctx->done_mutex);
    
    // Don't free seek_state here - it will be freed by the waiting thread
    return HG_SUCCESS;    
}

static hg_return_t
hvac_open_cb(const struct hg_cb_info *info)
{
    HVAC_TIMING("HvacCommClient_(hvac_open_cb)_total");
    hvac_open_out_t out;
    struct hvac_open_state *open_state = (struct hvac_open_state *)info->arg;    
    assert(info->ret == HG_SUCCESS);
    HG_Get_output(info->info.forward.handle, &out);    
    
    // Update file descriptor mapping and state
    if (out.ret_status > 0) {
        fd_redir_map[open_state->local_fd] = out.ret_status;
        hvac_set_fd_ready(open_state->local_fd);  // Mark FD as ready for I/O
        L4C_INFO("Open RPC Returned FD %d - marked as ready\n", out.ret_status);
    } else {
        hvac_set_fd_error(open_state->local_fd);  // Mark FD as error
        L4C_ERR("Open RPC failed with status %d\n", out.ret_status);
    }
    
    HG_Free_output(info->info.forward.handle, &out);
    HG_Destroy(info->info.forward.handle);

    /* signal to waiting thread that we are done - using individual sync context */
    pthread_mutex_lock(&open_state->sync_ctx->done_mutex);
    open_state->sync_ctx->done = HG_TRUE;
    open_state->sync_ctx->result = out.ret_status;
    pthread_cond_broadcast(&open_state->sync_ctx->done_cond);
    pthread_mutex_unlock(&open_state->sync_ctx->done_mutex);
    
    // Don't free open_state here - it will be freed by the waiting thread
    return HG_SUCCESS;
}

/* callback triggered upon receipt of rpc response */
/* In this case there is no response since that call was response less */
static hg_return_t
hvac_read_cb(const struct hg_cb_info *info)
{
    HVAC_TIMING("HvacCommClient_(hvac_read_cb)_total");
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

    /* signal to waiting thread that we are done - using individual sync context */
    pthread_mutex_lock(&hvac_rpc_state_p->sync_ctx->done_mutex);
    hvac_rpc_state_p->sync_ctx->done = HG_TRUE;
    hvac_rpc_state_p->sync_ctx->result = bytes_read;
    pthread_cond_broadcast(&hvac_rpc_state_p->sync_ctx->done_cond);
    pthread_mutex_unlock(&hvac_rpc_state_p->sync_ctx->done_mutex);
    
    // Free the RPC state but not the sync context (caller will handle that)
    free(hvac_rpc_state_p);
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

// Updated to use individual sync context
ssize_t hvac_wait_for_operation(struct hvac_sync_context *sync_ctx, const char* operation_name)
{
    HVAC_TIMING("HvacCommClient_(hvac_wait_for_operation)_total");
    ssize_t result;
    
    /* wait for callbacks to finish using individual sync context */
        pthread_mutex_lock(&sync_ctx->done_mutex); 
    {
        HVAC_TIMING("HvacCommClient_(hvac_wait_for_operation)_wait_for_done");
        while (sync_ctx->done != HG_TRUE) {
            pthread_cond_wait(&sync_ctx->done_cond, &sync_ctx->done_mutex);
        }
    }
    result = sync_ctx->result;
    pthread_mutex_unlock(&sync_ctx->done_mutex);

    // L4C_INFO("Operation %s completed with result: %zd", operation_name, result);
    return result;
}

// Legacy function - now redirects to the new implementation
void hvac_client_block()
{
    L4C_ERR("hvac_client_block() called - this should not happen with new sync mechanism");
    assert(false);
}

// Legacy function - now redirects to the new implementation  
ssize_t hvac_read_block()
{
    L4C_ERR("hvac_read_block() called - this should not happen with new sync mechanism");
    assert(false);
    return -1;
}

// Legacy function - now redirects to the new implementation
ssize_t hvac_seek_block()
{
    L4C_ERR("hvac_seek_block() called - this should not happen with new sync mechanism");
    assert(false);
    return -1;
}

void hvac_client_comm_gen_close_rpc(uint32_t svr_hash, int fd)
{   
    HVAC_TIMING("HvacCommClient_(hvac_client_comm_gen_close_rpc)_total");
    hg_addr_t svr_addr; 
    hvac_close_in_t in;
    hg_handle_t handle; 
    int ret;

    /* Get address */
    svr_addr = hvac_client_comm_lookup_addr(svr_hash);        

    /* create create handle to represent this rpc operation */
    hvac_comm_create_handle(svr_addr, hvac_client_close_id, &handle);

    // Check if we have a valid remote FD to close
    if (fd_redir_map.find(fd) != fd_redir_map.end() && fd_redir_map[fd] != 0) {
        in.fd = fd_redir_map[fd];
        
        ret = HG_Forward(handle, NULL, NULL, &in);
        if (ret != 0) {
            L4C_ERR("Failed to send close RPC for fd %d", fd);
        }
        
        // Clean up mapping regardless of RPC success
        fd_redir_map.erase(fd);
    } else {
        L4C_WARN("No remote FD mapping found for fd %d during close", fd);
    }

    // Clean up FD state tracking
    hvac_cleanup_fd_state(fd);

    HG_Destroy(handle);
    hvac_comm_free_addr(svr_addr);

    return;
}

ssize_t hvac_client_comm_gen_open_rpc(uint32_t svr_hash, string path, int fd)
{
    // HVAC_TIMING("HvacCommClient_(hvac_client_comm_gen_open_rpc)_total");
    hg_addr_t svr_addr;
    hvac_open_in_t in;
    hg_handle_t handle;
    struct hvac_open_state *hvac_open_state_p;
    struct hvac_sync_context sync_ctx;  // Individual sync context
    int ret;
    ssize_t result = -1;

    // Initialize FD state as opening before starting RPC
    hvac_set_fd_opening(fd);

    /* Get address */
    {
        HVAC_TIMING("HvacCommClient_(hvac_client_comm_gen_open_rpc)_addr_lookup");
        svr_addr = hvac_client_comm_lookup_addr(svr_hash);
    } 

    /* Allocate args for callback pass through */
    hvac_open_state_p = (struct hvac_open_state *)malloc(sizeof(*hvac_open_state_p));
    hvac_open_state_p->local_fd = fd;
    hvac_open_state_p->sync_ctx = &sync_ctx;  // Link to our sync context

    /* create create handle to represent this rpc operation */    
    hvac_comm_create_handle(svr_addr, hvac_client_open_id, &handle);  

    in.path = (hg_string_t)malloc(strlen(path.c_str()) + 1 );
    sprintf(in.path,"%s",path.c_str());
    
    ret = HG_Forward(handle, hvac_open_cb, hvac_open_state_p, &in);
    if (ret != 0) {
        // If RPC dispatch failed, mark as error
        hvac_set_fd_error(fd);
        free(hvac_open_state_p);
        free(in.path);
        hvac_comm_free_addr(svr_addr);
        return -1;
    }

    hvac_comm_free_addr(svr_addr);

    // Wait for the operation to complete using individual sync context
    {
        HVAC_TIMING("HvacCommClient_(hvac_client_comm_gen_open_rpc)_wait_for_operation");
        result = hvac_wait_for_operation(&sync_ctx, "OPEN");
    }
    
    // Clean up resources
    free(hvac_open_state_p);
    free(in.path);
    
    return result;
}

ssize_t hvac_client_comm_gen_read_rpc(uint32_t svr_hash, int localfd, void *buffer, ssize_t count, off_t offset)
{
    HVAC_TIMING("HvacCommClient_(hvac_client_comm_gen_read_rpc)_total");
    hg_addr_t svr_addr;
    hvac_rpc_in_t in;
    const struct hg_info *hgi;
    int ret;
    struct hvac_rpc_state *hvac_rpc_state_p;
    struct hvac_sync_context sync_ctx;  // Individual sync context
    ssize_t result = -1;

    // Wait for FD to be ready before proceeding with read
    {
        HVAC_TIMING("HvacCommClient_(hvac_client_comm_gen_read_rpc)_wait_fd_ready");
        if (!hvac_wait_fd_ready(localfd)) {
        L4C_ERR("File descriptor %d not ready for read operation", localfd);
        return -1;
    }
    }

    // Double-check that we have a valid remote FD mapping
    if (fd_redir_map.find(localfd) == fd_redir_map.end() || fd_redir_map[localfd] == 0) {
        L4C_ERR("No valid remote FD mapping for local fd %d", localfd);
        return -1;
    }

    /* Get address */
    svr_addr = hvac_client_comm_lookup_addr(svr_hash);

    /* set up state structure */
    hvac_rpc_state_p = (struct hvac_rpc_state *)malloc(sizeof(*hvac_rpc_state_p));
    hvac_rpc_state_p->size = count;
    hvac_rpc_state_p->sync_ctx = &sync_ctx;  // Link to our sync context

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
    //Convert FD to remote FD - now safe since we verified it exists
    in.accessfd = fd_redir_map[localfd];
    in.offset = offset;
    
    ret = HG_Forward(hvac_rpc_state_p->handle, hvac_read_cb, hvac_rpc_state_p, &in);
    if (ret != 0) {
        // Clean up on failure
        HG_Bulk_free(hvac_rpc_state_p->bulk_handle);
        HG_Destroy(hvac_rpc_state_p->handle);
        free(hvac_rpc_state_p);
        hvac_comm_free_addr(svr_addr);
        return -1;
    }

    hvac_comm_free_addr(svr_addr);

    // Wait for the operation to complete using individual sync context
    {
        HVAC_TIMING("HvacCommClient_(hvac_client_comm_gen_read_rpc)_wait_for_operation");
        result = hvac_wait_for_operation(&sync_ctx, "READ");
    }    
    // Note: hvac_rpc_state_p is freed in the callback, sync_ctx is stack-allocated
    return result;
}

ssize_t hvac_client_comm_gen_seek_rpc(uint32_t svr_hash, int fd, int offset, int whence)
{
    hg_addr_t svr_addr;
    hvac_seek_in_t in;
    hg_handle_t handle;
    struct hvac_seek_state *seek_state_p;
    struct hvac_sync_context sync_ctx;  // Individual sync context
    int ret;
    ssize_t result = -1;

    /* Get address */
    svr_addr = hvac_client_comm_lookup_addr(svr_hash);    

    /* Allocate args for callback pass through */    
    seek_state_p = (struct hvac_seek_state *)malloc(sizeof(*seek_state_p));
    seek_state_p->fd = fd;
    seek_state_p->sync_ctx = &sync_ctx;  // Link to our sync context
    
    /* create create handle to represent this rpc operation */    
    hvac_comm_create_handle(svr_addr, hvac_client_seek_id, &handle);  

    in.fd = fd_redir_map[fd];
    in.offset = offset;
    in.whence = whence;
    
    ret = HG_Forward(handle, hvac_seek_cb, seek_state_p, &in);
    assert(ret == 0);

    hvac_comm_free_addr(svr_addr);

    // Wait for the operation to complete using individual sync context
    result = hvac_wait_for_operation(&sync_ctx, "SEEK");
    
    // Clean up resources
    free(seek_state_p);
    
    return result;
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

    // Note: This function still uses the old global sync mechanism
    // for server stats RPC - we can refactor this later if needed

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

    // Create individual sync context for this operation
    struct hvac_sync_context sync_ctx;

    // Send the RPC request (HG_Forward) - using simplified approach for stats RPC
    hg_status = HG_Forward(rpc_handle,                        // RPC handle
                           NULL,                              // No callback - fire and forget for stats
                           NULL,                              // No callback argument needed
                           &in_payload);                      // Input payload

    if (hg_status != HG_SUCCESS) {
        L4C_ERR("hvac_client_request_server_to_print_stats: HG_Forward() failed with error %d.", hg_status);
        HG_Destroy(rpc_handle);         // Clean up handle on failure
        hvac_comm_free_addr(server_address); // Free looked-up address
        return -5; // HG_Forward call failed
    }
    
    // For stats printing, we don't need to wait for response - it's fire and forget
    HG_Destroy(rpc_handle);
    operation_status = 0; // Assume success for fire-and-forget

    hvac_comm_free_addr(server_address); // Free the server address resource

    return operation_status; // Return the status set by the callback (0 for success)
}
