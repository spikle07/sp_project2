#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


// add your code here ...
// Helper function to find the next newline character
static off_t find_next_newline(int fd, off_t start_pos, off_t max_pos) {
    char buffer[1024];
    off_t current_pos = start_pos;
    
    // Seek to the start position
    lseek(fd, start_pos, SEEK_SET);
    
    while (current_pos < max_pos) {
        size_t to_read = sizeof(buffer);
        if (current_pos + to_read > max_pos) {
            to_read = max_pos - current_pos;
        }
        
        ssize_t bytes_read = read(fd, buffer, to_read);
        if (bytes_read <= 0) break;
        
        for (ssize_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                return current_pos + i + 1;
            }
        }
        current_pos += bytes_read;
    }
    return max_pos;
}

static void get_split_positions(int fd, off_t total_size, int split_num, off_t *split_starts, off_t *split_sizes) {
    // First split always starts at beginning
    split_starts[0] = 0;
    
    // Calculate approximate split size
    off_t base_split_size = total_size / split_num;
    
    for (int i = 1; i < split_num; i++) {
        // Calculate target split point
        off_t target_pos = i * base_split_size;
        
        // Find next newline after target position
        split_starts[i] = find_next_newline(fd, target_pos, total_size);
        
        // Calculate size of previous split
        split_sizes[i-1] = split_starts[i] - split_starts[i-1];
    }
    
    // Calculate size of last split
    split_sizes[split_num-1] = total_size - split_starts[split_num-1];
    
    // Reset file position
    lseek(fd, 0, SEEK_SET);

    #ifdef DEBUG
    for (int i = 0; i < split_num; i++) {
        DEBUG_MSG("Split %d: start=%ld, size=%ld\n", i, (long)split_starts[i], (long)split_sizes[i]);
    }
    #endif
}

void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    // add you code here ...
    struct timeval start, end;
    int input_fd;
    off_t file_size;
    off_t *split_starts, *split_sizes;
    int *intermediate_fds;

    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }
    
    gettimeofday(&start, NULL);

    // add your code here ...
    input_fd = open(spec->input_data_filepath, O_RDONLY);
    if (input_fd < 0) {
        EXIT_ERROR(ERROR, "Cannot open input file: %s\n", spec->input_data_filepath);
    }
    
    file_size = lseek(input_fd, 0, SEEK_END);
    if (file_size <= 0) {
        close(input_fd);
        EXIT_ERROR(ERROR, "Empty or invalid input file\n");
    }
    lseek(input_fd, 0, SEEK_SET);
    
    int actual_split_num = (file_size < spec->split_num) ? 1 : spec->split_num;
    
    split_starts = malloc(actual_split_num * sizeof(off_t));
    split_sizes = malloc(actual_split_num * sizeof(off_t));
    intermediate_fds = malloc(actual_split_num * sizeof(int));
    
    if (!split_starts || !split_sizes || !intermediate_fds) {
        close(input_fd);
        free(split_starts);
        free(split_sizes);
        free(intermediate_fds);
        EXIT_ERROR(ERROR, "Memory allocation failed\n");
    }
    
    get_split_positions(input_fd, file_size, actual_split_num, split_starts, split_sizes);
    
    for (int i = 0; i < actual_split_num; i++) {
        char intermediate_filename[32];
        snprintf(intermediate_filename, sizeof(intermediate_filename), "mr-%d.itm", i);
        
        intermediate_fds[i] = open(intermediate_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (intermediate_fds[i] < 0) {
            EXIT_ERROR(ERROR, "Cannot create intermediate file: %s\n", intermediate_filename);
        }
        
        pid_t pid = fork();
        if (pid < 0) {
            EXIT_ERROR(ERROR, "Fork failed for map worker %d\n", i);
        } 
        else if (pid == 0) {  // Child process (map worker)
            int worker_fd = open(spec->input_data_filepath, O_RDONLY);
            if (worker_fd < 0) {
                _EXIT_ERROR(ERROR, "Worker cannot open input file\n");
            }
            
            close(input_fd);
            for (int j = 0; j < i; j++) {
                close(intermediate_fds[j]);
            }
            
            DATA_SPLIT split;
            split.fd = worker_fd;
            split.size = split_sizes[i];
            split.usr_data = spec->usr_data;
            
            if (lseek(worker_fd, split_starts[i], SEEK_SET) < 0) {
                _EXIT_ERROR(ERROR, "Worker seek failed\n");
            }
            
            int ret = spec->map_func(&split, intermediate_fds[i]);
            
            close(worker_fd);
            close(intermediate_fds[i]);
            
            _exit(ret == 0 ? 0 : 1);  // Changed: Convert -1 to exit status 1
        }
        else {  // Parent process
            result->map_worker_pid[i] = pid;
        }
    }
    
    // Wait for map workers
    for (int i = 0; i < actual_split_num; i++) {
        int status;
        waitpid(result->map_worker_pid[i], &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {  // Changed: Check for 0 instead of SUCCESS
            EXIT_ERROR(ERROR, "Map worker %d failed\n", i);
        }
        close(intermediate_fds[i]);
    }
    
    // Fork reduce worker
    pid_t reduce_pid = fork();
    if (reduce_pid < 0) {
        EXIT_ERROR(ERROR, "Fork failed for reduce worker\n");
    }
    else if (reduce_pid == 0) {  // Reduce worker
        int result_fd = open(result->filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (result_fd < 0) {
            _EXIT_ERROR(ERROR, "Cannot create result file\n");
        }
        
        for (int i = 0; i < actual_split_num; i++) {
            char intermediate_filename[32];
            snprintf(intermediate_filename, sizeof(intermediate_filename), "mr-%d.itm", i);
            intermediate_fds[i] = open(intermediate_filename, O_RDONLY);
            if (intermediate_fds[i] < 0) {
                _EXIT_ERROR(ERROR, "Cannot open intermediate file for reading\n");
            }
        }
        
        int ret = spec->reduce_func(intermediate_fds, actual_split_num, result_fd);
        
        close(result_fd);
        for (int i = 0; i < actual_split_num; i++) {
            close(intermediate_fds[i]);
        }
        
        _exit(ret == 0 ? 0 : 1);  // Changed: Convert -1 to exit status 1
    }
    else {  // Parent process
        result->reduce_worker_pid = reduce_pid;
        
        int status;
        waitpid(reduce_pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {  // Changed: Check for 0 instead of SUCCESS
            EXIT_ERROR(ERROR, "Reduce worker failed\n");
        }
    }
    
    close(input_fd);
    free(split_starts);
    free(split_sizes);
    free(intermediate_fds);
    

    gettimeofday(&end, NULL);   

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
