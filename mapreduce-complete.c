#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "mapreduce.h"
#include "common.h"

// Helper function to calculate split sizes with line boundaries
static void get_split_positions(int fd, int total_size, int split_num, off_t *split_starts, off_t *split_sizes) {
    char buffer[1024];
    int base_size = total_size / split_num;
    off_t current_pos = 0;
    
    // Initialize first split start
    split_starts[0] = 0;
    
    for (int i = 1; i < split_num; i++) {
        // Calculate target position for this split
        off_t target_pos = i * base_size;
        
        // Seek to approximate position
        lseek(fd, target_pos, SEEK_SET);
        
        // Read a chunk of data
        int bytes_read = read(fd, buffer, sizeof(buffer));
        if (bytes_read <= 0) continue;
        
        // Find next newline
        int offset = 0;
        while (offset < bytes_read && buffer[offset] != '\n') {
            offset++;
        }
        
        // Set split position
        split_starts[i] = target_pos + offset + 1;
    }
    
    // Calculate split sizes
    for (int i = 0; i < split_num - 1; i++) {
        split_sizes[i] = split_starts[i + 1] - split_starts[i];
    }
    // Last split size is from last start to end of file
    split_sizes[split_num - 1] = total_size - split_starts[split_num - 1];
}

void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result) {
    struct timeval start, end;
    int input_fd;
    off_t file_size;
    off_t *split_starts, *split_sizes;
    int *intermediate_fds;
    
    if (NULL == spec || NULL == result) {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }
    
    gettimeofday(&start, NULL);
    
    // Open input file
    input_fd = open(spec->input_data_filepath, O_RDONLY);
    if (input_fd < 0) {
        EXIT_ERROR(ERROR, "Cannot open input file: %s\n", spec->input_data_filepath);
    }
    
    // Get file size
    file_size = lseek(input_fd, 0, SEEK_END);
    lseek(input_fd, 0, SEEK_SET);
    
    // Allocate arrays for split information
    split_starts = malloc(spec->split_num * sizeof(off_t));
    split_sizes = malloc(spec->split_num * sizeof(off_t));
    intermediate_fds = malloc(spec->split_num * sizeof(int));
    
    if (!split_starts || !split_sizes || !intermediate_fds) {
        EXIT_ERROR(ERROR, "Memory allocation failed\n");
    }
    
    // Calculate split positions
    get_split_positions(input_fd, file_size, spec->split_num, split_starts, split_sizes);
    
    // Create intermediate files and fork map workers
    for (int i = 0; i < spec->split_num; i++) {
        // Create intermediate file
        char intermediate_filename[32];
        snprintf(intermediate_filename, sizeof(intermediate_filename), "mr-%d.itm", i);
        
        intermediate_fds[i] = open(intermediate_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (intermediate_fds[i] < 0) {
            EXIT_ERROR(ERROR, "Cannot create intermediate file: %s\n", intermediate_filename);
        }
        
        // Fork map worker
        pid_t pid = fork();
        if (pid < 0) {
            EXIT_ERROR(ERROR, "Fork failed for map worker %d\n", i);
        } 
        else if (pid == 0) {  // Child process (map worker)
            // Close other intermediate files
            for (int j = 0; j < i; j++) {
                close(intermediate_fds[j]);
            }
            
            // Setup split info
            DATA_SPLIT split;
            split.fd = input_fd;
            split.size = split_sizes[i];
            split.usr_data = spec->usr_data;
            
            // Seek to correct position
            lseek(input_fd, split_starts[i], SEEK_SET);
            
            // Run map function
            if (spec->map_func(&split, intermediate_fds[i]) != SUCCESS) {
                _EXIT_ERROR(ERROR, "Map function failed for worker %d\n", i);
            }
            
            close(intermediate_fds[i]);
            close(input_fd);
            _exit(SUCCESS);
        }
        else {  // Parent process
            result->map_worker_pid[i] = pid;
        }
    }
    
    // Wait for all map workers
    for (int i = 0; i < spec->split_num; i++) {
        int status;
        waitpid(result->map_worker_pid[i], &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != SUCCESS) {
            EXIT_ERROR(ERROR, "Map worker %d failed\n", i);
        }
        close(intermediate_fds[i]);  // Close write end of intermediate files
    }
    
    // Fork reduce worker (using first map worker's PID)
    pid_t reduce_pid = fork();
    if (reduce_pid < 0) {
        EXIT_ERROR(ERROR, "Fork failed for reduce worker\n");
    }
    else if (reduce_pid == 0) {  // Child process (reduce worker)
        // Open result file
        int result_fd = open(result->filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (result_fd < 0) {
            _EXIT_ERROR(ERROR, "Cannot create result file: %s\n", result->filepath);
        }
        
        // Open intermediate files for reading
        for (int i = 0; i < spec->split_num; i++) {
            char intermediate_filename[32];
            snprintf(intermediate_filename, sizeof(intermediate_filename), "mr-%d.itm", i);
            intermediate_fds[i] = open(intermediate_filename, O_RDONLY);
            if (intermediate_fds[i] < 0) {
                _EXIT_ERROR(ERROR, "Cannot open intermediate file for reading: %s\n", intermediate_filename);
            }
        }
        
        // Run reduce function
        if (spec->reduce_func(intermediate_fds, spec->split_num, result_fd) != SUCCESS) {
            _EXIT_ERROR(ERROR, "Reduce function failed\n");
        }
        
        // Cleanup
        close(result_fd);
        for (int i = 0; i < spec->split_num; i++) {
            close(intermediate_fds[i]);
        }
        
        _exit(SUCCESS);
    }
    else {  // Parent process
        result->reduce_worker_pid = reduce_pid;
        
        // Wait for reduce worker
        int status;
        waitpid(reduce_pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != SUCCESS) {
            EXIT_ERROR(ERROR, "Reduce worker failed\n");
        }
    }
    
    // Cleanup
    close(input_fd);
    free(split_starts);
    free(split_sizes);
    free(intermediate_fds);
    
    gettimeofday(&end, NULL);
    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
