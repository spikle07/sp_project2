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

/*helper function to find the next newline character in a file
 this ensures that splits occur at line boundaries to maintain data integrity
 */

static off_t find_next_newline(int fd, off_t start_pos, off_t max_pos) {
    char buffer[1024];
    off_t current_pos = start_pos;
    
    lseek(fd, start_pos, SEEK_SET);
    
    while (current_pos < max_pos) {
        // calculate bytes to be read
        size_t to_read = sizeof(buffer);
        if (current_pos + to_read > max_pos) {
            to_read = max_pos - current_pos;
        }

        ssize_t bytes_read = read(fd, buffer, to_read);
        if (bytes_read <= 0) break;  // Exit if we can't read more or reach EOF
        for (ssize_t i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n') {
                return current_pos + i + 1;  
            }
        }
        current_pos += bytes_read;
    }
    return max_pos;  // If no newline found, return the maximum position
}

/*
Calculate the start positions and sizes for each split of the input file
ensures even distribution of data among map workers
*/ 
static void get_split_positions(int fd, off_t total_size, int split_num, off_t *split_starts, off_t *split_sizes) {
    split_starts[0] = 0;
    
    off_t base_split_size = total_size / split_num;
    
    // spilt position calculation
    for (int i = 1; i < split_num; i++) {
        // Calculate target split point
        off_t target_pos = i * base_split_size;
        split_starts[i] = find_next_newline(fd, target_pos, total_size);     
        split_sizes[i-1] = split_starts[i] - split_starts[i-1];  //  size of previous split
    }

    split_sizes[split_num-1] = total_size - split_starts[split_num-1];
    lseek(fd, 0, SEEK_SET);
    #ifdef DEBUG
    for (int i = 0; i < split_num; i++) {
        DEBUG_MSG("Split %d: start=%ld, size=%ld\n", i, (long)split_starts[i], (long)split_sizes[i]);
    }
    #endif
}

// Main MapReduce function that coordinates the entire process
void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    struct timeval start, end;  //  measuring processing time
    int input_fd;              // File descriptor for input file
    off_t file_size;          // Size of input file
    off_t *split_starts;      // Array to store starting positions of splits
    off_t *split_sizes;       // Array to store sizes of splits
    int *intermediate_fds;    // Array of file descriptors for intermediate files

    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }
  
    gettimeofday(&start, NULL);

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
    
    // Check if memory allocation was successful
    if (!split_starts || !split_sizes || !intermediate_fds) {
        close(input_fd);
        free(split_starts);
        free(split_sizes);
        free(intermediate_fds);
        EXIT_ERROR(ERROR, "Memory allocation failed\n");
    }
    
    // Calculate split positions for the input file
    get_split_positions(input_fd, file_size, actual_split_num, split_starts, split_sizes);
    
    // Create and launch map workers
    for (int i = 0; i < actual_split_num; i++) {
        // intermidiate file for word counter
        char intermediate_filename[32];
        snprintf(intermediate_filename, sizeof(intermediate_filename), "mr-%d.itm", i);
        
        // open file
        intermediate_fds[i] = open(intermediate_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (intermediate_fds[i] < 0) {
            EXIT_ERROR(ERROR, "Cannot create intermediate file: %s\n", intermediate_filename);
        }
        
        //fork
        pid_t pid = fork();
        if (pid < 0) {
            EXIT_ERROR(ERROR, "Fork failed for map worker %d\n", i);
        } 
        else if (pid == 0) {  // Child process (map worker)
            // Open a new file descriptor for this worker
            int worker_fd = open(spec->input_data_filepath, O_RDONLY);
            if (worker_fd < 0) {
                _EXIT_ERROR(ERROR, "Worker cannot open input file\n");
            }
            
            // Close parent's file descriptors 
            close(input_fd);
            for (int j = 0; j < i; j++) {
                close(intermediate_fds[j]);
            }
            
            // Setup split information 
            DATA_SPLIT split;
            split.fd = worker_fd;
            split.size = split_sizes[i];
            split.usr_data = spec->usr_data;

            if (lseek(worker_fd, split_starts[i], SEEK_SET) < 0) {
                _EXIT_ERROR(ERROR, "Worker seek failed\n");
            }
            
            int ret = spec->map_func(&split, intermediate_fds[i]);
            
            // Cleanup and exit
            close(worker_fd);
            close(intermediate_fds[i]);
            
            _exit(ret == 0 ? 0 : 1); 
        }
        else {  // Parent process
            result->map_worker_pid[i] = pid;  // Store worker PID
        }
    }
    
    // Wait for all map workers to complete
    for (int i = 0; i < actual_split_num; i++) {
        int status;
        waitpid(result->map_worker_pid[i], &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            EXIT_ERROR(ERROR, "Map worker %d failed\n", i);
        }
        close(intermediate_fds[i]);
    }
    
    // Create and launch reduce worker
    pid_t reduce_pid = fork();
    if (reduce_pid < 0) {
        EXIT_ERROR(ERROR, "Fork failed for reduce worker\n");
    }
    else if (reduce_pid == 0) {  // Reduce worker process
        // Create the final result file
        int result_fd = open(result->filepath, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (result_fd < 0) {
            _EXIT_ERROR(ERROR, "Cannot create result file\n");
        }
        
        // Open all intermediate files for reading
        for (int i = 0; i < actual_split_num; i++) {
            char intermediate_filename[32];
            snprintf(intermediate_filename, sizeof(intermediate_filename), "mr-%d.itm", i);
            intermediate_fds[i] = open(intermediate_filename, O_RDONLY);
            if (intermediate_fds[i] < 0) {
                _EXIT_ERROR(ERROR, "Cannot open intermediate file for reading\n");
            }
        }

        int ret = spec->reduce_func(intermediate_fds, actual_split_num, result_fd);
        
        // Cleanup and exit
        close(result_fd);
        for (int i = 0; i < actual_split_num; i++) {
            close(intermediate_fds[i]);
        }
        
        _exit(ret == 0 ? 0 : 1);  
    }
    else {  // Parent process
        result->reduce_worker_pid = reduce_pid;  // Store reduce worker PID
        
        // Wait for reduce worker to complete
        int status;
        waitpid(reduce_pid, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            EXIT_ERROR(ERROR, "Reduce worker failed\n");
        }
    }
    
    // Final cleanup
    close(input_fd);
    free(split_starts);
    free(split_sizes);
    free(intermediate_fds);

    gettimeofday(&end, NULL);   
    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}