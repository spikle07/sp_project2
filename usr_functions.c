// #include <stdio.h>
// #include <stdlib.h>
// #include <unistd.h>
// #include <fcntl.h>
// #include <string.h>

// #include "common.h"
// #include "usr_functions.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include "common.h"
#include "usr_functions.h"

#define MAX_LINE_LENGTH 4096
#define BUFFER_SIZE 4096

// Helper function to check if a character is a word boundary
static int find_word_boundary(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || 
           c == '.' || c == ',' || c == ';' || c == '!' || 
           c == '?' || c == '"' || c == '\'' || c == '(' || 
           c == ')' || c == '[' || c == ']' || c == '{' || 
           c == '}' || c == '-' || c == ':' || c == '\0';
}

// Helper function to find exact word match
static int find_word(const char* line, const char* word) {
    const char* ptr = line;
    size_t word_len = strlen(word);
    
    while ((ptr = strstr(ptr, word)) != NULL) { 
        // Check if this is a whole word match
        int is_start = (ptr == line) || find_word_boundary(*(ptr - 1));
        int is_end = find_word_boundary(ptr[word_len]);
        
        if (is_start && is_end) {
            return 1;
        }
        ptr++;
    }
    return 0;
}

/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int letter_counter_map(DATA_SPLIT * split, int fd_out)
{
    // add your implementation here ...
    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    int letter_counts[26] = {0}; // Array to store counts for A-Z
    
    // Read and process the split in chunks
    size_t remaining = split->size;
    while (remaining > 0 && (bytes_read = read(split->fd, buffer, 
           remaining < BUFFER_SIZE ? remaining : BUFFER_SIZE)) > 0) {
        // Process each character in the buffer
        for (ssize_t i = 0; i < bytes_read; i++) {
            char c = buffer[i];
            if (isalpha(c)) {
                c = toupper(c);
                letter_counts[c - 'A']++;
            }
        }
        remaining -= bytes_read;
    }
    
    if (bytes_read < 0) {
        return -1;
    }
    
    // Write counts to intermediate file
    char output_line[32];
    for (int i = 0; i < 26; i++) {
        snprintf(output_line, sizeof(output_line), "%c %d\n", 'A' + i, letter_counts[i]);
        if (write(fd_out, output_line, strlen(output_line)) < 0) {
            return -1;
        }
    }
    
    // return SUCCESS;
    
    return 0;
}

/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    // add your implementation here ...
    int total_counts[26] = {0};
    char buffer[BUFFER_SIZE];
    char letter;
    int count;
    
    // Process each intermediate file
    for (int i = 0; i < fd_in_num; i++) {
        lseek(p_fd_in[i], 0, SEEK_SET);
        ssize_t bytes_read;
        char line_buffer[32];
        int pos = 0;
        
        while ((bytes_read = read(p_fd_in[i], buffer, BUFFER_SIZE)) > 0) {
            for (ssize_t j = 0; j < bytes_read; j++) {
                if (buffer[j] == '\n' || pos == sizeof(line_buffer) - 1) {
                    line_buffer[pos] = '\0';
                    if (sscanf(line_buffer, "%c %d", &letter, &count) == 2) {
                        if (letter >= 'A' && letter <= 'Z') {
                            total_counts[letter - 'A'] += count;
                        }
                    }
                    pos = 0;
                } else {
                    line_buffer[pos++] = buffer[j];
                }
            }
        }
    }
    
    // Write final counts to output file
    char output_line[32];
    for (int i = 0; i < 26; i++) {
        snprintf(output_line, sizeof(output_line), "%c %d\n", 'A' + i, total_counts[i]);
        if (write(fd_out, output_line, strlen(output_line)) < 0) {
            return -1;
        }
    }
    
    // return SUCCESS;
    
    return 0;
}

/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int word_finder_map(DATA_SPLIT * split, int fd_out)
{
    // add your implementation here ...
    char *line = malloc(MAX_LINE_LENGTH);
    char *word_to_find = (char *)split->usr_data;
    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    size_t pos = 0;
    size_t remaining = split->size;
    
    if (!line) {
        return -1;
    }
    
    // Read the split data in chunks
    while (remaining > 0 && (bytes_read = read(split->fd, buffer, 
           remaining < BUFFER_SIZE ? remaining : BUFFER_SIZE)) > 0) {
        
        for (ssize_t i = 0; i < bytes_read; i++) {
            if (pos >= MAX_LINE_LENGTH - 1) {
                // Line too long, process what we have
                line[pos] = '\0';
                if (find_word(line, word_to_find)) {
                    write(fd_out, line, pos);
                    write(fd_out, "\n", 1);
                }
                pos = 0;
            }
            
            if (buffer[i] == '\n') {
                // End of line found
                line[pos] = '\0';
                if (find_word(line, word_to_find)) {
                    write(fd_out, line, pos);
                    write(fd_out, "\n", 1);
                }
                pos = 0;
            } else {
                line[pos++] = buffer[i];
            }
        }
        
        remaining -= bytes_read;
    }
    
    // Process last line if it exists
    if (pos > 0) {
        line[pos] = '\0';
        if (find_word(line, word_to_find)) {
            write(fd_out, line, pos);
            write(fd_out, "\n", 1);
        }
    }
    
    free(line);
    return (bytes_read < 0) ? -1 : 0;
    
    // return 0;
}

/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    // add your implementation here ...
    char buffer[BUFFER_SIZE];
    char *current_line = malloc(MAX_LINE_LENGTH);
    char **seen_lines = malloc(sizeof(char *) * 1024);  
    int seen_count = 0;
    
    if (!current_line || !seen_lines) {
        free(current_line);
        free(seen_lines);
        return -1;
    }
    
    // Process each intermediate file
    for (int i = 0; i < fd_in_num; i++) {
        lseek(p_fd_in[i], 0, SEEK_SET);
        ssize_t bytes_read;
        size_t pos = 0;
        
        while ((bytes_read = read(p_fd_in[i], buffer, BUFFER_SIZE)) > 0) {
            for (ssize_t j = 0; j < bytes_read; j++) {
                if (buffer[j] == '\n' || pos >= MAX_LINE_LENGTH - 1) {
                    current_line[pos] = '\0';
                    
                    // Check if we've seen this line before
                    int is_duplicate = 0;
                    for (int k = 0; k < seen_count; k++) {
                        if (strcmp(seen_lines[k], current_line) == 0) {
                            is_duplicate = 1;
                            break;
                        }
                    }
                    
                    // If not a duplicate and line is not empty
                    if (!is_duplicate && pos > 0) {
                        if (seen_count < 1024) {
                            seen_lines[seen_count] = strdup(current_line);
                            seen_count++;
                        }
                        
                        // Write to output
                        write(fd_out, current_line, strlen(current_line));
                        write(fd_out, "\n", 1);
                    }
                    
                    pos = 0;
                } else {
                    current_line[pos++] = buffer[j];
                }
            }
        }
    }
    
    // Cleanup
    free(current_line);
    for (int i = 0; i < seen_count; i++) {
        free(seen_lines[i]);
    }
    free(seen_lines);
    
    // return SUCCESS;
    
    return 0;
}


