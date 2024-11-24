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

/* Helper Functions */

// Helper function to check if a character is a word boundary
static int is_word_boundary(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || 
           c == '.' || c == ',' || c == ';' || c == '!' || 
           c == '?' || c == '"' || c == '\'' || c == '(' || 
           c == ')' || c == '[' || c == ']' || c == '{' || 
           c == '}' || c == '-' || c == ':' || c == '\0';
}

// Helper function to find exact word match
static int has_exact_word(const char* line, const char* word) {
    const char* ptr = line;
    size_t word_len = strlen(word);
    
    while ((ptr = strstr(ptr, word)) != NULL) {
        // Check if this is a whole word match
        int is_start = (ptr == line) || is_word_boundary(*(ptr - 1));
        int is_end = is_word_boundary(ptr[word_len]);
        
        if (is_start && is_end) {
            return 1;
        }
        ptr++;
    }
    return 0;
}

/* Letter Counter Implementation */

int letter_counter_map(DATA_SPLIT * split, int fd_out) {
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
        return ERROR;
    }
    
    // Write counts to intermediate file
    char output_line[32];
    for (int i = 0; i < 26; i++) {
        snprintf(output_line, sizeof(output_line), "%c %d\n", 'A' + i, letter_counts[i]);
        if (write(fd_out, output_line, strlen(output_line)) < 0) {
            return ERROR;
        }
    }
    
    return SUCCESS;
}

int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out) {
    int total_counts[26] = {0};
    char buffer[BUFFER_SIZE];
    char letter;
    int count;
    
    // Process each intermediate file
    for (int i = 0; i < fd_in_num; i++) {
        // Reset file pointer to start
        lseek(p_fd_in[i], 0, SEEK_SET);
        
        // Read line by line
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
            return ERROR;
        }
    }
    
    return SUCCESS;
}

/* Word Finder Implementation */

int word_finder_map(DATA_SPLIT * split, int fd_out) {
    char *line = malloc(MAX_LINE_LENGTH);
    char *word_to_find = (char *)split->usr_data;
    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    size_t pos = 0;
    size_t remaining = split->size;
    
    if (!line) {
        return ERROR;
    }
    
    // Read the split data in chunks
    while (remaining > 0 && (bytes_read = read(split->fd, buffer, 
           remaining < BUFFER_SIZE ? remaining : BUFFER_SIZE)) > 0) {
        
        for (ssize_t i = 0; i < bytes_read; i++) {
            if (pos >= MAX_LINE_LENGTH - 1) {
                // Line too long, process what we have
                line[pos] = '\0';
                if (has_exact_word(line, word_to_find)) {
                    write(fd_out, line, pos);
                    write(fd_out, "\n", 1);
                }
                pos = 0;
            }
            
            if (buffer[i] == '\n') {
                // End of line found
                line[pos] = '\0';
                if (has_exact_word(line, word_to_find)) {
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
        if (has_exact_word(line, word_to_find)) {
            write(fd_out, line, pos);
            write(fd_out, "\n", 1);
        }
    }
    
    free(line);
    return (bytes_read < 0) ? ERROR : SUCCESS;
}

int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out) {
    char buffer[BUFFER_SIZE];
    char *current_line = malloc(MAX_LINE_LENGTH);
    char **seen_lines = malloc(sizeof(char *) * 1024);  // Dynamic array for seen lines
    int seen_count = 0;
    
    if (!current_line || !seen_lines) {
        free(current_line);
        free(seen_lines);
        return ERROR;
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
                        // Add to seen lines
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
    
    return SUCCESS;
}
