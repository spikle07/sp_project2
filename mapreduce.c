#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"

// add your code here ...


void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    // add you code here ...
    
    struct timeval start, end;

    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }
    
    gettimeofday(&start, NULL);

    // add your code here ...

    gettimeofday(&end, NULL);   

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
