#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <stdbool.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>


#define FILE_COUNT 1


void generate_files(uint32_t filecount){
    char tfn[256];
    /* 4K contiguous buffer in bytes */
    char buffer[4096];
    int testfile = 0;

    /* Seed random num gen */
    srand(time(NULL));

    for (int lcv = 0; lcv < filecount; lcv++)
    {
        snprintf(tfn,sizeof(tfn), "./testfile.%d.4096.%d", getpid(), lcv);

        if ((testfile = open(tfn, O_WRONLY | O_CREAT | O_TRUNC,
                        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1)
        {
            perror("Cannot open output file\n"); exit(1);
        }

        sprintf(buffer, "This is file %d and this was a successfully redirected read\n", lcv);
        write(testfile, (void *)buffer, 4096);

        close(testfile);
    }
}

void cleanup_files(uint32_t filecount){
    char tfn[256];
   
    //fprintf(stderr, "cleanup file start\n");
    for (int lcv = 0; lcv < filecount; lcv++)
    {
        snprintf(tfn,sizeof(tfn), "./testfile.%d.4096.%d", getpid(), lcv);
        unlink(tfn);
    }

}

void test_file_read(uint32_t iterations)
{
    char tfn[256];
    /* 4K contiguous buffer in bytes */
    char buffer[4096];
    char buffer2[4096];
    int pid = getpid();
    int testfile = 0;

    /* Seed random num gen */
    srand(time(NULL));

    


    for (int lcv = 0; lcv < iterations; lcv++)
    {
        uint32_t id = rand() % FILE_COUNT;
        //fprintf(stderr,"File ID %d\n",id);
        snprintf(tfn,sizeof(tfn), "./testfile.%d.4096.%d", pid, id);
        if ((testfile = open(tfn, O_RDONLY,
                        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1)
        {
            perror("Cannot open output file\n"); exit(1);
        }

        read(testfile, buffer, 4096);

        fprintf(stderr,"Actual test the contents of the buffer are \n%s\n", buffer);



        close(testfile);        
    }

if (fork() == 0)
    {
        printf("Hello from Child\n");
        exit(0);
    }


    for (int lcv = 0; lcv < iterations; lcv++)
    {
        uint32_t id = rand() % FILE_COUNT;
        snprintf(tfn,sizeof(tfn), "./testfile.%d.4096.%d", pid, id);
        if ((testfile = open(tfn, O_RDONLY,
                        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1)
        {
            fprintf(stderr,"Cannot open output file %s\n", tfn); exit(1);
        }

        read(testfile, (void *)buffer, 4096);

        fprintf(stderr,"Actual test the contents of the buffer are \n%s\n", buffer);


        close(testfile);        
    }

}

int main(int argc, char **argv){

    printf("Starting test program:");
    generate_files(FILE_COUNT);

    test_file_read(FILE_COUNT);

//    cleanup_files(FILE_COUNT);
    printf("Ending test program\n");
    fflush(stdout);
    return 0;
}

