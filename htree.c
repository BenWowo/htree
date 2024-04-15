#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <ctype.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

#define BSIZE 4096
#define BUFFERSIZE 8192
#define true 1
#define false 0
typedef uint8_t bool;

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int8_t i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

struct ThreadArgs {
    u64 num;
    u64 numThreads;
    int fd;
    off_t offset;
    size_t len;
};
typedef struct ThreadArgs ThreadArgs;

u32 jenkins_one_at_a_time_hash(const u8* key, size_t length);
void *tree(void *arg);

size_t getFileSize(int fd);
void validateUsage(int argc, char* argv[]);
int make_open(char* filename, int permissions, char* msg);
void exit_err(char* msg);
bool isInteger(char* number);

int
main(int argc, char* argv[])
{
    validateUsage(argc, argv);
    char* filename = argv[1];
    int fd = make_open(filename, O_RDWR, "failed to open file");
    size_t fileSize = getFileSize(fd);
    u64 numBlocks = fileSize / BSIZE;
    u64 numThreads = atoi(argv[2]);
    printf("File size: %ld\n", fileSize);
    printf("Blocks per thread: %lu\n", numBlocks/numThreads);

    struct timeval start, end;
    gettimeofday(&start, NULL);

    ThreadArgs args;
    args.num = 0;
    args.numThreads = numThreads;
    args.fd = fd;
    args.offset = 0;
    args.len = numBlocks * BSIZE / numThreads;

    pthread_t rootThread;
    void *resultHashPtr;
    pthread_create(&rootThread, NULL, tree, (void*)&args);
    pthread_join(rootThread, &resultHashPtr);

    gettimeofday(&end, NULL);
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    printf("Time taken: %.6f seconds\n", elapsed);

    printf("File hash: %lu\n", *((u64*)resultHashPtr));
    close(fd);
    free(resultHashPtr);


    printf("Thank you for using multithreaded hash tree!\n");
    return EXIT_SUCCESS;
}

// struct ThreadArgs {
//     u64 num;
//     u64 numThreads;
//     int fd;
//     off_t offset;
//     size_t len;
// };
void *
tree(void *arg)
{
    ThreadArgs* args = (ThreadArgs*)arg;
    u8 *consecutiveBlocks = mmap(NULL, args->len, PROT_READ, MAP_PRIVATE, args->fd, args->offset);
    u64 hash = jenkins_one_at_a_time_hash(consecutiveBlocks, args->len);
    u64 *resultHashPtr = (u64*)malloc(sizeof(u64));

    u64 leftNum = args->num * 2 + 1;
    u64 rightNum = args->num * 2 + 2;
    pthread_t leftThread, rightThread;
    void *leftHashPtr, *rightHashPtr;
    // check to make sure that offset is correct
    // I think the values I'm using for the offset is wrong :(
    ThreadArgs leftArgs = {leftNum, args->numThreads, args->fd, leftNum*args->len, args->len};
    ThreadArgs rightArgs = {rightNum, args->numThreads, args->fd, rightNum*args->len, args->len};
    u8 concatBuffer[BUFFERSIZE];

    // 3 conditions...
    // 1) both left and right exist
    // 2) just left exists
    // 3) neither exist
    if (rightNum < args->numThreads) {
        pthread_create(&leftThread, NULL, tree, &leftArgs);
        pthread_create(&rightThread, NULL, tree, &rightArgs);
        pthread_join(leftThread, &leftHashPtr);
        pthread_join(rightThread, &rightHashPtr);
        sprintf((char*)concatBuffer, "%lu%lu%lu", hash, *((u64*)leftHashPtr), *((u64*)rightHashPtr));
        free(leftHashPtr);
        free(rightHashPtr);
        *resultHashPtr = jenkins_one_at_a_time_hash(concatBuffer, strlen((char*)concatBuffer));
        pthread_exit(resultHashPtr);
    } else if (leftNum < args->numThreads) {
        pthread_create(&leftThread, NULL, tree, &leftArgs);
        pthread_join(leftThread, &leftHashPtr);
        sprintf((char*)concatBuffer, "%lu%lu", hash, *((u64*)leftHashPtr));
        free(leftHashPtr);
        *resultHashPtr = jenkins_one_at_a_time_hash(concatBuffer, strlen((char*)concatBuffer));
        pthread_exit(resultHashPtr);
    } else {
        *resultHashPtr = hash;
        pthread_exit(resultHashPtr);
    }
}

u32
jenkins_one_at_a_time_hash(const u8 *key, size_t length)
{
    size_t i = 0;
    u32 hash = 0;
    while (i != length) {
        hash += key[i++];
        hash += hash << 10;
        hash ^= hash >> 6;
    }
    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}

size_t
getFileSize(int fd)
{
    struct stat file_stat;
    fstat(fd, &file_stat);
    return file_stat.st_size;
}

void
validateUsage(int argc, char* argv[])
{
    if (argc != 3) {
        printf("usage: htree <filename> <num_threads>\n");
        exit(EXIT_FAILURE);
    } else if (!isInteger(argv[2])) {
        printf("ERROR: numThreads must be an integer\n");
        exit(EXIT_FAILURE);
    }
}

bool
isInteger(char* number)
{
    for (int i = 0; number[i] != '\0'; i++) {
        if (!isdigit(number[i])) {
            return false;
        }
    }
    return true;
}

int
make_open(char* filename, int permissions, char* msg)
{
    int fd = open(filename, permissions);
    if (fd == -1) {
        exit_err(msg);
    }
    return fd;
}

void
exit_err(char* msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}
