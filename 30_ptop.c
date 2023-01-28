#pragma GCC optimize("Ofast")

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <semaphore.h>
#include <sys/stat.h>

// can tune
// speedup fgets with fgets_unlocked, it is thread safe in our design because each file will only be accessed by a single thread
#define fgets fgets_unlocked

#define FILE_READ_CONSUMER_THREAD_COUNT 1 // 1 is enough for mode 0
#define FILE_PARSE_PRODUCER_COUNT 2
#define FILE_PARSE_BUFFER_QUEUE_SIZE 4 // queue size must be larger than FILE_PARSE_PRODUCER_COUNT
#define FILE_PARSE_PRODUCER_BUFFER_SIZE 10000

#define FILE_READ_CONSUMER_THREAD_COUNT_1 12
#define FILE_PARSED_BUFFER_SIZE_1 10000

#define HEAPSORT_THRESHOLD 10000

// can't tune
#define COUNTER_SIZE 9321
#define START_TIME 1645491600L
#define FILE_READ_CONSUMER_BUFFER_SIZE 41

struct HeapSortArg {
    int l;
    int r;
};

struct FileParseProducerArg {
    int producer_number;
    char* file_parse_buffer;
};

/*
    Global variables initialization
*/
int mode = 0; // we have two modes in this program, determinted by number of files in the test case
int timestamp[COUNTER_SIZE];
long counter[COUNTER_SIZE];
long k;
char folderpath[41];

sem_t file_parse_counter_mutex;

sem_t file_read_empty, file_read_full;
char file_read_filepath[256];

sem_t file_parse_available, file_parse_not_available, file_parse_buffer_queue_mutex;
sem_t file_parse_buffer_filled[FILE_PARSE_PRODUCER_COUNT];
// char file_parse_buffer[FILE_PARSE_PRODUCER_COUNT][FILE_PARSE_PRODUCER_BUFFER_SIZE][FILE_READ_CONSUMER_BUFFER_SIZE];
long file_parse_buffer_line_count[FILE_PARSE_PRODUCER_COUNT];
int file_parse_buffer_finished[FILE_PARSE_PRODUCER_COUNT];
int file_parse_buffer_queue[FILE_PARSE_BUFFER_QUEUE_SIZE];
int file_parse_buffer_queue_startidx;
int file_parse_buffer_queue_endidx;

/* 
    Helper macro to compare two timestamps
*/
#define compareNodeGreater(a, b) (((counter[timestamp[a]]) != (counter[timestamp[b]])) ? ((counter[timestamp[a]])>(counter[timestamp[b]])) : ((timestamp[a])>(timestamp[b])))

/*
    custom function to replace strtol(), faster becuase we already know the length is 10 and we don't have to handle special case
*/
long custom_strtol(char *s) {
    long res = (*s - '0');
    for(int i=1; i<10; ++i) {
        res = res*10 + (*(++s) - '0');
    }
    return res;
}

/*
    Producer function for the producer consumer model in multiple file reading
*/
void* file_read_producer() 
{
    DIR *d;
    struct dirent *dir;
    d = opendir((char*)folderpath);
    while ((dir = readdir(d)) != NULL) {
        if (dir->d_name[0]=='.')
            continue;

        sem_wait(&file_read_empty);
        
        sprintf(file_read_filepath,"%s%s",folderpath,dir->d_name);
        // read_file(filepath);

        sem_post(&file_read_full);
    }

    sem_wait(&file_read_empty);
    file_read_filepath[0] = '\0';
    sem_post(&file_read_full);

    closedir(d);

    // printf("FILE READ PRODUCER FINISHED\n");
}

/*
    Consumer function for the producer consumer model in multiple file reading
*/
void* file_read_consumer(void* arg) 
{
    char* file_parse_buffer = ((struct FileParseProducerArg*)arg)->file_parse_buffer;
    char filepath[256];
    char local_buffer[41];
    long time_stamp;
    FILE* input;
    int file_unfinished;
    int file_parse_buffer_number;

    while(1) {  
        sem_wait(&file_read_full);
        
        strcpy(filepath, file_read_filepath);
        
        // ceck if producer finished
        if(filepath[0]=='\0') {
            sem_post(&file_read_full);
            break;
        }

        sem_post(&file_read_empty);

        input = fopen(filepath,"r");

        long i;
        char* bufferptr;
        // long long idx;

        while(fgets(local_buffer, 41, input)) {
            sem_wait(&file_parse_available);
            sem_wait(&file_parse_buffer_queue_mutex);
            
            file_parse_buffer_number = file_parse_buffer_queue[file_parse_buffer_queue_startidx];
            file_parse_buffer_queue_startidx = (file_parse_buffer_queue_startidx+1)%FILE_PARSE_BUFFER_QUEUE_SIZE;

            sem_post(&file_parse_buffer_queue_mutex);
            sem_post(&file_parse_not_available);

            // printf("FILE READ CONSUMER OBTAINED PRODUCER %d\n", file_parse_buffer_number);

            // put text into buffer

            bufferptr = file_parse_buffer + file_parse_buffer_number * (FILE_PARSE_PRODUCER_BUFFER_SIZE * (sizeof(char) * FILE_READ_CONSUMER_BUFFER_SIZE));
            strncpy(bufferptr, local_buffer, 10);
            for(i=1; i<FILE_PARSE_PRODUCER_BUFFER_SIZE; ++i) {
                bufferptr += sizeof(char) * FILE_READ_CONSUMER_BUFFER_SIZE;
                if(!fgets(bufferptr, FILE_READ_CONSUMER_BUFFER_SIZE, input)) {
                    break;
                }
            }

            // printf("FILE READ CONSUMER OBTAINED PRODUCER %d\n", file_parse_buffer_number);
            // indicate finished
            file_parse_buffer_line_count[file_parse_buffer_number] = i;
            sem_post(&file_parse_buffer_filled[file_parse_buffer_number]);
        }

        fclose(input);
    }

    for(int i=0; i<FILE_PARSE_PRODUCER_COUNT; ++i) {
            sem_wait(&file_parse_available);
            sem_wait(&file_parse_buffer_queue_mutex);
            
            file_parse_buffer_number = file_parse_buffer_queue[file_parse_buffer_queue_startidx];
            file_parse_buffer_queue_startidx = (file_parse_buffer_queue_startidx+1)%FILE_PARSE_BUFFER_QUEUE_SIZE;

            sem_post(&file_parse_buffer_queue_mutex);
            sem_post(&file_parse_not_available);
            file_parse_buffer_finished[file_parse_buffer_number] = 1;
            sem_post(&file_parse_buffer_filled[file_parse_buffer_number]);
            // printf("%d\n",i);
    }

    // printf("FILE READ CONSUMER FINISHED\n");

}

/*
    Producer function for the producer consumer model in multiple file parsing
    Although it seems to be a consumer, it is actually a producer of my reverse producer-consumer model design, read the document for more information
*/
void* file_parse_producer(void* arg) 
{   
    int producer_number = ((struct FileParseProducerArg*)arg)->producer_number;
    // char file_parse_buffer[FILE_PARSE_PRODUCER_COUNT * (FILE_PARSE_PRODUCER_BUFFER_SIZE*(sizeof(char) * FILE_READ_CONSUMER_BUFFER_SIZE))];
    char* file_parse_buffer = ((struct FileParseProducerArg*)arg)->file_parse_buffer;
    int consumer_unfinished;
    long local_buffer[FILE_PARSE_PRODUCER_BUFFER_SIZE];
    long lines;
    char* bufferptr;
    // printf("PRODUCER STARTED %d\n", producer_number);
    
    while(1) {  
        // printf("PRODUCER STARTED %d\n", producer_number);
        sem_wait(&file_parse_buffer_filled[producer_number]);
        
        // check if all consumers finished
        if(file_parse_buffer_finished[producer_number]) {
            // printf("PRODUCER %d FINISHED\n", producer_number);
            break;
        }
        // printf("PRODUCER %d FILLED BY CONSUMER\n", producer_number);
        // parse lines in buffer
        lines = file_parse_buffer_line_count[producer_number];
        bufferptr = file_parse_buffer + producer_number * (FILE_PARSE_PRODUCER_BUFFER_SIZE* (sizeof(char) * FILE_READ_CONSUMER_BUFFER_SIZE));
        for(long i=0; i<lines; ++i) {
            // local_buffer[i] = (strtol(idx, NULL, 10)-START_TIME)/3600;
            local_buffer[i] = (custom_strtol(bufferptr)-START_TIME)/3600;
            bufferptr += sizeof(char) * FILE_READ_CONSUMER_BUFFER_SIZE;
        }

        sem_wait(&file_parse_counter_mutex);
        for(long i=0; i<lines; ++i) {
            ++counter[local_buffer[i]];
        }
        sem_post(&file_parse_counter_mutex);

        // finished, add to queue
        sem_wait(&file_parse_not_available);
        sem_wait(&file_parse_buffer_queue_mutex);
        
        file_parse_buffer_queue[file_parse_buffer_queue_endidx] = producer_number;
        file_parse_buffer_queue_endidx = (file_parse_buffer_queue_endidx+1) % FILE_PARSE_BUFFER_QUEUE_SIZE;
        sem_post(&file_parse_buffer_queue_mutex);
        sem_post(&file_parse_available);

        // printf("PRODUCER %d AVAILABLE AGAIN\n", producer_number);
    }
}

/*
    Helper function for heap sort
*/
void heapify(int i, int l, int r) {
	int leftidx = l + 2*(i-l) + 1;
	int rightidx = l + 2*(i-l) + 2;
	int idx = i;

	if(rightidx < r) {
		if(compareNodeGreater(rightidx, idx)) {
			idx = rightidx;
		}
		if(compareNodeGreater(leftidx, idx)) {
			idx = leftidx;
		}
	} else if(leftidx < r) {
		if(compareNodeGreater(leftidx, idx)) {
			idx = leftidx;
		}
	}

	if(i != idx) {
        // struct Node temp = counter[i];
		// counter[i] = counter[idx];
		// counter[idx] = temp;
        int temp = timestamp[i];
        timestamp[i] = timestamp[idx];
        timestamp[idx] = temp;
		heapify(idx,l,r);		
	}
}

/*
    Helper function for merging top k in heap sort
*/
void merge(int l, int m, int r) {

    int kcount = k;
    if(kcount>r-l) kcount = r-l;

    int *newarr = malloc(sizeof(int) * kcount);
    int idx = kcount;
    int idx1 = m-1;
    int idx2 = r-1;
    int mn1 = m-k;
    int mn2 = r-k;
    if(mn1<l) mn1 = l;
    if(mn2<m) mn2 = m;

    while(idx && idx1>=(mn1) && idx2>=(mn2)) {
        if(compareNodeGreater(idx1,idx2)) {
            newarr[--idx] = timestamp[idx1];
            --idx1;     
        } else {
            newarr[--idx] = timestamp[idx2];
            --idx2;
        }
    }

    while(idx && idx1>=mn1) {
        newarr[--idx] = timestamp[idx1];
        --idx1;             
    }

    while(idx && idx2>=mn2) {
        newarr[--idx] = timestamp[idx2];
        --idx2;
    }

    --idx;

    for(int i=0; i<kcount; ++i) {
        timestamp[i+(r-kcount)] = newarr[i];
    }

    free(newarr);
}

/*
    Function for heap sort
    Input: Left index (inclusive) and right index (exclusive) for sorting 
    Output: Finished heap sort for topk records
*/
void *heap_sort(void* arg) {
    
    struct HeapSortArg* arg_value = (struct HeapSortArg*) arg;
    int l = arg_value->l;
    int r = arg_value->r;

    if(r-l+1<HEAPSORT_THRESHOLD) {
        // heapify
        for(int i=(l+r)/2-1; i>=l; --i) {
            heapify(i,l,r);
        }
        int temp;

        // heap sort ascending order for top k entries
        int targetk = r-k;
        if(targetk<l) targetk = l;
        for(int i=r-1; i>targetk; --i) {
            temp = timestamp[l];
            timestamp[l] = timestamp[i];
            timestamp[i] = temp;
            heapify(l,l,i);
        }    
        timestamp[targetk] = timestamp[l];
    } else {
        int m = (l+r)/2;
        struct HeapSortArg left_arg = {.l = l, .r = m};
        struct HeapSortArg right_arg = {.l = m, .r = r};
        pthread_t t1;
        pthread_t t2;
        pthread_create(&t1, NULL, heap_sort, (void*) &left_arg);
        pthread_create(&t2, NULL, heap_sort, (void*) &right_arg);
        pthread_join(t1, NULL);
        pthread_join(t2, NULL);
        merge(l,m,r);
    }

    return NULL;
}

/*
    Producer function for the producer consumer model in multiple file reading
*/
void* file_read_producer_1() 
{
    DIR *d;
    struct dirent *dir;
    d = opendir((char*)folderpath);
    while ((dir = readdir(d)) != NULL) {
        if (dir->d_name[0]=='.')
            continue;

        sem_wait(&file_read_empty);
        
        sprintf(file_read_filepath,"%s%s",folderpath,dir->d_name);
        // read_file(filepath);

        sem_post(&file_read_full);
    }

    sem_wait(&file_read_empty);
    file_read_filepath[0] = '\0';
    sem_post(&file_read_full);

    closedir(d);
}

/*
    Consumer function for the producer consumer model in multiple file reading
*/
void* file_read_consumer_1() 
{
    char filepath[256];
    char buffer[FILE_READ_CONSUMER_BUFFER_SIZE];
    long time_stamp;
    FILE* input;
    int file_unfinished;
    int arr[FILE_PARSED_BUFFER_SIZE_1];

    while(1) {  
        sem_wait(&file_read_full);
        
        strcpy(filepath, file_read_filepath);
        
        // ceck if producer finished
        if(filepath[0]=='\0') {
            sem_post(&file_read_full);
            break;
        }

        sem_post(&file_read_empty);

        input = fopen(filepath,"r");

        file_unfinished = 1;

        while(file_unfinished){
            int i;
            for(i=0; i<FILE_PARSED_BUFFER_SIZE_1; ++i) {
                if(!fgets(buffer,FILE_READ_CONSUMER_BUFFER_SIZE,input)) {
                    file_unfinished = 0;
                    break;
                }
                // time_stamp = strtol(buffer,NULL,10);
                arr[i] = (custom_strtol(buffer)-START_TIME)/3600;
            }
            sem_wait(&file_parse_counter_mutex);
            while(i) {
                ++counter[arr[--i]];
            }
            sem_post(&file_parse_counter_mutex);
        }
        fclose(input);
    }
}

int main(int argc, char **argv)
{
    /*
        Initialize variables
    */
    // int n_processors = get_nprocs();
    // printf("system cpu num is %d\n", get_nprocs_conf());
    // printf("system enable num is %d\n", n_processors);

    DIR *d;
    int count = 0;
    struct dirent *dir;
    strcpy(folderpath, argv[1]);
    k = strtol(argv[3], NULL, 10); // remember don't use custom_strtol because the size of k is unknown

    d = opendir((char*)folderpath);
    while (count<=1 && (dir = readdir(d)) != NULL) {
        if (dir->d_name[0]!='.') {
            count++;
        }
    }
    closedir(d);

    // determine the mode by number of files
    mode = (count>1);

    /*
        Common code
    */
    sem_init(&file_read_empty, 0, 1);
    sem_init(&file_read_full, 0, 0);
    sem_init(&file_parse_counter_mutex, 0, 1);
    
    for(int i=0; i<COUNTER_SIZE; ++i) {
        timestamp[i] = i;
    }

    if(mode==0) {
        /*
            MODE 0: Suited for single file
        */
        // char file_parse_buffer[FILE_PARSE_PRODUCER_COUNT][FILE_PARSE_PRODUCER_BUFFER_SIZE][FILE_READ_CONSUMER_BUFFER_SIZE];
        char file_parse_buffer[FILE_PARSE_PRODUCER_COUNT * (FILE_PARSE_PRODUCER_BUFFER_SIZE*(sizeof(char) * FILE_READ_CONSUMER_BUFFER_SIZE))];

        pthread_t file_read_producer_thread;
        pthread_t file_read_consumer_threads[FILE_READ_CONSUMER_THREAD_COUNT];
        pthread_t file_parse_producer_threads[FILE_PARSE_PRODUCER_COUNT];

        sem_init(&file_parse_available, 0, FILE_PARSE_PRODUCER_COUNT);
        sem_init(&file_parse_not_available, 0, 0);
        sem_init(&file_parse_buffer_queue_mutex, 0, 1);
        file_parse_buffer_queue_startidx = 0;
        file_parse_buffer_queue_endidx = 0;

        for(int i=0; i<FILE_PARSE_PRODUCER_COUNT; ++i) {
            sem_init(&file_parse_buffer_filled[i], 0, 0);
            file_parse_buffer_queue[file_parse_buffer_queue_endidx++] = i;
        }

        pthread_create(&file_read_producer_thread, NULL, file_read_producer, NULL);

        struct FileParseProducerArg* arg = malloc(sizeof(struct FileParseProducerArg) * FILE_READ_CONSUMER_THREAD_COUNT);
        for(int i=0; i<FILE_READ_CONSUMER_THREAD_COUNT; ++i ) {
            arg[i].file_parse_buffer = file_parse_buffer;
            pthread_create(&file_read_consumer_threads[i], NULL, file_read_consumer, (void*) &arg[i]);
        }
        struct FileParseProducerArg* arg2 = malloc(sizeof(struct FileParseProducerArg) * FILE_PARSE_PRODUCER_COUNT);
        for(int i=0; i<FILE_PARSE_PRODUCER_COUNT; ++i) {
            arg2[i].producer_number = i;
            arg2[i].file_parse_buffer = file_parse_buffer;
            pthread_create(&file_parse_producer_threads[i], NULL, file_parse_producer, (void*) &arg2[i]);
        }

        /*
            File reading and parsing
        */

        pthread_join(file_read_producer_thread, NULL);
        for(int i=0; i<FILE_READ_CONSUMER_THREAD_COUNT; ++i) {
            pthread_join(file_read_consumer_threads[i], NULL);
        }

        for(int i=0; i<FILE_PARSE_PRODUCER_COUNT; ++i) {
            pthread_join(file_parse_producer_threads[i], NULL);
        }

        // printf("TEST\n");
        /*
        Clean-up
        */
        free(arg);
        free(arg2);
    } else {
        /*
            MODE 1: Suited for multiple small files
        */

        pthread_t file_read_producer_thread;
        pthread_t file_read_consumer_threads[FILE_READ_CONSUMER_THREAD_COUNT_1];

        pthread_create(&file_read_producer_thread, NULL, file_read_producer_1, NULL);
        for(int i=0; i<FILE_READ_CONSUMER_THREAD_COUNT_1; ++i ) {
            pthread_create(&file_read_consumer_threads[i], NULL, file_read_consumer_1, NULL);
        }

        /*
            File reading and parsing
        */
        pthread_join(file_read_producer_thread, NULL);
        for(int i=0; i<FILE_READ_CONSUMER_THREAD_COUNT_1; ++i) {
            pthread_join(file_read_consumer_threads[i], NULL);
        }
    }

    /*
        Sorting
    */
    struct HeapSortArg heap_sort_arg = {.l = 0, .r = COUNTER_SIZE};
    heap_sort((void*) &heap_sort_arg);

    /*
        Output
    */

    char output_buffer[121];
    printf("Top K frequently accessed hour:\n");

    int mn = COUNTER_SIZE-k;
    time_t rawtime;
    struct tm *ptm;

    for(long i=COUNTER_SIZE-1; i>=mn; --i) {
        rawtime = START_TIME + 3600L * timestamp[i];
        ptm = localtime(&rawtime);
        strftime(output_buffer, 121, "%a %b %e %H:00:00 %Y\t", ptm);
        printf("%s",output_buffer);
        printf("%ld\n", counter[timestamp[i]]);
    }

    return 0;
}
