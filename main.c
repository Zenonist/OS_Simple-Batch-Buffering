//Phonarnun Tatiyamaneekul 6188062,Witchaiyut Phiewla-or 6188143
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sched.h>
#include <string.h>
#include "common.h"
#include "common_threads.h"
#include <semaphore.h>
//declare variables
#define sizeofbuffer 100
#define maximum_wait 80
#define maximum_batch 8
#define used_time 160
#define milisecond_per_input 4
//create enum to represent boolean [false == 0,true == 1]
typedef enum {false, true} bool;
//we can set which thread to do first by sleep the main() before create new thread[worker 2]
bool worker1startsfirst = true;
//this variable use to break the while loop in batcher and worker threads
int program_over = 0;
//create variable pointers for I/O
FILE *outputfile;
//declare mutex lock and semaphore
pthread_mutex_t lock;
sem_t requestworker;
//declare struct
struct inputinformation_t {
    int inputid;
    double timetonext;
    double arrivaltime;
    double finishedtime;
};
struct batchinformation_t {
    struct inputinformation_t input[maximum_batch];
    int inputcounter;
};
//store the time that problem starts running in milisecond
double start;
//create new arraqys by using struct
struct inputinformation_t input_buffer[sizeofbuffer];
struct batchinformation_t batch_buffer[sizeofbuffer];
struct inputinformation_t turnaroundarray[sizeofbuffer];
//declare variable to represent the pointer for client,batcher,worker
int input_read_pointer = 0; //when batcher read input from input_buffer <then put input into the batch_buffer>
int input_write_pointer = 0; //when writer write input to input_buffer
int batch_write_pointer = 0; //when batcher sent input from batch_buffer to worker
int batch_read_pointer = 0; //when worker read input from the batcher (current_batch)
// Number input that store in the worker.
int num_input_requested = 0;                    
int num_input_responded = 0;                    
int amountofinput = 0;

char batcher_str[maximum_batch * 10];
char worker1_str[maximum_batch * 10];
char worker2_str[maximum_batch * 10];

void write_batch(struct batchinformation_t *batch, char *output, int i) {
    //bi uses in for-loop
    int bi = 0;
    //si represents the index 0
    int si = 0;
    //pull the data from char array[at the first index]
    char *output_temp = &output[0];
    //i == 1 means we will remove all elements inside the char array but i == 0 means we will add new inputs into the char array
    if(i == 1){
        si += sprintf(&output[si],"%s"," ");   
    }
    else{
        for (bi = 0; bi < batch->inputcounter; bi++) {
            si += sprintf(&output[si], "%d", batch->input[bi].inputid);
            si += sprintf(&output[si], "%s", " ");
        }
    }

}
//Set the finished time on each input
//[We use logic that we will check the worker#_str after worker# done their job]
//We take split the char by using strtok function and take only the element that is not space[which are inputid<char>]
void write_finished_time(int i){
    //create new char array to store the information from original array because we try to avoid the error that will cause when we use the original one
    char arraytemp[maximum_batch * 10];
    //create struct[pointer] to pull information
    struct inputinformation_t *Turnaround;
    //i presents the worker_id[i = 1 is worker1]
    if (i == 1){
        //copy all elements from original array to temp array
        strcpy(arraytemp,worker1_str);
        //create the char[pointer] that stores the information when strtok function returns the result that is not space
        char  *token = strtok(arraytemp," ");
        //we will loop it until there is no element that is not space in the array
        while(token != NULL ){
            //we use atoi function to convert char into int then we point the struct at the specfic point
            Turnaround = &turnaroundarray[atoi(token)];
            //store the finished time into struct
            Turnaround->finishedtime = GetMSTime() - start;
            printf("%d ",atoi(token));
            //reset the token
            token = strtok(NULL, " ");
        }
        printf("\n");
    }
    else{
        //copy all elements from original array to temp array
        strcpy(arraytemp,worker2_str);
        //create the char[pointer] that stores the information when strtok function returns the result that is not space
        char  *token = strtok(arraytemp," ");
        //we will loop it until there is no element that is not space in the array
        while(token != NULL ){
            //we use atoi function to convert char into int then we point the struct at the specfic point
            Turnaround = &turnaroundarray[atoi(token)];
            //store the finished time into struct
            Turnaround->finishedtime = GetMSTime() - start;
            printf("%d ",atoi(token));
            //reset the token
            token = strtok(NULL, " ");
        }
        printf("\n");
    }
}

// The outinput in file .txt [ 0.00 | QUEUE | 7 8 9 | 1 2 3 | 4 5 6 ].
void log_output(char *event) {
    // Get the time for each input. 
    double current_time = GetMSTime() - start;
    //print the information into file
    fprintf(outputfile,"%lf: %s | %s | %s | %s \n",current_time, event, batcher_str, worker1_str, worker2_str);
}

void *client(void *arg) {
    // Open and read file ready to bring the information in file to use in the code.
    char *inputfile_name= arg;
    FILE* inputfile = fopen(inputfile_name, "r");
    int sleeptime = 0;
    int inputid;
    double request_time;
    struct inputinformation_t *new_job;  
    do {
        // Lock the mutex.
        pthread_mutex_lock(&lock);
        printf("client: %d\n", input_write_pointer);
        new_job = &input_buffer[input_write_pointer];
        input_write_pointer++;
        fscanf (inputfile, "%d %d", &inputid, &sleeptime);
        request_time = GetMSTime() - start;
        new_job->timetonext = request_time;
        new_job->inputid = inputid;
        // Unlock the mutex.
        pthread_mutex_unlock(&lock);
        // Check time of next input for let them sleep if they found value more than 0 and let them sleep equal that value.
        if (sleeptime > 0) {
            msleep(sleeptime);
        }
        // stop the program if they found value less than 0.
        else if (sleeptime < 0){
            break;
        }
    } while (!feof (inputfile));
    // close the input file
    fclose(inputfile);
    printf("client exits\n");
    return NULL;
}

void *batcher(void *arg) {
    int *program_over = (int *)arg;
    
    bool append_mode = false;
    struct inputinformation_t *new_job;
    struct inputinformation_t *job;
    struct batchinformation_t *current_batch;
    struct inputinformation_t *Turnaround;
    bool new_job_received = false;
    double next_batch_at_ms = 1e9;

    current_batch = &batch_buffer[batch_write_pointer];
    current_batch->inputcounter = 0;
    while (true) {
        // Lock the writing mutex.
        pthread_mutex_lock(&lock);
        if (input_read_pointer < input_write_pointer) {         
            // New batch.
            printf("batcher: %d\n", input_read_pointer);
            new_job = &input_buffer[input_read_pointer];
            // Record the arrival time into struct for print infomration into the output file and calculate the turnaround time
            new_job->arrivaltime = GetMSTime() - start;
            Turnaround = &turnaroundarray[amountofinput];
            Turnaround->arrivaltime = GetMSTime() - start;
            // increase the value in each variable for recording the input[amountofinput for pointing the index inside the struct]
            amountofinput++;
            input_read_pointer++;
            num_input_requested++;
            new_job_received = true;
            // unlock the write mutex.
            pthread_mutex_unlock(&lock);
        }
        else{
            // in case that client thread finished their job and exited         
            pthread_mutex_unlock(&lock);
        }
        // if batcher already sent input to worker and there are inputs that are waiting for worker to process
        if (append_mode == true && batch_read_pointer > batch_write_pointer) {      
            batch_write_pointer++;
            // pull the information from struct and reset amount of input inside the current batch as 0 (including next_batch_at_ms)
            current_batch = &batch_buffer[batch_write_pointer];
            current_batch->inputcounter = 0;
            next_batch_at_ms = 1e9;
            // set append_mode to false for new worker to work
            append_mode = false;
        }
        //If there is new input
        if (new_job_received == true) {
            // reset the time if next_batch_at_ms is longer than the the amount of time that inputs should wait until it meets maximum_wait
            if (next_batch_at_ms > new_job->arrivaltime + maximum_wait) {             
                next_batch_at_ms = new_job->arrivaltime + maximum_wait;
            }
            job = &current_batch->input[current_batch->inputcounter];
            job->inputid = new_job->inputid;
            job->arrivaltime = new_job->arrivaltime;
            current_batch->inputcounter++;
            // Write new inputs into batcher_str
            write_batch(current_batch, batcher_str,0);
            // print information into the output file
            log_output("INPUT");
        }
            // If batch is full or arrival time is more than 1000000000 s [next_batch_at_ms is the value that create for case program use long time to compute]
        if (current_batch->inputcounter == maximum_batch || (GetMSTime() - start) > next_batch_at_ms) {
            // if amount of inputs are not at 8 but it already arrives at their time
            if (current_batch->inputcounter < maximum_batch) {
                // if batcher didn't sent input to worker yet
                if (append_mode == false) {
                    // Write new inputs into batcher_str
                    write_batch(current_batch, batcher_str,0);
                    // print information into the output file
                    log_output("QUEUE");
                    // Sent signal to the worker
                    sem_post(&requestworker);
                }
                // Set append_mode to true because it already sent inputs to worker
                append_mode = true;
            // if amount of inputs are more than 8 but it already arrives at their time
            } else {
                // increase the pointer
                batch_write_pointer++;
                // pull the information from struct
                current_batch = &batch_buffer[batch_write_pointer];
                // reset amount of id
                current_batch->inputcounter = 0;
                // reset time that next batch
                next_batch_at_ms = 1e9;
                // Set append_mode to true because it already sent inputs to worker
                append_mode = false;
                // Write new inputs into batcher_str
                write_batch(current_batch, batcher_str,0);
                // print information into the output file
                log_output("QUEUE");
                // Sent signal to the worker
                sem_post(&requestworker);
            }
        }
        // stop batcher if all threads already stop [worker1,worker2][client already exits when these threads exit]
        if (*program_over == -1 && current_batch->inputcounter == 0) {
            break;
        }
        // Set new input false and make it sleep.
        new_job_received = false;
        msleep(1);
    }
    printf("batcher exits\n");
    return NULL;
}

void *worker(void *arg) {
    // pull int from parameter[pointer] by converting pointer into int [*(int *)]
    int worker_id = *(int *)arg;
    // pull the information from struct 
    struct batchinformation_t *current_batch;
    // create new virable to store the amount of time that worker need to wait before it can declare it done
    double work_ms;
    while (true) {
        // wait for the signal from batcher that input is ready for them
        sem_wait(&requestworker);
        printf("worker %d: %d\n",worker_id, batch_read_pointer);
        // pull the information from struct 
        current_batch = &batch_buffer[batch_read_pointer];
        // increase the value which presents amount of id that worker already read[or pull] inputs from batch_buffer
        batch_read_pointer++;
        // pull the information from struct to store amount of inputs that are inside the current_batch[inputs that workers need to work on them]
        num_input_responded += current_batch->inputcounter;
        // check that which workerno is this for write_batch function to write inputs into the correct char array
        if (worker_id == 1) {
            // write inputs into the char array
            write_batch(current_batch, worker1_str,0);
            // remove all inputs inside the batch_buffer[or dispatcher]
            write_batch(NULL,batcher_str,1);
        } else {
            // write inputs into the char array
            write_batch(current_batch, worker2_str,0);
            // remove all inputs inside the batch_buffer[or dispatcher]
            write_batch(NULL,batcher_str,1);
        }
        // print information into the output file
        log_output("START");
        // calculate amount of time that worker need to wait[or sleep] based on used_time and amount of inputs inside the worker
        work_ms = (long) current_batch->inputcounter * milisecond_per_input + used_time;
        msleep(work_ms);
        printf("worker %d: DONE!\n", worker_id);
        // print information into the output file
        log_output("DONE");
        
        // Check if worker ID is 1 print worker1 else print worker2.
        if (worker_id == 1) {
            //record the finished time of these inputs into turnaroundarray struct[number represents the worker no] 
            write_finished_time(1);
            //remove all inputs inside the worker1_str
            write_batch(NULL, worker1_str,1);
        } else {
            //record the finished time of these inputs into turnaroundarray struct[number represents the worker no]
            write_finished_time(2);
            //remove all inputs inside the worker2_str
            write_batch(NULL, worker2_str,1);
        }
        // Stop the worker if the input in worker has equel the requestment.
        if (program_over == -1 && num_input_requested == num_input_responded) {
            break;
        }
    }
    printf("worker %d exits\n", worker_id);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        // If invalid argument 
        printf("Invalid arguments!\n");
        printf("Usage: ./batch_buffer <path/input_file> <path/output_file>");
        return 1;
        
    }
    outputfile = fopen(argv[2], "w");
    int worker1 = 1;
    int worker2 = 2;
    int rc;
    struct sched_param param;
    pthread_attr_t tattr;
    pthread_t client_thread, batcher_thread, worker1_thread, worker2_thread;
    //install attr
    pthread_attr_init(&tattr);
    // Start record the time.
    start = GetMSTime();
    //Create mutex.
    pthread_mutex_init(&lock, NULL);   
    //Create sem.
    sem_init(&requestworker, 0,0);
    Pthread_create(&client_thread, &tattr, client, argv[1]);
    //Create Threads.
    Pthread_create(&batcher_thread, &tattr, batcher, &program_over);
    Pthread_create(&worker1_thread, &tattr, worker, &worker1);    
    // Let worker2 sleep a little bit for let's worker1 do their job.
    if (worker1startsfirst == true){
        usleep(0.0000001);
    }
    Pthread_create(&worker2_thread, &tattr, worker, &worker2);
    //Declare join threads<suspend the calling thread until theses threads finished[client,batcher,worker1,worker2]>
    Pthread_join(client_thread, NULL);
    //Declare program_over as -1 to stop while loop in batcher and worker
    program_over = -1;
    Pthread_join(batcher_thread, NULL);
    Pthread_join(worker1_thread, NULL);
    Pthread_join(worker2_thread, NULL);
    
    // Print and compute Turnarountime for each input.
    double avgturnaround = 0;
    struct inputinformation_t *Turnaround;
    for (int i = 0;i < amountofinput;i++){
        Turnaround = &turnaroundarray[i];
        //Add the turnaround time of each input <For calculating the average>.
        avgturnaround += Turnaround->finishedtime - Turnaround->arrivaltime;
        printf("Input:%d | Turnaroundtime %lf\n",i,Turnaround->finishedtime - Turnaround->arrivaltime);
    }
    //compute the average turnage of the inputs
    avgturnaround = avgturnaround / amountofinput;
    //print the average turnaround time into file
    fprintf(outputfile,"The average turnaround time of the inputs: %lf ms\n",avgturnaround);
    // close the output file
    fclose(outputfile);
    return 0;
}
