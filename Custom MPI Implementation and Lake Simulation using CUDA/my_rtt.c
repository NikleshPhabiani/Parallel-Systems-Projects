/*
Single Author info:
nphabia Niklesh Ashok Phabiani
*/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include "my_mpi.h"

#define NUMBER_OF_ITERATIONS 10

int main (int argc, char *argv[])
{
	// Initialize MPI
	int rank;
	rank = atoi( argv[2] );
	MPI_Init(&argc, argv);
	//printf("Init finished for rank%d\n", rank);
	//fflush(stdout);
	int send_status;		// To record the return value of data sent
	int receive_status;		// To record the return value of the reply
	int tag = 1;			// Keeping tag as 1
	double *latency;		// To record latency
	double sum_of_latencies = 0;	// To record sum for average
	double average = 0;		// To calculate average and then gather at root
	double standard_deviation = 0;	// To record standard deviation for the pairs
	struct timeval start_time, end_time;	// For measuring RTT
	double *averages;		// To store all the averages at process with rank 0
	double *std_deviation;		// To store all the standard deviations at process with rank 0
	double summation_for_std_deviation = 0;
	char *data = (char*) malloc(2*1024*1024 * sizeof(char));
	int latency_index = 0;

	double *averagePerPair;
	double *stdPerPair;
	int indexPerPair;


	// Allocating memory for (averages) and (std_deviation) only for process with rank since only this process accumulates 
	// the average RTT and standard deviation
	if(rank == 0) {
		averages = (double*) malloc(sizeof(double) * 17 * 4);
		std_deviation = (double*) malloc(sizeof(double) * 17 * 4);
	}
	averagePerPair = malloc(sizeof(double)*17);
	stdPerPair = malloc(sizeof(double)*17);
	indexPerPair = 0;
	

	// Latency to be calculated only for even processes
	if(rank % 2 == 0)
		latency = (double*) malloc(sizeof(double) * 10);
	
	// Starting from message size of 16 because it takes more time for the initial messages.
	// Mostly, the connection gets established and then again terminates before MPI realizes that 
	// processes are communicating continuously, and then establishes a persistent connection.
	for(int message_size = 16; message_size <= (2*1024*1024); message_size *= 2) {
		// Data would be sent to and reply would be received from the same process one time more than the total number of iterations
		// (10 here). An additional message passing is done to ignore the first instance since connection establishment
		// would even happen here, and hence latency would be more.
		for(int iteration_number = 0; iteration_number <= NUMBER_OF_ITERATIONS; iteration_number++) {
			if(rank % 2 == 0) {	// Processes with even ranks are initiators
				// Record start time
				gettimeofday(&start_time, 0);
				// Send data of size message_size to a process with (rank + 1) in the communicator
				//("Sending from rank = %d and message size = %d\n", rank, message_size);
				//fflush(stdout);
				send_status = MPI_Send(data, message_size/sizeof(double), 2.0, rank + 1, message_size - iteration_number, MPI_COMM_WORLD);
				
				receive_status = MPI_Recv(data, message_size/sizeof(double), 2.0, rank + 1, message_size - iteration_number, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

				// Record end time
				gettimeofday(&end_time, 0);

				// Ignoring first iteration. Calculating latency for all other iterations and adding it to the sum.
				if(iteration_number != 0) {
					latency[latency_index] = end_time.tv_usec - start_time.tv_usec;
					sum_of_latencies += latency[latency_index];
					latency_index++;
				}
			}
			else {	
				// Processes with odd ranks receive data and then send reply
				// Receive data from neighbor in pair

				receive_status = MPI_Recv(data, message_size/sizeof(double), 2.0, rank - 1, message_size - iteration_number, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

				send_status = MPI_Send(data, message_size/sizeof(double), 2.0, rank - 1, message_size - iteration_number, MPI_COMM_WORLD);
						
			}
		}
		// After 10 iterations, average and std deviation would be calculated and sent to process with rank 0 for each message size (32 to 2*1024*1024 bytes)
		if(rank % 2 == 0 && message_size != 16) {
			// Calculating average
			average = sum_of_latencies / NUMBER_OF_ITERATIONS;

			// Standard Deviation calculation
			summation_for_std_deviation = 0;
			for(int std_dev_index = 0; std_dev_index < NUMBER_OF_ITERATIONS; std_dev_index++) {
				summation_for_std_deviation += (average - latency[std_dev_index]) * (average - latency[std_dev_index]);
			}
			standard_deviation = sqrt(summation_for_std_deviation / NUMBER_OF_ITERATIONS);
		
			averagePerPair[indexPerPair] = average;
			stdPerPair[indexPerPair] = standard_deviation;
			indexPerPair++;

		}
				
		sum_of_latencies = 0;
		latency_index = 0;
	}
	//printf("Came out of loop %d\n", rank);
	//fflush(stdout);
	MPI_Barrier();

	if(rank == 0){
		for(int i=0;i<17;i++){
			averages[i] = averagePerPair[i];
			std_deviation[i] = stdPerPair[i];
		}
		MPI_Recv(averages+17, 17, 1, 2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		MPI_Recv(std_deviation+17, 17, 1, 2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		MPI_Recv(averages+34, 17, 1, 4, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		MPI_Recv(std_deviation+34, 17, 1, 4, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		MPI_Recv(averages+51, 17, 1, 6, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		MPI_Recv(std_deviation+51, 17, 1, 6, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
	}
	if(rank%2 == 0 && rank != 0){
		MPI_Send(averagePerPair, 17, 1, 0, 0, MPI_COMM_WORLD);
		MPI_Send(stdPerPair, 17, 1, 0, 0, MPI_COMM_WORLD);
	}

	if(rank == 0){
		for(int i=0;i<4;i++){
			int messagesize = 32;
			int index = i*17;
			for(int j=index;j<index+17;j++){
				printf("%d %f %f\n", messagesize, averages[j], std_deviation[j]);
				fflush(stdout);
				messagesize*=2;
			}
			printf("\n");
			fflush(stdout);
		}
	}

	//completed loop

	// Freeing allocated memory
	if(rank == 0) {
		free(averages);
		free(std_deviation);
	}
	if(rank % 2 == 0)
		free(latency);

	free(data);

	// Graceful exit
	MPI_Finalize();	
}
