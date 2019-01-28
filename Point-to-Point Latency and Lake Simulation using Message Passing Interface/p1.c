/*
Single Author info:
nphabia Niklesh Ashok Phabiani
*/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <sys/time.h>
#include "mpi.h"

#define NUMBER_OF_ITERATIONS 10

int main (int argc, char *argv[])
{
	// Initialize MPI
	MPI_Init(&argc, &argv);
	
	int number_of_processes, rank;
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
	char *data = (char*) malloc(2048 * sizeof(char));
	int index = 0;	
	int latency_index = 0;


	// Get number of processes as a part of which this program will run
	MPI_Comm_size(MPI_COMM_WORLD, &number_of_processes);

	// Get rank of process in the communicator
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	// Allocating memory for (averages) and (std_deviation) only for process with rank since only this process accumulates 
	// the average RTT and standard deviation
	if(rank == 0) {
		averages = (double*) malloc(sizeof(double) * 28);
		std_deviation = (double*) malloc(sizeof(double) * 28);
	}

	// Latency to be calculated only for even processes
	if(rank % 2 == 0)
		latency = (double*) malloc(sizeof(double) * 10);
	
	// Starting from message size of 16 because it takes more time for the initial messages.
	// Mostly, the connection gets established and then again terminates before MPI realizes that 
	// processes are communicating continuously, and then establishes a persistent connection.
	for(int message_size = 16; message_size <= 2048; message_size *= 2) {
		// Data would be sent to and reply would be received from the same process one time more than the total number of iterations
		// (10 here). An additional message passing is done to ignore the first instance since connection establishment
		// would even happen here, and hence latency would be more.
		for(int iteration_number = 0; iteration_number <= NUMBER_OF_ITERATIONS; iteration_number++) {
			if(rank % 2 == 0) {	// Processes with even ranks are initiators
				// Record start time
				gettimeofday(&start_time, 0);
				// Send data of size message_size to a process with (rank + 1) in the communicator
				send_status = MPI_Send(data, message_size, MPI_CHAR, rank + 1, message_size - iteration_number, MPI_COMM_WORLD);
				
				// Status check of send
				if(send_status != MPI_SUCCESS) {
					printf("Error encountered while sending data. Aborting and Exiting.\n");
					MPI_Abort(MPI_COMM_WORLD, send_status);
					exit(1);
				}

				// Wait for receiving reply from the (rank + 1) process
				receive_status = MPI_Recv(data, message_size, MPI_CHAR, rank + 1, message_size - iteration_number, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

				// Status check of receive
				if(receive_status != MPI_SUCCESS) {
					printf("Error occured while receiving reply. Aborting and exiting\n");
					MPI_Abort(MPI_COMM_WORLD, receive_status);
					exit(1);
				}
				// Record end time
				gettimeofday(&end_time, 0);

				// Ignoring first iteration. Calculating latency for all other iterations and adding it to the sum.
				if(iteration_number != 0) {
					latency[latency_index] = end_time.tv_usec - start_time.tv_usec;
					sum_of_latencies += latency[latency_index];
					latency_index++;
				}
			}
			else {	// Processes with odd ranks receive data and then send reply
				// Receive data from neighbor in pair
				receive_status = MPI_Recv(data, message_size, MPI_CHAR, rank - 1, message_size - iteration_number, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				
				if(receive_status != MPI_SUCCESS) {
                			printf("Error occured while receiving reply. Aborting and exiting\n");
                        		MPI_Abort(MPI_COMM_WORLD, receive_status);
                			exit(1);
                		}
				// Send echo reply
				send_status = MPI_Send(data, message_size, MPI_CHAR, rank - 1, message_size - iteration_number, MPI_COMM_WORLD);
								
				if(send_status != MPI_SUCCESS) {
					printf("Error occured while sending back reply. Aborting and Exiting\n");
					MPI_Abort(MPI_COMM_WORLD, send_status);
					exit(1);
				}
			}
		}
		// After 10 iterations, average and std deviation would be calculated and sent to process with rank 0 for each message size (32 to 2048 bytes)
		if(rank % 2 == 0) {
			// Calculating average
			average = sum_of_latencies / NUMBER_OF_ITERATIONS;

			// Standard Deviation calculation
			summation_for_std_deviation = 0;
			for(int std_dev_index = 0; std_dev_index < NUMBER_OF_ITERATIONS; std_dev_index++) {
				summation_for_std_deviation += (average - latency[std_dev_index]) * (average - latency[std_dev_index]);
			}
			standard_deviation = sqrt(summation_for_std_deviation / NUMBER_OF_ITERATIONS);
		
			// Receiving average RTT and standard deviation for a particular message size from other pairs
			if(rank == 0 && message_size != 16) {
				averages[index] = average;
				std_deviation[index] = standard_deviation;
				printf("%d ", message_size);
				printf("%f %f ", averages[index], std_deviation[index]);
	
				for(int i = 2; i <= 6; i += 2) {
					MPI_Recv(averages + ((i / 2) * 7 + index), 1, MPI_DOUBLE, i, message_size, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					MPI_Recv(std_deviation + ((i / 2) * 7 + index), 1, MPI_DOUBLE, i, message_size, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

					printf("%f %f ", averages[(i / 2) * 7 + index], std_deviation[(i / 2) * 7 + index]); 

				}
				index++;
				printf("\n");
			}	// Processes 2, 4 and 6 will send the average RTT and standard deviation values to process 0
			else if(message_size != 16){ // Sending average and standard deviation of this pair to rank 0 process (Accumulating)
				MPI_Send(&average, 1, MPI_DOUBLE, 0, message_size, MPI_COMM_WORLD);
				MPI_Send(&standard_deviation, 1, MPI_DOUBLE, 0, message_size, MPI_COMM_WORLD);
			}
		}
				
		sum_of_latencies = 0;
		latency_index = 0;
	}

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
