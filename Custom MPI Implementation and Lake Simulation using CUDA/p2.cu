/*
Single Author info:
nphabia Niklesh Ashok Phabiani
Group info:
nphabia Niklesh Ashok Phabiani
anjain2 Akshay Narendra Jain
rtnaik	Rohit Naik
*/

/* Program to compute Pi using Monte Carlo methods */

#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <curand_kernel.h>
#define SEED 35791246

//Global function to calculate the value of pi
__global__ void calculate_value_of_pi(int *count, curandState *curandStates) {
	unsigned int thread_id = threadIdx.x + blockDim.x * blockIdx.x;
	curand_init(SEED, thread_id, 0, &curandStates[thread_id]);
	double x = curand_uniform(&curandStates[thread_id]);
      	double y = curand_uniform(&curandStates[thread_id]);
      	double z = x*x+y*y;
	if(z <= 1) {
		count[thread_id]++;
	}
}

int main(int argc, char** argv)
{
	int niter=0;
   	//double x,y;
   	int i,count=0; /* # of points in the 1st quadrant of unit circle */
   	//double z;
   	double pi;

	//number of iterations
   	niter = atoi(argv[1]);

	//random from library
	curandState *curandStates;

	//count devices and hosts
	int *count_d;
	int *count_h = (int*) malloc(niter * sizeof(int));  
	for(i = 0; i < niter; i++) {
		count_h[i] = 0;
	}

   	count=0;

	//Memory allocation
	cudaMalloc((void**)&count_d, niter * sizeof(int));
	cudaMalloc((void**)&curandStates, niter * sizeof(curandState));
	cudaMemcpy(count_d, count_h, niter * sizeof(int), cudaMemcpyHostToDevice);
	calculate_value_of_pi<<<10, niter/10>>>(count_d, curandStates);

	cudaMemcpy(count_h, count_d, niter * sizeof(int), cudaMemcpyDeviceToHost);

	for(i = 0; i < niter; i++) {
		count += count_h[i];
		//printf("Count: %d\n", count);
	}
	//Pi calculation
   	pi=(double)count/niter*4;
	cudaFree(count_d);
	cudaFree(curandStates);
   	printf("# of trials= %d , estimate of pi is %.16f \n",niter,pi);
}