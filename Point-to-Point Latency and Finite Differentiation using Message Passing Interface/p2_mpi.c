/*
Single Author info:
nphabia Niklesh Ashok Phabiani
Group info:
nphabia Niklesh Ashok Phabiani
anjain2 Akshay Narendra Jain
rtnaik	Rohit Naik
*/

#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

/* first grid point */
#define   XI              1.0
/* last grid point */
#define   XF              100.0

/* function declarations */
double     fn(double);
void        print_function_data(int, double*, double*, double*, int);
int         main(int, char**);

int main (int argc, char *argv[])
{
	/* process information */
        int   numproc, rank;

        /* initialize MPI */
        MPI_Init(&argc, &argv);

	MPI_Status status;

        /* get the number of procs in the comm */
        MPI_Comm_size(MPI_COMM_WORLD, &numproc);
	MPI_Request *requests;
	requests = (MPI_Request *)malloc(sizeof(MPI_Request)*(numproc-1)*3); //For non-blocking

        /* get my rank in the comm */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	clock_t start, end;
     	double cpu_time_used;
	if(rank == 0)
	    	start = clock();

        int NGRID;
        if(argc > 1)
            NGRID = atoi(argv[1]);
        else 
        {
                printf("Please specify the number of grid points.\n");
                exit(0);
        }

	//Receiving arguments from command line
	int point_to_point_type, gather_type;
	point_to_point_type = atoi(argv[2]);
	gather_type = atoi(argv[3]);

        //loop index
        int         i;

	//"real" grid indices
        int         imin, imax;  

        imin = 1;
	if(rank != numproc-1)
	        imax = NGRID/numproc;
	else //Handling uneven distribution
		imax = NGRID/numproc+NGRID%numproc;

        //domain array and step size
        double       *xc = (double *)malloc(sizeof(double)* (imax + 2) );
        double      dx;

        //function array and derivative
        //the size will be dependent on the
        //number of processors used
        //to the program
        double     *yc, *dyc;

	//final array
	double *fullx, *fully, *fulldyc;

        //construct grid
        for (i = 1; i <= imax ; i++)
        {
		int index = (i-1) + (NGRID/numproc)*rank;
                xc[i] = XI + (XF - XI) * (double)(index)/(double)(NGRID - 1);
        }
        //step size and boundary points
        dx = xc[2] - xc[1];
        xc[0] = xc[1] - dx;
        xc[imax+1] = xc[imax] + dx;

        //allocate function arrays
        yc  =   (double *) malloc((imax+2) * sizeof(double));
        dyc =   (double *) malloc((imax) * sizeof(double));

	//Allocating NGRID size arrays for gathering all values in master node
	if(rank == 0){
		fullx = (double*) malloc((NGRID) * sizeof(double));
		fully = (double*) malloc((NGRID) * sizeof(double));
		fulldyc = (double*) malloc((NGRID) * sizeof(double));
	}
	
        //define the function
        for( i = imin; i <= imax; i++ )
        {
                yc[i] = fn(xc[i]);
        }

	/*
	Calculate yc[0] and yc[imax+1]
	*/
	
	//Using MPI_Request for Isend and Irecv for non-blocking communication
	MPI_Request *mpi_requests = (MPI_Request *)malloc(4*sizeof(MPI_Request));
	if(rank == 0){
		yc[0] = fn(xc[0]);
		if(point_to_point_type == 0){
			MPI_Send(yc+imax, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD);
		}
		else{
			MPI_Isend(yc+imax, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD, &(mpi_requests[0]));
		}
		//Only 1 receieve and has to wait anyway. So no point using Irecv
		MPI_Recv(yc+imax+1, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD, &status);
	}
	else if(rank==numproc-1){
		yc[imax+1] = fn(xc[imax+1]);

		/*
		Avoiding deadlock by making even processes send first, and receive after that, and reverse for odd processes
		*/
		if(rank%2==1){
			//Only 1 receieve and has to wait anyway. So no point using Irecv
			MPI_Recv(yc, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &status);
			if(point_to_point_type == 0){
				MPI_Send(yc+1, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD);
			}
			else{ //for non-blocking communication
				MPI_Isend(yc+1, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &(mpi_requests[0]));
			}	
		}
		else{
			if(point_to_point_type == 0){
				MPI_Send(yc+1, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD);
			}
			else{ //for non-blocking communication

				MPI_Isend(yc+1, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &(mpi_requests[0]));
			}	
			//Only 1 receieve and has to wait anyway. So no point using Irecv		
			MPI_Recv(yc, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &status);
		}
	}
	else{
		/*
		Avoiding deadlock by making even processes send first, and receive after that, and reverse for odd processes
		*/

		if(point_to_point_type == 0){
			if(rank%2==1){
				MPI_Recv(yc, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &status);
				MPI_Recv(yc+imax+1, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD, &status);
				MPI_Send(yc+1, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD);
				MPI_Send(yc+imax, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD);
			}
			else{
				MPI_Send(yc+1, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD);
				MPI_Send(yc+imax, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD);
				MPI_Recv(yc, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &status);
				MPI_Recv(yc+imax+1, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD, &status);	
			}
		}
		else{ //non-blocking communication, works with always Irecv first
			MPI_Irecv(yc, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &(mpi_requests[0]));
			MPI_Irecv(yc+imax+1, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD, &(mpi_requests[1]));
			MPI_Isend(yc+1, 1, MPI_DOUBLE, rank-1, 0, MPI_COMM_WORLD, &(mpi_requests[2]));
			MPI_Isend(yc+imax, 1, MPI_DOUBLE, rank+1, 0, MPI_COMM_WORLD, &(mpi_requests[3]));
			MPI_Waitall(2, mpi_requests, MPI_STATUSES_IGNORE);
		}
	}


        //NB: boundary values of the whole domain
        //should be set
        //yc[0] = fn(xc[0]);
        //yc[imax + 1] = fn(xc[NGRID + 1]);

        //compute the derivative using first-order finite differencing
        //
        //  d           f(x + h) - f(x - h)
        // ---- f(x) ~ --------------------
        //  dx                 2 * dx
        //
        for (i = imin; i <= imax; i++)
        {
                dyc[i-1] = (yc[i + 1] - yc[i - 1])/(2.0 * dx);
        }

	int *recv_counts, *displs;
	if(rank == 0){
		recv_counts = (int *)malloc(numproc*sizeof(int));
		displs = (int *)malloc(numproc*sizeof(int));
		int counts;
		for(counts=0;counts<numproc;counts++){
			recv_counts[counts] = NGRID/numproc;
			displs[counts] = NGRID/numproc*counts;
		}	
		//Last process may have more points due to uneven distribution
		recv_counts[numproc-1] = NGRID/numproc + NGRID%numproc;
	}
	
	if(point_to_point_type == 0 && gather_type == 0){
		MPI_Gatherv(xc+1, imax, MPI_DOUBLE, fullx, recv_counts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD);
		MPI_Gatherv(yc+1, imax, MPI_DOUBLE, fully, recv_counts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD);
		MPI_Gatherv(dyc, imax, MPI_DOUBLE, fulldyc, recv_counts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD);
	}
	
	else if(point_to_point_type == 1 && gather_type == 0){
		//Using Igatherv for non-blocking communication
		MPI_Igatherv(xc+1, imax, MPI_DOUBLE, fullx, recv_counts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD, &requests[0]);
		MPI_Igatherv(yc+1, imax, MPI_DOUBLE, fully, recv_counts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD, &requests[1]);
		MPI_Igatherv(dyc, imax, MPI_DOUBLE, fulldyc, recv_counts, displs, MPI_DOUBLE, 0, MPI_COMM_WORLD, &requests[2]);
		//But has to wait to receive values from all other processes and all values (x, y, dy/dx)
		MPI_Waitall(3, requests, MPI_STATUSES_IGNORE);
	}

	//Synchronous call: Root process waits to receive from each node before proceeding to receive from another node
	else if(point_to_point_type == 0 && gather_type == 1){
		if(rank!=0){
			MPI_Send(xc+1, imax, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
			MPI_Send(yc+1, imax, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
			MPI_Send(dyc, imax, MPI_DOUBLE, 0, 2, MPI_COMM_WORLD);
		}
		else{ //rank is 0
			//Manually setting values calculated by rank 0
			for(int i=0;i<imax; i++){
				fullx[i] = xc[i+1];
				fully[i] = yc[i+1];
				fulldyc[i] = dyc[i];
			}
			for(int i=1;i<numproc-1;i++){	
				MPI_Recv(fullx+i*(NGRID/numproc), NGRID/numproc, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &status);
				MPI_Recv(fully+i*(NGRID/numproc), NGRID/numproc, MPI_DOUBLE, i, 1, MPI_COMM_WORLD, &status);
				MPI_Recv(fulldyc+i*(NGRID/numproc), NGRID/numproc, MPI_DOUBLE, i, 2, MPI_COMM_WORLD, &status);
			}
			//last process may have more points due to uneven distribution
			int last = numproc-1;
			MPI_Recv(fullx+last*(NGRID/numproc), NGRID/numproc+NGRID%numproc, MPI_DOUBLE, last, 0, MPI_COMM_WORLD, &status);
			MPI_Recv(fully+last*(NGRID/numproc), NGRID/numproc+NGRID%numproc, MPI_DOUBLE, last, 1, MPI_COMM_WORLD, &status);
			MPI_Recv(fulldyc+last*(NGRID/numproc), NGRID/numproc+NGRID%numproc, MPI_DOUBLE, last, 2, MPI_COMM_WORLD, &status);
		}
	}

	//Asynchronous call: Root process simultenously receives from other nodes
	else{
		if(rank!=0){
			//Not using Isend because memory may be freed before sending completes otherwise
			MPI_Send(xc+1, imax, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
			MPI_Send(yc+1, imax, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
			MPI_Send(dyc, imax, MPI_DOUBLE, 0, 2, MPI_COMM_WORLD);
		}
		else{ //rank is 0
			//Manually setting values calculated by rank 0
			for(int i=0;i<imax; i++){
				fullx[i] = xc[i+1];
				fully[i] = yc[i+1];
				fulldyc[i] = dyc[i];
			}
			for(int i=1;i<numproc-1;i++){
				MPI_Irecv(fullx+i*(NGRID/numproc), NGRID/numproc, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &requests[(i-1)*3]);
				MPI_Irecv(fully+i*(NGRID/numproc), NGRID/numproc, MPI_DOUBLE, i, 1, MPI_COMM_WORLD, &requests[(i-1)*3+1]);
				MPI_Irecv(fulldyc+i*(NGRID/numproc), NGRID/numproc, MPI_DOUBLE, i, 2, MPI_COMM_WORLD, &requests[(i-1)*3+2]);
			}

			//last process may have more points due to uneven distribution
			int last = numproc-1;
			MPI_Irecv(fullx+last*(NGRID/numproc), NGRID/numproc+NGRID%numproc, MPI_DOUBLE, last, 0, MPI_COMM_WORLD, &requests[(last-1)*3]);
			MPI_Irecv(fully+last*(NGRID/numproc), NGRID/numproc+NGRID%numproc, MPI_DOUBLE, last, 1, MPI_COMM_WORLD, &requests[(last-1)*3+1]);
			MPI_Irecv(fulldyc+last*(NGRID/numproc), NGRID/numproc+NGRID%numproc, MPI_DOUBLE, last, 2, MPI_COMM_WORLD, &requests[(last-1)*3+2]);
			
			//But has to wait to receive values from all other processes and all values (x, y, dy/dx)
			MPI_Waitall((numproc-1)*3, requests, MPI_STATUSES_IGNORE);
		}
	}
	

	if(rank==0)
		print_function_data(NGRID, fullx, fully, fulldyc, rank);

        //free allocated memory 
	free(xc);
        free(yc);
        free(dyc);

	if(rank == 0){
		free(fullx);
		free(fully);
		free(fulldyc);
	}

	if(rank == 0){
		end = clock();
		cpu_time_used = ((double) (end - start)) / (CLOCKS_PER_SEC/1000);
		//printf("Time required for point-to-point_type %d and gather_type %d with %d processes = %f milliseconds\n", point_to_point_type, gather_type, numproc, cpu_time_used);
	}

	/* graceful exit */
        MPI_Finalize();
	
        return 0;
}

//prints out the function and its derivative to a file
void print_function_data(int np, double *x, double *y, double *dydx, int rank)
{

	int   i;

        char filename[1024];
        sprintf(filename, "fn-%d.dat", np);

        FILE *fp = fopen(filename, "w");

        for(i = 0; i < np; i++)
        {
                fprintf(fp, "%f %f %f\n", x[i], y[i], dydx[i]);
        }

        fclose(fp);

}