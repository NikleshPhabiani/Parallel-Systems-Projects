/*
Single Author info:
nphabia Niklesh Ashok Phabiani
Group info:
nphabia Niklesh Ashok Phabiani
anjain2 Akshay Narendra Jain
rtnaik	Rohit Naik
*/
#include<netdb.h>
#include<errno.h>
#include<netinet/in.h>
#include<string.h>
#include<sys/socket.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<time.h>

#define MPI_COMM_WORLD 1
#define MPI_Datatype double
#define MPI_STATUS_IGNORE -1
int numproc;

struct sockaddr_in address;
int sock;
char myIP[16];
int addrlen = sizeof(address);
int rank;

// Initialization
int MPI_Init(int *argc, char *argv[]) {
	char my_hostname[256]; 
	char *ip_as_string; 
    	struct hostent *h_entry; 
    	int hostname;
	int server_fd; 
	int opt = 1;
	int port_no;
  	char* file = malloc(sizeof(char) * 20);
	rank = atoi(argv[2]);
	numproc = atoi(argv[3]);
	
    	// To retrieve hostname 
    	hostname = gethostname(my_hostname, sizeof(my_hostname));  
  
    	// To retrieve entire host entry
    	h_entry = gethostbyname(my_hostname);  
  
    	// To convert the address into ASCII string 
    	ip_as_string = inet_ntoa(*((struct in_addr*)h_entry->h_addr_list[0])); 

	//Create a new TCP socket
        if((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0){
                printf("Socket creation failed");
		fflush(stdout);
                return -1;
        }

	// Set socket options
        if(setsockopt(sock, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))){
                printf("Setting options failed");
		fflush(stdout);
                return -1;
        }

	// Set address family to internet
	address.sin_family = AF_INET;
        
	// Storing the IP address present as a quad string in the network address (address.sin_addr.s_addr)
	// aton: Address to Network
	inet_aton(ip_as_string, &(address.sin_addr));        
	address.sin_port = htons(0);

	// Bind the socket to a random port and get the address of the server
        if((bind(sock, (struct sockaddr*)&address, sizeof(address))) < 0){
                printf("Binding failed in recv\n");
		fflush(stdout);
                return -1;
        }

	// Number of concurrent connections the server can listen to
	if(listen(sock, 10) < 0){
                printf("listening failed");
		fflush(stdout);
                return -1;
        }

	if(getsockname(sock, (struct sockaddr *) &address, &addrlen) == -1) {
		printf("getsockname() failed\n");
		fflush(stdout);
		return -1;
	}

	// Converting rank which is an int to an string in order to use it as the filename
	sprintf(file, "%d", rank);
	// Opening file for writing. Trying till the file gets opened. Just in case opening fails, it will keep retrying
	FILE *file_pointer = fopen(file, "w+");
	while(file_pointer == NULL) {
		file_pointer = fopen(file, "r");
	}
	port_no = ntohs(address.sin_port);
	
	// Writing IP address and port number in file
	fprintf(file_pointer, "%s %d\n", ip_as_string, port_no);
	fclose(file_pointer);
	
	return 0;  	
}

// Implementing barrier with creation and deletion of files
// Each process waits for file to be created by the process with (rank - 1)
int MPI_Barrier(){
  FILE *fp;
  char *filename = (char *)malloc(sizeof(char)*20);
  if(rank != 0){
  	sprintf(filename, "%d-barrier.txt", rank-1);
  	fp = fopen(filename, "r");
 	while (fp == NULL) {
    		fp = fopen(filename, "r");
	}
  	fclose(fp);
  }
  sprintf(filename, "%d-barrier.txt", rank);
  fp = fopen(filename, "w");
  while(fp == NULL){
	fp = fopen(filename, "w");
  }
  fclose(fp);
}

// Send implementation using sockets
int MPI_Send(void *buf, int count, double datatype, int dest, int tag, int comm) {
        int valread;
	int client_fd;
        struct sockaddr_in serv_addr;
       	double* buffer = (double*) malloc(count * sizeof(double));
	buffer = buf;
	FILE* file_pointer;
		
	char* file = malloc(sizeof(char) * 20);
	int port_no;
	char* server_address = malloc(sizeof(char) * 20);

	//Create a new TCP socket
        if((client_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
                printf("Socket creation failed");
		fflush(stdout);
                return -1;
        }
	
        memset(&serv_addr, '0', sizeof(serv_addr));

        serv_addr.sin_family = AF_INET;
	
	// Converting rank which is an int to string so that the filename can then be used to read relevant file(address and port of destination)
        sprintf(file, "%d", dest);
	// Opening file to read. Trying till the time the file is found. It might so happen that the other process has not yet written
	// its IP and port in the file but this process is trying to access such a file. In short, it will wait till the 
	// destination process has written the data into the file
	file_pointer = fopen(file, "r");
	while(file_pointer == NULL) {
		file_pointer = fopen(file, "r");
	}
	fscanf(file_pointer, "%s %d", server_address, &port_no);
	fclose(file_pointer);
	
	serv_addr.sin_port = htons(port_no);

        if((inet_pton(AF_INET, server_address, &(serv_addr.sin_addr))) <= 0){
                printf("Conversion from text to binary failed");
		fflush(stdout);
                return -1;
        }

        while((connect(client_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr))) < 0){
		sleep(3);
                //printf("Trying to connect new 1\n");
		//fflush(stdout);
        }
        send(client_fd, buf, count*sizeof(double), 0);

        return 0;
}

// Receive call implementation using sockets
int MPI_Recv(void *buffer, int count, double datatype, int source, int tag, int comm, int status) {
        int server_fd, recv_socket, valread;
	struct sockaddr_in client_addr;
	int client_addrlen;
        int opt = 1;
	
	FILE *file_pointer;
	char* file = malloc(sizeof(char) * 10);
	int port_no;
	
	// Wait for a connection from a client
        if((recv_socket = accept(sock, (struct sockaddr*)&address, (socklen_t*)&addrlen)) < 0){
                printf("Acceptance(connection socket) failed");
                return -1;
        }
        valread = read(recv_socket, buffer, count * sizeof(double));
	close(recv_socket);
        return 0;
}

// MPI_Finalize implementation for graceful exit. Will delete all files created for barrier purpose
int MPI_Finalize(){
	char *detailsFile = malloc(sizeof(char)*2);
	sprintf(detailsFile, "%d", rank);
	remove(detailsFile);
	if(rank == numproc-1){
		for(int i=0;i<numproc;i++){
			sprintf(detailsFile, "%d-barrier.txt", i);
			remove(detailsFile);
		}
	}
	close(sock);
}
