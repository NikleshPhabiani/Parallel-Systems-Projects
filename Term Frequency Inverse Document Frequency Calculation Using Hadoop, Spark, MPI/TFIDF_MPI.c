/*
Single Author Info:
nphabia Niklesh Ashok Phabiani
*/
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<dirent.h>
#include<math.h>
#include "mpi.h"

#define MAX_WORDS_IN_CORPUS 32
#define MAX_FILEPATH_LENGTH 16
#define MAX_WORD_LENGTH 16
#define MAX_DOCUMENT_NAME_LENGTH 8
#define MAX_STRING_LENGTH 64

typedef char word_document_str[MAX_STRING_LENGTH];

typedef struct o {
	char word[32];
	char document[8];
	int wordCount;
	int docSize;
	int numDocs;
	int numDocsWithWord;
} obj;

typedef struct w {
	char word[32];
	int numDocsWithWord;
	int currDoc;
} u_w;

static int myCompare (const void * a, const void * b)
{
    return strcmp (a, b);
}

int main(int argc , char *argv[]){

  // Initialize MPI
  MPI_Init(&argc, &argv);
	// Process information
  int numproc, rank;
  // Get the number of processes in the communicator
  MPI_Comm_size(MPI_COMM_WORLD, &numproc);
  // Get my rank in the communicator
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	DIR* files;
	struct dirent* file;
	int i,j;
	int numDocs = 0, docSize, contains;
	char filename[MAX_FILEPATH_LENGTH], word[MAX_WORD_LENGTH], document[MAX_DOCUMENT_NAME_LENGTH];
	int evenlyDistributedNumberOfFiles = 0;
	int additionalFiles = 0;
	int numberOfDocsForEachWorker[numproc];
	// Received by worker processes from the master
	int numberOfDocsToBeProcessed = 0;
	// 1. startingDocNumber used by Master to calculate starting doc number for each worker
	// 2. startingDocNumber used by each worker to know the document number from which it needs to start processing
	// the number of documents it needs to process.
	// 3. Each worker receives the starting doc number and the number of documents it needs to
	// process from the Master (rank 0).
	int startingDocNumber = 1;
	int startingDocNumberForWorker[numproc];
	int workerRank;
	int iterationNumber = 0;
	int *uniqueWordEntriesFromWorker;
	int *numberOfTFEntriesFromWorker;
	MPI_Request *sendRequests;
	MPI_Request *receiveRequests;
	MPI_Request *gatherRequests;
	MPI_Status status;

	// Derived data type corresponding to the u_w struct
	MPI_Datatype u_w_StructType;
	// count = 2 because two different data types in the struct u_w (char and int)
	int count = 2;
	// Declaration of arrays needed to construct the derived MPI_Datatype (each one with size 'count')
	int blocklengths[count];
	MPI_Aint displacements[count];
	MPI_Datatype types[count];

	// To store unique words present with each worker on receiving it from each one of them
	u_w *uniqueWordsReceivedFromOtherWorkers;
	int totalNumberOfUniqueWordsFromOtherWorkers = 0;
	int *recv_counts, *displs;

	// Derived data type corresponding to the word_document_str array
	MPI_Datatype word_document_str_type;
	MPI_Type_contiguous(MAX_STRING_LENGTH, MPI_CHAR, &word_document_str_type);
	MPI_Type_commit(&word_document_str_type);

	if(rank != 0) {
		//For non-blocking
		sendRequests = (MPI_Request *)malloc(sizeof(MPI_Request)*(numproc-2));
		receiveRequests = (MPI_Request *)malloc(sizeof(MPI_Request)*(numproc-2));
		uniqueWordEntriesFromWorker = (int *) malloc((numproc) * sizeof(int));
	}
	else if(rank == 0) {
		gatherRequests = (MPI_Request *)malloc(sizeof(MPI_Request)*2);
		numberOfTFEntriesFromWorker = (int *) malloc((numproc) * sizeof(int));
	}

	// Will hold all TFIDF objects for all documents
	obj TFIDF[MAX_WORDS_IN_CORPUS];
	int TF_idx = 0;

	// Will hold all unique words in the corpus and the number of documents with that word
	u_w unique_words[MAX_WORDS_IN_CORPUS];
	int uw_idx = 0;

	// Will hold the final strings that will be printed out
	word_document_str strings[MAX_WORDS_IN_CORPUS];

	//Count numDocs
	if((files = opendir("input")) == NULL){
		printf("Directory failed to open\n");
		exit(1);
	}

	// Only Master (process with rank 0) will calculate the total number
	// of documents and send it to all the worker nodes
	if(rank == 0) {

		while((file = readdir(files))!= NULL){
			// On linux/Unix we don't want current and parent directories
			if(!strcmp(file->d_name, "."))	 continue;
			if(!strcmp(file->d_name, "..")) continue;
			numDocs++;
		}
		// Each worker process will get atleast these many files to work on
		evenlyDistributedNumberOfFiles = numDocs / (numproc - 1);
		additionalFiles = numDocs % (numproc - 1);

		// Calculate the number of documents and starting number of document for each worker
		for(int destinationRank = 1; destinationRank < numproc; destinationRank++) {
				numberOfDocsForEachWorker[destinationRank] = evenlyDistributedNumberOfFiles;
				if(additionalFiles != 0) {
					numberOfDocsForEachWorker[destinationRank] += 1;
					additionalFiles--;
				}
				startingDocNumberForWorker[destinationRank] = startingDocNumber;
				startingDocNumber += numberOfDocsForEachWorker[destinationRank];
		}

		// 1. Send numDocs to all the workers in the system
		// 2. Send number of documents each worker needs to process
		// 3. Send the starting document number (offset) from where it needs to start the processing
		for(int destinationRank = 1; destinationRank < numproc; destinationRank++) {
				MPI_Send(&numDocs, 1, MPI_INT, destinationRank, 0, MPI_COMM_WORLD);
				MPI_Send(&numberOfDocsForEachWorker[destinationRank], 1, MPI_INT, destinationRank, 0, MPI_COMM_WORLD);
				MPI_Send(&startingDocNumberForWorker[destinationRank], 1, MPI_INT, destinationRank, 0, MPI_COMM_WORLD);
		}
	}
	else {
		// All worker processes will receive the numDocs value from master (rank 0)
		// All worker processes will receive the number of documents it needs to process from the master (rank 0)
		// All worker processes will receive the document number from where it needs to start, from the master (rank 0)
		MPI_Recv(&numDocs, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
		MPI_Recv(&numberOfDocsToBeProcessed, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
		MPI_Recv(&startingDocNumber, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
	}

	// Loop through the documents the worker is responsible for and gather TFIDF variables for each word
	if(rank != 0) {	// Only workers will calculate the TFIDF
		int uniqueWordsReceivedIndex = 0;
		for(i = 0; i < numberOfDocsToBeProcessed; i++){
			sprintf(document, "doc%d", (startingDocNumber + i));
			sprintf(filename,"input/%s",document);
			FILE* fp = fopen(filename, "r");
			if(fp == NULL){
				printf("Error Opening File: %s\n", filename);
				exit(0);
			}

			// Get the document size
			docSize = 0;
			while((fscanf(fp,"%s",word))!= EOF)
				docSize++;

			// For each word in the document
			fseek(fp, 0, SEEK_SET);
			while((fscanf(fp,"%s",word))!= EOF){
				contains = 0;

				// If TFIDF array already contains the word@document, just increment wordCount and break
				for(j=0; j<TF_idx; j++) {
					if(!strcmp(TFIDF[j].word, word) && !strcmp(TFIDF[j].document, document)){
						contains = 1;
						TFIDF[j].wordCount++;
						break;
					}
				}

				//If TFIDF array does not contain it, make a new one with wordCount=1
				if(!contains) {
					strcpy(TFIDF[TF_idx].word, word);
					strcpy(TFIDF[TF_idx].document, document);
					TFIDF[TF_idx].wordCount = 1;
					TFIDF[TF_idx].docSize = docSize;
					TFIDF[TF_idx].numDocs = numDocs;
					TF_idx++;
				}

				contains = 0;
				// If unique_words array already contains the word, just increment numDocsWithWord
				for(j=0; j<uw_idx; j++) {
					if(!strcmp(unique_words[j].word, word)){
						contains = 1;
						if(unique_words[j].currDoc != i) {
							unique_words[j].numDocsWithWord++;
							unique_words[j].currDoc = i;
						}
						break;
					}
				}

				// If unique_words array does not contain it, make a new one with numDocsWithWord=1
				if(!contains) {
					strcpy(unique_words[uw_idx].word, word);
					unique_words[uw_idx].numDocsWithWord = 1;
					unique_words[uw_idx].currDoc = i;
					uw_idx++;
				}
			}
			fclose(fp);
		}


		// Print TF job similar to HW4/HW5 (For debugging purposes)
		printf("-------------TF Job-------------\n");
		for(j=0; j<TF_idx; j++)
			printf("%s@%s\t%d/%d\n", TFIDF[j].word, TFIDF[j].document, TFIDF[j].wordCount, TFIDF[j].docSize);

		// Send to and receive from all other workers the number of unique word entries with them
		iterationNumber = 0;
		for(workerRank = 1; workerRank < numproc; workerRank++) {
			if(workerRank != rank) {
				MPI_Isend(&uw_idx, 1, MPI_INT, workerRank, 0, MPI_COMM_WORLD, &sendRequests[iterationNumber]);
				MPI_Irecv(&(uniqueWordEntriesFromWorker[workerRank]), 1, MPI_INT, workerRank, 0, MPI_COMM_WORLD, &receiveRequests[iterationNumber]);
				iterationNumber++;
			}
		}
		// Proceed only after received unique word entries from all other workers
		MPI_Waitall(numproc - 2, receiveRequests, MPI_STATUSES_IGNORE);

		// Calculate the total number of unique word entries to be received from other workers
		for(workerRank = 1; workerRank < numproc; workerRank++) {
			if(workerRank != rank) {
				totalNumberOfUniqueWordsFromOtherWorkers += uniqueWordEntriesFromWorker[workerRank];
			}
		}

		// Allocate memory to receive unique word entries from other workers
		uniqueWordsReceivedFromOtherWorkers = (u_w *) malloc(totalNumberOfUniqueWordsFromOtherWorkers * sizeof(u_w));

		// Defining the arrays for the derived MPI_Datatype u_w
		blocklengths[0] = 32;
		displacements[0] = (size_t)&(unique_words[0].word[0]) - (size_t)&unique_words;
		types[0] = MPI_CHAR;

		blocklengths[1] = 2;
		displacements[1] = (size_t)&(unique_words[0].numDocsWithWord) - (size_t)&unique_words;
		types[1] = MPI_INT;

		MPI_Type_create_struct(count, blocklengths, displacements, types, &u_w_StructType);
		MPI_Type_commit(&u_w_StructType);

		free(sendRequests);
		free(receiveRequests);

		sendRequests = (MPI_Request *)malloc(sizeof(MPI_Request)*(uw_idx) * (numproc));
		receiveRequests = (MPI_Request *)malloc(sizeof(MPI_Request)*(totalNumberOfUniqueWordsFromOtherWorkers));

		// Send from each worker to all other workers all the unique words data
		iterationNumber = 0;
		uniqueWordsReceivedIndex = 0;
		for(workerRank = 1; workerRank < numproc; workerRank++) {
			if(workerRank != rank) {
				for(j = 0; j < uw_idx; j++) {
					MPI_Isend(&unique_words[j], 1, u_w_StructType, workerRank, 0, MPI_COMM_WORLD, &sendRequests[iterationNumber]);
					iterationNumber++;
				}
				for(j = 0; j < uniqueWordEntriesFromWorker[workerRank]; j++) {
					MPI_Irecv(&uniqueWordsReceivedFromOtherWorkers[uniqueWordsReceivedIndex], 1, u_w_StructType, workerRank, 0, MPI_COMM_WORLD, &receiveRequests[uniqueWordsReceivedIndex]);
					uniqueWordsReceivedIndex++;
				}
			}
		}

		// Proceed only after received unique word entries from all other workers
		MPI_Waitall(totalNumberOfUniqueWordsFromOtherWorkers, receiveRequests, MPI_STATUSES_IGNORE);
		// Free the derived data type
		MPI_Type_free(&u_w_StructType);

		// Update numDocsWithWord for all words with itself as well as with some other worker(s)
		for(uniqueWordsReceivedIndex = 0; uniqueWordsReceivedIndex < totalNumberOfUniqueWordsFromOtherWorkers; uniqueWordsReceivedIndex++) {
			for(j = 0; j < uw_idx; j++) {
				if(!strcmp(uniqueWordsReceivedFromOtherWorkers[uniqueWordsReceivedIndex].word, unique_words[j].word)) {
					unique_words[j].numDocsWithWord += uniqueWordsReceivedFromOtherWorkers[uniqueWordsReceivedIndex].numDocsWithWord;
					break;
				}
			}
		}

		// Use unique_words array to populate TFIDF objects with: numDocsWithWord
		for(i=0; i<TF_idx; i++) {
			for(j=0; j<uw_idx; j++) {
				if(!strcmp(TFIDF[i].word, unique_words[j].word)) {
					TFIDF[i].numDocsWithWord = unique_words[j].numDocsWithWord;
					break;
				}
			}
		}

		// Print IDF job similar to HW4/HW5 (For debugging purposes)
		printf("------------IDF Job-------------\n");
		for(j=0; j<TF_idx; j++)
			printf("%s@%s\t%d/%d\n", TFIDF[j].word, TFIDF[j].document, TFIDF[j].numDocs, TFIDF[j].numDocsWithWord);

		// Calculates TFIDF value and puts: "document@word\tTFIDF" into strings array
		for(j=0; j<TF_idx; j++) {
			double TF = 1.0 * TFIDF[j].wordCount / TFIDF[j].docSize;
			double IDF = log(1.0 * TFIDF[j].numDocs / TFIDF[j].numDocsWithWord);
			double TFIDF_value = TF * IDF;
			sprintf(strings[j], "%s@%s\t%.16f", TFIDF[j].document, TFIDF[j].word, TFIDF_value);
		}
	}

	// Gather the number of TFIDF entries at the master from each worker
	MPI_Igather(&TF_idx, 1, MPI_INT, numberOfTFEntriesFromWorker, 1, MPI_INT, 0, MPI_COMM_WORLD, &gatherRequests[0]);
	MPI_Waitall(1, gatherRequests, MPI_STATUSES_IGNORE);

	// Calculate recv_counts and displs to gather the TFIDF from each worker
	if(rank == 0) {
		// Calculate the total number of unique word entries received from other workers
		for(workerRank = 1; workerRank < numproc; workerRank++) {
			totalNumberOfUniqueWordsFromOtherWorkers += numberOfTFEntriesFromWorker[workerRank];
		}
		int cumulativeDispls = 0;
		recv_counts = (int *)malloc(numproc*sizeof(int));
		displs = (int *)malloc(numproc*sizeof(int));
		recv_counts[0] = 0;
		displs[0] = 0;
		for(workerRank = 1; workerRank < numproc; workerRank++) {
			recv_counts[workerRank] = numberOfTFEntriesFromWorker[workerRank];
			displs[workerRank] = cumulativeDispls;
			cumulativeDispls += recv_counts[workerRank];
		}
	}

	// Gather the TFIDF strings at the master from each worker
	MPI_Igatherv(strings, TF_idx, word_document_str_type, strings, recv_counts, displs, word_document_str_type, 0, MPI_COMM_WORLD, &gatherRequests[0]);

	MPI_Waitall(1, gatherRequests, MPI_STATUSES_IGNORE);

	// Free the derived data type
	MPI_Type_free(&word_document_str_type);

	if(rank == 0) {
		// Sort strings and print to file
		qsort(strings, totalNumberOfUniqueWordsFromOtherWorkers, sizeof(char)*MAX_STRING_LENGTH, myCompare);
		FILE* fp = fopen("output.txt", "w");
		if(fp == NULL){
			printf("Error Opening File: output.txt\n");
			exit(0);
		}
		for(i=0; i<totalNumberOfUniqueWordsFromOtherWorkers; i++)
			fprintf(fp, "%s\n", strings[i]);
		fclose(fp);
		free(numberOfTFEntriesFromWorker);
		free(recv_counts);
		free(displs);
		free(gatherRequests);
	}
	else {
		free(sendRequests);
		free(receiveRequests);
		free(uniqueWordEntriesFromWorker);
		free(uniqueWordsReceivedFromOtherWorkers);
	}

	MPI_Finalize();
	return 0;
}
