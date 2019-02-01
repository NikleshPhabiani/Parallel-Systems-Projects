#Single Author info:
#nphabia Niklesh Ashok Phabiani
#Group info:
#nphabia Niklesh Ashok Phabiani
#anjain2 Akshay Narendra Jain
#rtnaik	Rohit Naik

CC = nvcc
CFLAGS = -lm -Wno-deprecated-gpu-targets

p2: p2.cu
	$(CC) $(CFLAGS) -o p2 p2.cu