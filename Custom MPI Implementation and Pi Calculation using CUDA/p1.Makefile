#Single Author info:
#nphabia Niklesh Ashok Phabiani
#Group info:
#nphabia Niklesh Ashok Phabiani
#anjain2 Akshay Narendra Jain
#rtnaik	Rohit Naik

# Makefile to compile program that calculates RTT and standard deviation
CC = gcc
CFLAGS = -lm -O3

my_rtt: my_rtt.c
	$(CC) $(CFLAGS) -o my_rtt my_rtt.c
