C_FLAGS = -lm -pthread
CC = gcc

all: prod-cons

prod-cons: prod-cons.o
	$(CC) prod-cons.o -o prod-cons $(C_FLAGS)

prod-cons.o: prod-cons.c
	$(CC) -c prod-cons.c

clean:
	rm *.o prod-cons
