CC = g++ -std=c++11
CLIBS = -pthread 
CFLAGS = -g -Wall -O2 -lm -lrt
OBJ_DIR = obj
UTIL_DIR = Util
JERASURE_LIB = Jerasure/jerasure.o Jerasure/galois.o Jerasure/reed_sol.o Jerasure/cauchy.o 
all: tinyxml2.o Config.o Coordinator.o PeerNode.o FastPRPeerNode FastPRCoordinator FastPRHotStandby

tinyxml2.o: $(UTIL_DIR)/tinyxml2.cpp $(UTIL_DIR)/tinyxml2.h
	$(CC) $(CFLAGS) -c $<  

Config.o: Config.cc tinyxml2.o
	$(CC) $(CFLAGS) -c $<

Socket.o: Socket.cc 
	$(CC) $(CFLAGS) -c $<

Coordinator.o: Coordinator.cc Config.o Socket.o
	$(CC) $(CFLAGS) -c $<

PeerNode.o: PeerNode.cc Socket.o
	$(CC) $(CFLAGS) -c $<
 
FastPRPeerNode: FastPRPeerNode.cc PeerNode.o Config.o Socket.o tinyxml2.o
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS) $(JERASURE_LIB)

FastPRCoordinator: FastPRCoordinator.cc Coordinator.o Config.o Socket.o tinyxml2.o
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS) $(JERASURE_LIB)

FastPRHotStandby: FastPRHotStandby.cc PeerNode.o Config.o Socket.o tinyxml2.o
	$(CC) $(CFLAGS) -o $@ $^ $(CLIBS) $(JERASURE_LIB)

clean:
	rm FastPRCoordinator FastPRPeerNode FastPRHotStandby *.o 
