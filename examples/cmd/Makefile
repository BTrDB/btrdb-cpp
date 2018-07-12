all: dynamic static

dynamic: main.o
	g++ -std=c++11 -pthread -ggdb3 main.cpp -L/usr/local/lib -lbtrdb -lgrpc++ -lgrpc -lprotobuf -lpthread -ldl -o btrdb-cmd

static: main.o
	g++ -std=c++11 -pthread -ggdb3 main.o -L/usr/local/lib -Wl,-Bstatic -lbtrdb -lgrpc++ -lgrpc -lprotobuf -lz -lssl -lcrypto -Wl,-Bdynamic -lpthread -ldl -o btrdb-cmd-static

main.o: main.cpp
	g++ -std=c++11 -pthread -ggdb3 -c main.cpp -o main.o

clean:
	rm -f *.o btrdb-cmd btrdb-cmd-static
