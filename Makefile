CXX = g++
CXXFLAGS = -std=c++11 -ggdb3 -I/usr/local/include -pthread -fPIC -Wall -Wpedantic -c

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`
PROTOS_PATH=./proto

STATIC_TARGET = libbtrdb.a
DYNAMIC_TARGET = libbtrdb.so.1.0
VERSION_SO_NAME = libbtrdb.so.1
BARE_SO_NAME = libbtrdb.so

INSTALL_LIB_DIR = /usr/local/lib
INSTALL_HDR_DIR = /usr/local/include

CPP_SRCS = $(wildcard *.cpp)
CPP_HDRS = $(wildcard *.h)
CPP_OBJS = $(patsubst %.cpp,%.o,$(CPP_SRCS))

ALL_OBJS = $(CPP_OBJS) btrdb.grpc.pb.o btrdb.pb.o

all: $(STATIC_TARGET) $(DYNAMIC_TARGET)

.PHONY: install
install: $(STATIC_TARGET) $(DYNAMIC_TARGET)
	cp $(STATIC_TARGET) $(INSTALL_LIB_DIR)
	cp $(DYNAMIC_TARGET) $(INSTALL_LIB_DIR)
	ln -f -s $(INSTALL_LIB_DIR)/$(DYNAMIC_TARGET) $(INSTALL_LIB_DIR)/$(VERSION_SO_NAME)
	ln -f -s $(INSTALL_LIB_DIR)/$(DYNAMIC_TARGET) $(INSTALL_LIB_DIR)/$(BARE_SO_NAME)
	mkdir -p $(INSTALL_HDR_DIR)/btrdb
	rm -rf $(INSTALL_HDR_DIR)/btrdb/*
	cp *.h $(INSTALL_HDR_DIR)/btrdb
	ldconfig

$(DYNAMIC_TARGET): $(ALL_OBJS)
	g++ -shared -Wl,-soname,libbtrdb.so.1 -o $(DYNAMIC_TARGET) $(ALL_OBJS)

$(STATIC_TARGET): $(ALL_OBJS)
	ar rcs $(STATIC_TARGET) $(ALL_OBJS)

%.o: %.cpp $(CPP_HDRS) btrdb.grpc.pb.cc btrdb.pb.cc
	$(CXX) $(CXXFLAGS) $< -o $@

%.pb.o: %.pb.cc
	$(CXX) $(CXXFLAGS) $< -o $@

proto: btrdb.grpc.pb.cc btrdb.pb.cc

btrdb.grpc.pb.cc: $(PROTOS_PATH)/btrdb.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

btrdb.pb.cc: $(PROTOS_PATH)/btrdb.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h $(STATIC_TARGET) $(DYNAMIC_TARGET)
