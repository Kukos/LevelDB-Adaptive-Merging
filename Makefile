APP_DIR := ./app
INC_DIR := ./include
SRC_DIR := ./src
SUBMODULES_DIR := ./external/submodules
LEVELDB_DIR := $(SUBMODULES_DIR)/leveldb

LEVELDB_HEADERS := -I $(LEVELDB_DIR)/include/
LEVELDB_LIB := -L$(LEVELDB_DIR)/build -lleveldb

CXX := clang++

CXX_WARN := -Weverything
CXX_STD := -std=c++17
CXX_OPT := -O3
CXX_LIN := -fno-rtti
CXX_DEF := -DLEVELDB_PLATFORM_POSIX=1
CXX_LIB := -lpthread $(LEVELDB_LIB)
CXX_INC := -I$(INC_DIR) $(LEVELDB_HEADERS)
CXX_FLAGS := $(CXX_WARN) $(CXX_STD) $(CXX_OPT) $(CXX_LIN) $(CXX_DEF) $(CXX_LIB) $(CXX_INC)

APP_NAME := ./main.out

SRCS := $(wildcard $(SRC_DIR)/*.cpp) $(wildcard $(APP_DIR)/*.cpp)
HDRS := $(wildcard $(INC_DIR)/*.hpp)

all:
	@$(MAKE) app --no-print-directory

.PHONY: app
app: leveldb $(SRCS) $(HDRS)
	$(CXX) $(SRCS) $(CXX_FLAGS) -o $(APP_NAME)


.PHONY: leveldb
leveldb:
	if [ ! -f $(LEVELDB_DIR)/build/libleveldb.a  ]; then \
		git submodule update --init --recursive \
		cd $(LEVELDB_DIR)  && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .; \
	fi


.PHONY: clean
clean:
	rm -rf $(APP_NAME)

