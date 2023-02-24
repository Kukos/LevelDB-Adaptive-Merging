APP_DIR := ./app
INC_DIR := ./include
SRC_DIR := ./src
SUBMODULES_DIR := ./external/submodules

LEVELDB_DIR := $(SUBMODULES_DIR)/leveldb

LEVELDB_HEADERS := -I $(LEVELDB_DIR)/include/
LEVELDB_LIB := -L$(LEVELDB_DIR)/build -lleveldb

SPDLOG_DIR := $(SUBMODULES_DIR)/spdlog

SPDLOG_HEADERS := -I $(SPDLOG_DIR)/include/
SPDLOG_LIB := -L$(SPDLOG_DIR)/build -lspdlog

CXX := clang++

CXX_SPDLOG_WARN_SUPRESS := -Wno-documentation-unknown-command -Wno-c++98-compat -Wno-newline-eof -Wno-covered-switch-default -Wno-float-equal -Wno-switch-enum
CXX_WARN := -Weverything -Wno-c++98-compat-pedantic -Wno-shadow-field-in-constructor \
 			-Wno-weak-vtables -Wno-old-style-cast -Wno-padded -Wno-global-constructors -Wno-exit-time-destructors \
			-Wno-ctad-maybe-unsupported $(CXX_SPDLOG_WARN_SUPRESS)
CXX_STD := -std=c++17
CXX_OPT := -O3
CXX_LIN := -fno-rtti
CXX_DEF := -DLEVELDB_PLATFORM_POSIX=1
CXX_LIB := -lpthread $(LEVELDB_LIB) $(SPDLOG_LIB)
CXX_INC := -I$(INC_DIR) $(LEVELDB_HEADERS) $(SPDLOG_HEADERS)
CXX_FLAGS := $(CXX_WARN) $(CXX_STD) $(CXX_OPT) $(CXX_LIN) $(CXX_DEF) $(CXX_LIB) $(CXX_INC)

APP_NAME := ./main.out

SRCS := $(wildcard $(SRC_DIR)/*.cpp) $(wildcard $(APP_DIR)/*.cpp)
HDRS := $(wildcard $(INC_DIR)/*.hpp)

all:
	@$(MAKE) app --no-print-directory

.PHONY: app
app: leveldb spdlog $(SRCS) $(HDRS)
	$(CXX) $(SRCS) $(CXX_FLAGS) -o $(APP_NAME)


.PHONY: leveldb
leveldb:
	if [ ! -f $(LEVELDB_DIR)/build/libleveldb.a  ]; then \
		git submodule update --init --recursive ; \
		cd $(LEVELDB_DIR) && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .; \
	fi

.PHONY: spdlog
spdlog:
	if [ ! -f $(SPDLOG_DIR)/build/libspdlog.a  ]; then \
		git submodule update --init --recursive ; \
		cd $(SPDLOG_DIR) && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .; \
	fi


.PHONY: clean
clean:
	rm -rf $(APP_NAME)

