CXX ?= g++
CPPFLAGS := -Iinclude -D_FILE_OFFSET_BITS=64
CXXFLAGS ?= -std=c++17 -O2 -g -Wall -Wextra -Wpedantic -pthread
LDFLAGS ?= -pthread
BUILD ?= build
BIN ?= .

COMMON := src/common/protocol.cpp src/common/net.cpp
TRACKER := src/tracker/main.cpp src/tracker/server.cpp src/tracker/state.cpp src/tracker/journal.cpp
CLIENT := src/client/main.cpp src/client/cli.cpp
TRACKER_OBJ := $(patsubst %.cpp,$(BUILD)/%.o,$(COMMON) $(TRACKER))
CLIENT_OBJ := $(patsubst %.cpp,$(BUILD)/%.o,$(COMMON) $(CLIENT))

.PHONY: all clean test sanitize test-sanitize thread-sanitize test-thread-sanitize
all: $(BIN)/tracker.out $(BIN)/client.out

$(BIN)/tracker.out: $(TRACKER_OBJ)
	@mkdir -p $(@D)
	$(CXX) $^ $(LDFLAGS) -o $@

$(BIN)/client.out: $(CLIENT_OBJ)
	@mkdir -p $(@D)
	$(CXX) $^ $(LDFLAGS) -o $@

$(BUILD)/%.o: %.cpp
	@mkdir -p $(@D)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@

test: all
	python3 tests/integration.py --bin-dir "$(BIN)"

sanitize:
	$(MAKE) BUILD=build/sanitize BIN=build/sanitize/bin CXXFLAGS='-std=c++17 -O1 -g -Wall -Wextra -Wpedantic -pthread -fsanitize=address,undefined -fno-omit-frame-pointer' LDFLAGS='-pthread -fsanitize=address,undefined' all

test-sanitize: sanitize
	python3 tests/integration.py --bin-dir build/sanitize/bin

thread-sanitize:
	$(MAKE) BUILD=build/tsan BIN=build/tsan/bin CXXFLAGS='-std=c++17 -O1 -g -Wall -Wextra -Wpedantic -pthread -fsanitize=thread -fno-omit-frame-pointer' LDFLAGS='-pthread -fsanitize=thread' all

test-thread-sanitize: thread-sanitize
	python3 tests/integration.py --bin-dir build/tsan/bin

clean:
	rm -rf build tracker client "$(BIN)/tracker.out" "$(BIN)/client.out"

-include $(TRACKER_OBJ:.o=.d) $(CLIENT_OBJ:.o=.d)
