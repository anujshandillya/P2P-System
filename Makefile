CXX ?= g++
CPPFLAGS := -Iinclude -D_FILE_OFFSET_BITS=64
CXXFLAGS ?= -std=c++17 -O2 -g -Wall -Wextra -Wpedantic -pthread
LDFLAGS ?= -pthread
BUILD ?= build
BIN ?= .

COMMON := src/common/protocol.cpp src/common/net.cpp src/common/sha1.cpp src/common/file.cpp
TRACKER := src/tracker/main.cpp src/tracker/server.cpp src/tracker/state.cpp src/tracker/journal.cpp
CLIENT := src/client/main.cpp src/client/cli.cpp src/client/transfer.cpp
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

$(BUILD)/file_probe: $(patsubst %.cpp,$(BUILD)/%.o,$(COMMON) tests/file_probe.cpp)
	@mkdir -p $(@D)
	$(CXX) $^ $(LDFLAGS) -o $@

.PHONY: test-files
test-files: $(BUILD)/file_probe
	python3 tests/file_inspection.py "$(BUILD)/file_probe"

test: all test-files
	python3 tests/integration.py --bin-dir "$(BIN)"
	python3 tests/publication.py "$(BIN)"
	python3 tests/transfer.py "$(BIN)"
	python3 tests/recovery.py "$(BIN)"

sanitize:
	$(MAKE) BUILD=build/sanitize BIN=build/sanitize/bin CXXFLAGS='-std=c++17 -O1 -g -Wall -Wextra -Wpedantic -pthread -fsanitize=address,undefined -fno-omit-frame-pointer' LDFLAGS='-pthread -fsanitize=address,undefined' all test-files

test-sanitize: sanitize
	python3 tests/integration.py --bin-dir build/sanitize/bin
	python3 tests/publication.py build/sanitize/bin
	python3 tests/transfer.py build/sanitize/bin
	python3 tests/recovery.py build/sanitize/bin

thread-sanitize:
	$(MAKE) BUILD=build/tsan BIN=build/tsan/bin CXXFLAGS='-std=c++17 -O1 -g -Wall -Wextra -Wpedantic -pthread -fsanitize=thread -fno-omit-frame-pointer' LDFLAGS='-pthread -fsanitize=thread' all test-files

test-thread-sanitize: thread-sanitize
	python3 tests/integration.py --bin-dir build/tsan/bin
	python3 tests/publication.py build/tsan/bin
	python3 tests/transfer.py build/tsan/bin
	python3 tests/recovery.py build/tsan/bin

clean:
	rm -rf build tracker client "$(BIN)/tracker.out" "$(BIN)/client.out"

-include $(TRACKER_OBJ:.o=.d) $(CLIENT_OBJ:.o=.d)
-include $(BUILD)/tests/file_probe.d
