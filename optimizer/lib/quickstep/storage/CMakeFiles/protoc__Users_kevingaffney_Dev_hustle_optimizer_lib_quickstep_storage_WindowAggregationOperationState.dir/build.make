# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.12

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/local/Cellar/cmake/3.12.4/bin/cmake

# The command to remove a file.
RM = /usr/local/Cellar/cmake/3.12.4/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/kevingaffney/Dev/hustle/optimizer/lib

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/kevingaffney/Dev/hustle/optimizer/lib

# Utility rule file for protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.

# Include the progress variables for this target.
include quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/progress.make

quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState: quickstep/storage/WindowAggregationOperationState.pb.h


quickstep/storage/WindowAggregationOperationState.pb.cc: quickstep/storage/WindowAggregationOperationState.proto
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --blue --bold --progress-dir=/Users/kevingaffney/Dev/hustle/optimizer/lib/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Running C++ protocol buffer compiler on WindowAggregationOperationState.proto"
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/storage && /usr/local/bin/protoc --cpp_out /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep -I/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/storage/WindowAggregationOperationState.proto

quickstep/storage/WindowAggregationOperationState.pb.h: quickstep/storage/WindowAggregationOperationState.pb.cc
	@$(CMAKE_COMMAND) -E touch_nocreate quickstep/storage/WindowAggregationOperationState.pb.h

protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState: quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState
protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState: quickstep/storage/WindowAggregationOperationState.pb.cc
protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState: quickstep/storage/WindowAggregationOperationState.pb.h
protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState: quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/build.make

.PHONY : protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState

# Rule to build all files generated by this target.
quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/build: protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState

.PHONY : quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/build

quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/clean:
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/storage && $(CMAKE_COMMAND) -P CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/cmake_clean.cmake
.PHONY : quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/clean

quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/depend:
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/kevingaffney/Dev/hustle/optimizer/lib /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/storage /Users/kevingaffney/Dev/hustle/optimizer/lib /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/storage /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : quickstep/storage/CMakeFiles/protoc__Users_kevingaffney_Dev_hustle_optimizer_lib_quickstep_storage_WindowAggregationOperationState.dir/depend

