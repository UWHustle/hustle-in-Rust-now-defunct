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

# Utility rule file for gflags.

# Include the progress variables for this target.
include quickstep/third_party/gflags/CMakeFiles/gflags.dir/progress.make

gflags: quickstep/third_party/gflags/CMakeFiles/gflags.dir/build.make

.PHONY : gflags

# Rule to build all files generated by this target.
quickstep/third_party/gflags/CMakeFiles/gflags.dir/build: gflags

.PHONY : quickstep/third_party/gflags/CMakeFiles/gflags.dir/build

quickstep/third_party/gflags/CMakeFiles/gflags.dir/clean:
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/gflags && $(CMAKE_COMMAND) -P CMakeFiles/gflags.dir/cmake_clean.cmake
.PHONY : quickstep/third_party/gflags/CMakeFiles/gflags.dir/clean

quickstep/third_party/gflags/CMakeFiles/gflags.dir/depend:
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/kevingaffney/Dev/hustle/optimizer/lib /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/src/gflags /Users/kevingaffney/Dev/hustle/optimizer/lib /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/gflags /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/gflags/CMakeFiles/gflags.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : quickstep/third_party/gflags/CMakeFiles/gflags.dir/depend

