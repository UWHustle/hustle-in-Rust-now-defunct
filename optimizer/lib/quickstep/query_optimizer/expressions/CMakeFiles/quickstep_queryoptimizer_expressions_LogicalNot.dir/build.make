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

# Include any dependencies generated for this target.
include quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/depend.make

# Include the progress variables for this target.
include quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/progress.make

# Include the compile flags for this target's objects.
include quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/flags.make

quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.o: quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/flags.make
quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.o: quickstep/query_optimizer/expressions/LogicalNot.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/kevingaffney/Dev/hustle/optimizer/lib/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.o"
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.o -c /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions/LogicalNot.cpp

quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.i"
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions/LogicalNot.cpp > CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.i

quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.s"
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions && /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions/LogicalNot.cpp -o CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.s

# Object files for target quickstep_queryoptimizer_expressions_LogicalNot
quickstep_queryoptimizer_expressions_LogicalNot_OBJECTS = \
"CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.o"

# External object files for target quickstep_queryoptimizer_expressions_LogicalNot
quickstep_queryoptimizer_expressions_LogicalNot_EXTERNAL_OBJECTS =

quickstep/query_optimizer/expressions/libquickstep_queryoptimizer_expressions_LogicalNot.a: quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/LogicalNot.cpp.o
quickstep/query_optimizer/expressions/libquickstep_queryoptimizer_expressions_LogicalNot.a: quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/build.make
quickstep/query_optimizer/expressions/libquickstep_queryoptimizer_expressions_LogicalNot.a: quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/Users/kevingaffney/Dev/hustle/optimizer/lib/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Linking CXX static library libquickstep_queryoptimizer_expressions_LogicalNot.a"
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions && $(CMAKE_COMMAND) -P CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/cmake_clean_target.cmake
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/build: quickstep/query_optimizer/expressions/libquickstep_queryoptimizer_expressions_LogicalNot.a

.PHONY : quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/build

quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/clean:
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions && $(CMAKE_COMMAND) -P CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/cmake_clean.cmake
.PHONY : quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/clean

quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/depend:
	cd /Users/kevingaffney/Dev/hustle/optimizer/lib && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/kevingaffney/Dev/hustle/optimizer/lib /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions /Users/kevingaffney/Dev/hustle/optimizer/lib /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : quickstep/query_optimizer/expressions/CMakeFiles/quickstep_queryoptimizer_expressions_LogicalNot.dir/depend
