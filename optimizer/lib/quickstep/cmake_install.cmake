# Install script for directory: /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "RelWithDebInfo")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/googletest/googletest/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/benchmark/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/farmhash/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/gflags/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/re2/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/linenoise/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/tmb/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/catalog/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/cli/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/compression/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/expressions/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/parser/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_execution/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/query_optimizer/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/relational_operators/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/storage/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/threading/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/transaction/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/types/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/utility/cmake_install.cmake")
  include("/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/yarn/cmake_install.cmake")

endif()

