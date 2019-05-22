# Install script for directory: /Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/src/glog

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

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/libglog.a")
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libglog.a" AND
     NOT IS_SYMLINK "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libglog.a")
    execute_process(COMMAND "/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/ranlib" "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libglog.a")
  endif()
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/glog" TYPE FILE FILES
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/config.h"
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/glog/logging.h"
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/glog/raw_logging.h"
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/glog/stl_logging.h"
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/glog/vlog_is_on.h"
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/src/glog/src/glog/log_severity.h"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/glog" TYPE FILE FILES
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/glog-config.cmake"
    "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/glog-config-version.cmake"
    )
endif()

if("x${CMAKE_INSTALL_COMPONENT}x" STREQUAL "xUnspecifiedx" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets.cmake")
    file(DIFFERENT EXPORT_FILE_CHANGED FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets.cmake"
         "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/CMakeFiles/Export/lib/cmake/glog/glog-targets.cmake")
    if(EXPORT_FILE_CHANGED)
      file(GLOB OLD_CONFIG_FILES "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets-*.cmake")
      if(OLD_CONFIG_FILES)
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/glog/glog-targets.cmake\" will be replaced.  Removing files [${OLD_CONFIG_FILES}].")
        file(REMOVE ${OLD_CONFIG_FILES})
      endif()
    endif()
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/glog" TYPE FILE FILES "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/CMakeFiles/Export/lib/cmake/glog/glog-targets.cmake")
  if("${CMAKE_INSTALL_CONFIG_NAME}" MATCHES "^([Rr][Ee][Ll][Ww][Ii][Tt][Hh][Dd][Ee][Bb][Ii][Nn][Ff][Oo])$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/glog" TYPE FILE FILES "/Users/kevingaffney/Dev/hustle/optimizer/lib/quickstep/third_party/glog/CMakeFiles/Export/lib/cmake/glog/glog-targets-relwithdebinfo.cmake")
  endif()
endif()

