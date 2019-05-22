#----------------------------------------------------------------
# Generated CMake target import file for configuration "RelWithDebInfo".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "gflags-static" for configuration "RelWithDebInfo"
set_property(TARGET gflags-static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(gflags-static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELWITHDEBINFO "CXX"
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libgflags.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags-static )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags-static "${_IMPORT_PREFIX}/lib/libgflags.a" )

# Import target "gflags_nothreads-static" for configuration "RelWithDebInfo"
set_property(TARGET gflags_nothreads-static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELWITHDEBINFO)
set_target_properties(gflags_nothreads-static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELWITHDEBINFO "CXX"
  IMPORTED_LOCATION_RELWITHDEBINFO "${_IMPORT_PREFIX}/lib/libgflags_nothreads.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS gflags_nothreads-static )
list(APPEND _IMPORT_CHECK_FILES_FOR_gflags_nothreads-static "${_IMPORT_PREFIX}/lib/libgflags_nothreads.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
