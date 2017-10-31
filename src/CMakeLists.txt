#
# CMakeLists.txt  cmake file for deltafs-nexus library
# 12-May-2017  chuck@ece.cmu.edu
#

# list of source files
set (deltafs-nexus-srcs nexus_setup.cc nexus_iter.cc nexus_map.cc)

#
# configure/load in standard modules we plan to use and probe the enviroment
#
include (CMakePackageConfigHelpers)

set (CMAKE_THREAD_PREFER_PTHREAD TRUE)
set (THREADS_PREFER_PTHREAD_FLAG TRUE)

# XXX: cmake 3.1 and newer define a Threads::Threads imported target
# that we should switch too when we are ready to require 3.1 or better.
# (3.1 was released late 2014)

# where to install files for "find_package"
set (deltafs-nexus-pkg-loc "share/cmake/deltafs-nexus")

#
# library version set here (e.g. for shared libs).
#
set (DELTAFS_NEXUS_VERSION_MAJOR 1)
set (DELTAFS_NEXUS_VERSION_MINOR 0)
set (DELTAFS_NEXUS_VERSION_PATCH 0)
set (deltafs-nexus-vers 
    "${DELTAFS_NEXUS_VERSION_MAJOR}.${DELTAFS_NEXUS_VERSION_MINOR}")
set (DELTAFS_NEXUS_VERSION 
    "${deltafs-nexus-vers}.${DELTAFS_NEXUS_VERSION_PATCH}")

#
# create library target (user can specify shared vs. static using
# BUILD_SHARED_LIBS).  arrange for users of our lib to get the correct
# -Is for our include directories.
#
add_library (deltafs-nexus ${deltafs-nexus-srcs})

target_include_directories (deltafs-nexus PUBLIC $<INSTALL_INTERFACE:include>)
# make sure our build includes are BEFORE a previously installed version
target_include_directories (deltafs-nexus BEFORE PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)

target_link_libraries (deltafs-nexus mercury)
if (CMAKE_THREAD_LIBS_INIT)
  target_link_libraries (deltafs-nexus "${CMAKE_THREAD_LIBS_INIT}")
endif ()

set_target_properties(deltafs-nexus
                      PROPERTIES VERSION ${DELTAFS_NEXUS_VERSION}
                      SOVERSION ${DELTAFS_NEXUS_VERSION_MAJOR})

# special handling for MPI, where the config comes in via MPI_<lang>_ vars.
# Note that the mpich on ub14 gives a leading space that we need to trim off.
string (REPLACE " " ";" mpicxx_flags "${MPI_CXX_COMPILE_FLAGS}")
foreach (lcv ${mpicxx_flags})
    if (NOT ${lcv} STREQUAL "")
        target_compile_options (deltafs-nexus PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endif ()
endforeach ()

# XXX: have to do this one dir at a time otherwise, otherwise I get
# error: target 'deltafs' INTERFACE_INCLUDE_DIRECTORIES contains path
#         prefixed in the source directory
foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (deltafs-nexus PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()
foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(deltafs-nexus $<BUILD_INTERFACE:${lcv}>)
endforeach ()
# XXX: this doesn't propagate to lib users, is that a problem?
# XXX: prob ok.
set_property (TARGET deltafs-nexus APPEND PROPERTY LINK_FLAGS
              ${MPI_CXX_LINK_FLAGS})
get_property (link_flags TARGET deltafs-nexus PROPERTY LINK_FLAGS)

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "deltafs-nexus-config-version.cmake"
    VERSION $DELTAFS_NEXUS_VERSIONBBOS_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (deltafs-nexus-config.cmake.in deltafs-nexus-config.cmake @ONLY)

#
# "make install" rules
#
install (TARGETS deltafs-nexus EXPORT deltafs-nexus-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
install (EXPORT deltafs-nexus-targets
         DESTINATION ${deltafs-nexus-pkg-loc}
         FILE "deltafs-nexus-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/deltafs-nexus-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/deltafs-nexus-config-version.cmake"
         DESTINATION ${deltafs-nexus-pkg-loc} )
install (DIRECTORY ../include/
         DESTINATION include/deltafs-nexus
         FILES_MATCHING PATTERN "*.h")
