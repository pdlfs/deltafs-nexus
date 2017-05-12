#
# CMakeLists.txt  cmake file for deltafs-preload directory
# 12-May-2017  chuck@ece.cmu.edu
#

#
# this file is either included from ../CMakeLists.txt or some other
# file if we are being embedded within another project.
#

#
# create the library target
#
add_library (deltafs-nexus deltafs-nexus.cc)

# XXX: Remove when we add to deltafs-umbrella
target_include_directories (deltafs-nexus PUBLIC ${MERCURY_INCLUDE_DIR})
target_link_libraries (deltafs-nexus mercury Threads::Threads)

#
# make sure we link with MPI
#

# mpich on ub14 gives a leading space that we need to trim off
foreach (lcv ${MPI_CXX_COMPILE_FLAGS_LIST})
    if (NOT ${lcv} STREQUAL "")
        target_compile_options (deltafs-nexus
                                PUBLIC $<BUILD_INTERFACE:${lcv}>)
    endif ()
endforeach ()

# XXX: have to do this one dir at a time otherwise, otherwise I get
# error: target 'deltafs' INTERFACE_INCLUDE_DIRECTORIES contains path
#         prefixed in the source directory
foreach (lcv ${MPI_CXX_INCLUDE_PATH})
    target_include_directories (deltafs-nexus
                                PUBLIC $<BUILD_INTERFACE:${lcv}>)
endforeach ()

foreach (lcv ${MPI_CXX_LIBRARIES})
    target_link_libraries(deltafs-nexus $<BUILD_INTERFACE:${lcv}>)
endforeach ()

# XXX: this doesn't propagate to lib users, is that a problem?
# XXX: prob ok.
set_property (TARGET deltafs-nexus APPEND PROPERTY LINK_FLAGS
              ${MPI_CXX_LINK_FLAGS})

#
# installation stuff
#

# "make install" rules...
install (TARGETS deltafs-nexus
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
