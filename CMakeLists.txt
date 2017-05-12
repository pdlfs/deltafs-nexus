#
# CMakeLists.txt  cmake for deltafs-nexus
# 11-May-2017  chuck@ece.cmu.edu
#

#
# specify the location of mercury with CMAKE_INSTALL_PATH or
# CMAKE_PREFIX_PATH...
#
cmake_minimum_required(VERSION 3.1)
project (deltafs-nexus C CXX)
enable_testing ()

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# nexus library is always shared, force on and hide it as an advanced var
set (BUILD_SHARED_LIBS On CACHE STRING "Build shared libs (must be on)." FORCE)
mark_as_advanced (BUILD_SHARED_LIBS)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (DEBUG_SANITIZER Off CACHE STRING "Sanitizer for debug builds")
set_property (CACHE DEBUG_SANITIZER PROPERTY STRINGS
              "Off" "Address" "Thread")
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")

#
# sanitizer config (XXX: does not probe compiler to see if sanitizer flags
# are supported... )
#
set (as_flags "-fsanitize=address -O1 -fno-omit-frame-pointer")
set (ts_flags "-fsanitize=thread  -O1 -fno-omit-frame-pointer")
if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    if (${DEBUG_SANITIZER} STREQUAL "Address")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${as_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${as_flags}")
    elseif (${DEBUG_SANITIZER} STREQUAL "Thread")
        set (CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${ts_flags}")
        set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${ts_flags}")
    endif ()
endif ()

# Find required packages
find_package (mercury CONFIG REQUIRED)
find_package (MPI MODULE REQUIRED)
string (REPLACE " " ";" MPI_CXX_COMPILE_FLAGS_LIST "${MPI_CXX_COMPILE_FLAGS}")

add_subdirectory (src)
add_subdirectory (tests)
