#
# Copyright (c) 2017 Carnegie Mellon University.
# George Amvrosiadis <gamvrosi@cs.cmu.edu>
#
# All rights reserved.
#
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.
#

#
# CMakeLists.txt  cmake file for deltafs-shuffle tests/ directory
#

#
# test programs
#
set (tests nexus)

foreach (tst ${tests})

    add_executable (${tst}-test ${tst}-test.cc)
    target_link_libraries (${tst}-test deltafs-nexus)

    add_test (${tst}-single ${CMAKE_CURRENT_SOURCE_DIR}/${tst}-test.sh
                            ${CMAKE_BINARY_DIR} 1)
    add_test (${tst}-multi  ${CMAKE_CURRENT_SOURCE_DIR}/${tst}-test.sh
                            ${CMAKE_BINARY_DIR} 8)

endforeach ()
