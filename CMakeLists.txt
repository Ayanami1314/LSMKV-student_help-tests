cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_COMPILER "clang++")
project(my_project)

# GoogleTest requires at least C++14

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# debug on
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FALGS} -Wall -Ofast")
# link_libraries(ubsan asan)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()
set(TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/myunittests")

# Get all .cc files in the TEST_DIR directory
# the tests only depand the itself .cc
file(GLOB SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
include(GoogleTest)
foreach(src_file ${SRC_FILES})
  # Get file name without directory and extension
  get_filename_component(src_name ${src_file} NAME_WE)

  # Check if the test file exists
  set(test_file "${TEST_DIR}/${src_name}_test.cc")
  if(EXISTS ${test_file})
    # Add the source file and the test file to the executable
    add_executable(${src_name}_test ${src_file} ${test_file})

    target_link_libraries(${src_name}_test GTest::gtest_main)

    gtest_discover_tests(${src_name}_test)
  endif()
endforeach()

SET(SU "${CMAKE_CURRENT_SOURCE_DIR}/my_special_unittests")

add_executable(sstable_test "${SU}/sstable_test.cc" sstable.cc bloomfilter.cc skiplist.cc)
add_executable(kvstore_test "${SU}/kvstore_test.cc" vlog.cc kvstore.cc sstable.cc bloomfilter.cc skiplist.cc)
add_executable(kvstore_cbf_test "${SU}/kvstore_changebf_test.cc" vlog.cc kvstore.cc sstable.cc bloomfilter.cc skiplist.cc type.h)
add_executable(vlog_test "${SU}/vlog_test.cc" sstable.cc bloomfilter.cc skiplist.cc vlog.cc)

target_link_libraries(sstable_test GTest::gtest_main)
target_link_libraries(kvstore_test  GTest::gtest_main)
target_link_libraries(kvstore_cbf_test  GTest::gtest_main)
target_link_libraries(vlog_test  GTest::gtest_main)
gtest_discover_tests(sstable_test kvstore_test kvstore_cbf_test vlog_test)

SET(SRC_WITHOUT_TEST kvstore.cc bloomfilter.cc skiplist.cc sstable.cc vlog.cc)
add_executable(persistence persistence.cc ${SRC_WITHOUT_TEST})
add_executable(correctness correctness.cc ${SRC_WITHOUT_TEST})