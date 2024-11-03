cmake_minimum_required(VERSION 3.10)

# set the project name and version
project(Hnswdb VERSION 0.1)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

file(
  DOWNLOAD
  https://github.com/cpm-cmake/CPM.cmake/releases/download/v0.38.3/CPM.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/cmake/CPM.cmake
  EXPECTED_HASH SHA256=cc155ce02e7945e7b8967ddfaff0b050e958a723ef7aad3766d368940cb15494
)
include(${CMAKE_CURRENT_BINARY_DIR}/cmake/CPM.cmake)

CPMAddPackage("gh:RedSpah/xxhash_cpp#ad24e1c174f855cfdebc22ad4e678fd3fee3b943")
add_library(bitcask-db SHARED src/bitcask-db.cpp)
target_link_libraries(bitcask-db PUBLIC xxhash_cpp)

add_executable(bitcask-test test/test.cpp)
target_link_libraries(bitcask-test PUBLIC bitcask-db)
target_include_directories(bitcask-test PUBLIC src)