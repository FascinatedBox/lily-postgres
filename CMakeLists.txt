cmake_minimum_required(VERSION 3.0.0)
project("lily-postgres")

set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

find_package(PQ)

include_directories(${PQ_INCLUDE_DIRS})
include_directories("${CMAKE_INSTALL_PREFIX}/include/lily/")

add_library(postgres SHARED lily_postgres.c)
target_link_libraries(postgres pq)
set_target_properties(postgres PROPERTIES PREFIX "")
