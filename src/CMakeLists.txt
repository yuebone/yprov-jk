cmake_minimum_required(VERSION 2.8)
project(yprov)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fpic -g")

include_directories(${PROJECT_SOURCE_DIR})
include_directories(/home/bon/db/pg/pg9.5-bin/include/postgresql/server)

add_definitions(-DY_PRINT_ON)

aux_source_directory(${PROJECT_SOURCE_DIR}/prov PROV_SRC)
aux_source_directory(${PROJECT_SOURCE_DIR}/utils UTILS_SRC)
aux_source_directory(${PROJECT_SOURCE_DIR}/utils/rbt RBT_SRC)
aux_source_directory(${PROJECT_SOURCE_DIR}/utils/json JSON_SRC)

add_library(yprov_so SHARED ${PROV_SRC} ${UTILS_SRC} ${RBT_SRC} ${JSON_SRC})
set_target_properties(yprov_so PROPERTIES OUTPUT_NAME "yprov")
