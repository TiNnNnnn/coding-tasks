cmake_minimum_required (VERSION 3.1)
project (Sig18)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g") 

#find_package(Boost REQUIRED COMPONENTS system thread)

include_directories(src/include)
include_directories(src/fiber/include)

add_subdirectory(src)

add_library(database 
  src/Relation.cpp 
  src/Operators.cpp 
  src/Parser.cpp 
  src/Utils.cpp 
  src/Joiner.cpp 
  src/MemoryPool.cpp 
  )

target_include_directories(database PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

OPTION(FORCE_TESTS "Build tests, regardless of build type." ON)
if (CMAKE_BUILD_TYPE MATCHES "[Dd][Ee][Bb][Uu][Gg]" OR FORCE_TESTS)
#add_subdirectory(test)
endif()

link_directories(${CMAKE_SOURCE_DIR}/lib)

#构建可执行文件
add_executable(Driver src/main/main.cpp)
#target_link_libraries(Driver database)
#target_link_libraries(Driver database Boost::system Boost::thread tbb pthread m stdc++ dl jemalloc fiber_lib)
target_link_libraries(Driver database pthread m stdc++ dl jemalloc fiber_lib)

add_executable(Query2SQL src/main/Query2SQL.cpp)
target_link_libraries(Query2SQL database)

# Test harness
add_executable(harness src/main/harness.cpp)

ADD_CUSTOM_TARGET(link_target ALL
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/workloads
  ${CMAKE_CURRENT_BINARY_DIR}/workloads)
#add_definitions(-DMONITOR_ASYNC_JOIN)
#add_definitions(-DVERBOSE)
