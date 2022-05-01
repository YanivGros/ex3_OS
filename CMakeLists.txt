cmake_minimum_required(VERSION 3.16)
project(ex3_OS)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS -pthread)
add_executable(ex3_OS Atomic/atomicdemo.cpp Atomic/atomicdemo.cpp MapReduceFramework.cpp)
add_executable(ex3_OS_1 Atomic/atomicdemo_multivar.cpp)
add_executable(ex3_OS_Barrier Barrier/barrierdemo.cpp Barrier/Barrier.cpp Barrier/Barrier.h)
add_executable(ex3_OS_3 SampleClient/SampleClient.cpp MapReduceClient.h MapReduceFramework.h MapReduceFramework.cpp Barrier/Barrier.h Barrier/Barrier.cpp)
#add_executable(ex_3_os_4 MapReduceFramework.h)

