cmake_minimum_required(VERSION 3.10.2)
if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.12)
  cmake_policy(SET CMP0074 NEW)  # Controls how find_package() uses variables for searching
endif()

# A place to put any FindXXX.cmake files we need to create
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/libs/cmake/Modules/")

project(HVAC 
	VERSION 0.0.1
        DESCRIPTION "A client and server library for implementing a distributed machine learning cache"
		LANGUAGES C CXX)

#Some Parts are C
set(CMAKE_C_STANDARD 11)
set(C_STD_REQUIRED ON)

#Some Parts are C++
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_CXX_EXTENSIONS OFF)
#set(CXX_STD_REQUIRED ON)

#Basename is used a fair amount
#This seems to be the best place to define this debacle
add_definitions(-D_GNU_SOURCE)

if (NOT CMAKE_BUILD_TYPE)
  # Build type hasn't been set, which probably means there's no cache to read
  # from.  Set defaults for the build type and a few other variables.
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -DDEBUG" CACHE STRING
     "Flags used by the compiler during debug builds." FORCE)
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -DDEBUG" CACHE STRING
      "Flags used by the compiler during release builds with debug info." FORCE)
  # Note: I'm not convinced this is the best place to change the C_FLAGS_*
  #       variables, but I wanted a method that resulted in the changes being
  #       shown in ccmake so that the user would know exactly how the code
  #       was being compiled without having to run 'make VERBOSE=1'     
  # Note: -Wall is *NOT* cross-platform.  It'll probably work with all the
  #        Unix-based compilers, though.
endif()

find_package(PkgConfig REQUIRED)

add_subdirectory(src)

add_subdirectory(tests)

