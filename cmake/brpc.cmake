# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

include_guard()

include(cmake/utils.cmake)
include(CMakeParseArguments)
include(ProcessorCount)

get_filename_component(_BRPC_PROJECT_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)

set(BRPC_THIRD_PARTY_DIR "${_BRPC_PROJECT_DIR}/third_party" CACHE PATH
    "Base directory for bRPC third-party sources and toolchain artifacts")
set(BRPC_SOURCE_CACHE_DIR "${BRPC_THIRD_PARTY_DIR}/src-cache" CACHE PATH
    "Directory used to cache downloaded source trees for bRPC dependencies")
set(BRPC_TOOLCHAIN_DIR "${BRPC_THIRD_PARTY_DIR}/toolchain" CACHE PATH
    "Directory used to install built bRPC third-party dependencies")
option(BRPC_ALLOW_SYSTEM_PACKAGE
       "Allow using a system-installed brpc package instead of the project-managed toolchain"
       OFF)

set(BRPC_GFLAGS_VERSION "2.2.2" CACHE STRING "Pinned gflags version used by the project-managed brpc toolchain")
set(BRPC_PROTOBUF_VERSION "3.19.0" CACHE STRING "Pinned protobuf version used by the project-managed brpc toolchain")
set(BRPC_PROTOBUF_COMPILER_VERSION "3.19.0.0" CACHE STRING
    "Installed protoc version string used by the project-managed brpc toolchain")
set(BRPC_LEVELDB_VERSION "1.23" CACHE STRING "Pinned leveldb version used by the project-managed brpc toolchain")
set(BRPC_VERSION "1.16.0" CACHE STRING "Pinned brpc version used by the project-managed brpc toolchain")
set(BRPC_BUILD_JOBS "" CACHE STRING
    "Override the parallel job count used to build project-managed brpc dependencies")

set(BRPC_GFLAGS_SOURCE_URL "https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.zip" CACHE STRING
    "Source archive URL for gflags")
set(BRPC_GFLAGS_SOURCE_URL_HASH "" CACHE STRING
    "Optional URL_HASH for the gflags source archive")
set(BRPC_PROTOBUF_SOURCE_URL "https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.19.0.zip" CACHE STRING
    "Source archive URL for protobuf")
set(BRPC_PROTOBUF_SOURCE_URL_HASH "" CACHE STRING
    "Optional URL_HASH for the protobuf source archive")
set(BRPC_LEVELDB_SOURCE_URL "https://github.com/google/leveldb/archive/refs/tags/1.23.zip" CACHE STRING
    "Source archive URL for leveldb")
set(BRPC_LEVELDB_SOURCE_URL_HASH "" CACHE STRING
    "Optional URL_HASH for the leveldb source archive")
set(BRPC_SOURCE_URL "https://github.com/apache/brpc/archive/refs/tags/1.16.0.zip" CACHE STRING
    "Source archive URL for brpc")
set(BRPC_SOURCE_URL_HASH "" CACHE STRING
    "Optional URL_HASH for the brpc source archive")

set(BRPC_ROOT "${BRPC_TOOLCHAIN_DIR}/brpc-build" CACHE PATH "Prefix of a prebuilt brpc installation")
set(BRPC_LIBRARY "" CACHE FILEPATH "Path to libbrpc.a")
set(BRPC_INCLUDE_DIR "" CACHE PATH "Path to brpc headers")
set(GFLAGS_ROOT "${BRPC_TOOLCHAIN_DIR}/gflags-build" CACHE PATH "Prefix of a prebuilt gflags installation")
set(GFLAGS_LIBRARY "" CACHE FILEPATH "Path to libgflags.a")
set(GFLAGS_INCLUDE_DIR "" CACHE PATH "Path to gflags headers")
set(PROTOBUF_ROOT "${BRPC_TOOLCHAIN_DIR}/protobuf-build" CACHE PATH "Prefix of a prebuilt protobuf installation")
set(PROTOBUF_LIBRARY "" CACHE FILEPATH "Path to libprotobuf.a")
set(PROTOBUF_PROTOC_LIBRARY "" CACHE FILEPATH "Path to libprotoc.a")
set(PROTOBUF_INCLUDE_DIR "" CACHE PATH "Path to protobuf headers")
set(PROTOBUF_PROTOC_EXECUTABLE "" CACHE FILEPATH "Path to protoc executable")
set(LEVELDB_ROOT "${BRPC_TOOLCHAIN_DIR}/leveldb-build" CACHE PATH "Prefix of a prebuilt leveldb installation")
set(LEVELDB_LIBRARY "" CACHE FILEPATH "Path to libleveldb.a")
set(LEVELDB_INCLUDE_DIR "" CACHE PATH "Path to leveldb headers")

set(GFLAGS_SOURCE_DIR "${BRPC_SOURCE_CACHE_DIR}/gflags-src")
set(PROTOBUF_SOURCE_DIR "${BRPC_SOURCE_CACHE_DIR}/protobuf-src")
set(LEVELDB_SOURCE_DIR "${BRPC_SOURCE_CACHE_DIR}/leveldb-src")
set(BRPC_SOURCE_DIR "${BRPC_SOURCE_CACHE_DIR}/brpc-src")

set(GFLAGS_INSTALL_DIR "${GFLAGS_ROOT}")
set(PROTOBUF_INSTALL_DIR "${PROTOBUF_ROOT}")
set(LEVELDB_INSTALL_DIR "${LEVELDB_ROOT}")
set(BRPC_INSTALL_DIR "${BRPC_ROOT}")

if("${BRPC_LIBRARY}" STREQUAL "" AND EXISTS "${BRPC_ROOT}/lib64/libbrpc.a")
    set(BRPC_LIBRARY "${BRPC_ROOT}/lib64/libbrpc.a")
endif()
if("${BRPC_INCLUDE_DIR}" STREQUAL "" AND EXISTS "${BRPC_ROOT}/include/brpc/channel.h")
    set(BRPC_INCLUDE_DIR "${BRPC_ROOT}/include")
endif()
if("${GFLAGS_LIBRARY}" STREQUAL "" AND EXISTS "${GFLAGS_ROOT}/lib/libgflags.a")
    set(GFLAGS_LIBRARY "${GFLAGS_ROOT}/lib/libgflags.a")
endif()
if("${GFLAGS_INCLUDE_DIR}" STREQUAL "" AND EXISTS "${GFLAGS_ROOT}/include/gflags/gflags.h")
    set(GFLAGS_INCLUDE_DIR "${GFLAGS_ROOT}/include")
endif()
if("${PROTOBUF_LIBRARY}" STREQUAL "" AND EXISTS "${PROTOBUF_ROOT}/lib64/libprotobuf.a")
    set(PROTOBUF_LIBRARY "${PROTOBUF_ROOT}/lib64/libprotobuf.a")
endif()
if("${PROTOBUF_PROTOC_LIBRARY}" STREQUAL "" AND EXISTS "${PROTOBUF_ROOT}/lib64/libprotoc.a")
    set(PROTOBUF_PROTOC_LIBRARY "${PROTOBUF_ROOT}/lib64/libprotoc.a")
endif()
if("${PROTOBUF_INCLUDE_DIR}" STREQUAL "" AND EXISTS "${PROTOBUF_ROOT}/include/google/protobuf/message.h")
    set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_ROOT}/include")
endif()
if("${PROTOBUF_PROTOC_EXECUTABLE}" STREQUAL "")
    if(EXISTS "${PROTOBUF_ROOT}/bin/protoc")
        set(PROTOBUF_PROTOC_EXECUTABLE "${PROTOBUF_ROOT}/bin/protoc")
    elseif(EXISTS "${PROTOBUF_ROOT}/bin/protoc-${BRPC_PROTOBUF_COMPILER_VERSION}")
        set(PROTOBUF_PROTOC_EXECUTABLE "${PROTOBUF_ROOT}/bin/protoc-${BRPC_PROTOBUF_COMPILER_VERSION}")
    endif()
endif()
if("${LEVELDB_LIBRARY}" STREQUAL "" AND EXISTS "${LEVELDB_ROOT}/lib/libleveldb.a")
    set(LEVELDB_LIBRARY "${LEVELDB_ROOT}/lib/libleveldb.a")
endif()
if("${LEVELDB_INCLUDE_DIR}" STREQUAL "" AND EXISTS "${LEVELDB_ROOT}/include/leveldb/db.h")
    set(LEVELDB_INCLUDE_DIR "${LEVELDB_ROOT}/include")
endif()

if(BRPC_BUILD_JOBS)
    set(_brpc_build_jobs "${BRPC_BUILD_JOBS}")
else()
    ProcessorCount(_brpc_detected_jobs)
    if(NOT _brpc_detected_jobs)
        set(_brpc_detected_jobs 4)
    endif()
    set(_brpc_build_jobs "${_brpc_detected_jobs}")
endif()

function(_brpc_find_local_library out_var cache_value root_var)
    if(DEFINED ${cache_value} AND NOT "${${cache_value}}" STREQUAL "")
        if(NOT EXISTS "${${cache_value}}")
            message(FATAL_ERROR
                "${cache_value} was set to '${${cache_value}}', but that library file does not exist")
        endif()
        set(${out_var} "${${cache_value}}" PARENT_SCOPE)
        return()
    endif()

    set(_lib_path "")
    if(DEFINED ${root_var} AND NOT "${${root_var}}" STREQUAL "")
        find_library(_lib_path NAMES ${ARGN}
            HINTS "${${root_var}}"
            PATH_SUFFIXES lib lib64
            NO_DEFAULT_PATH
        )
    endif()

    set(${out_var} "${_lib_path}" PARENT_SCOPE)
endfunction()

function(_brpc_find_local_path out_var cache_value root_var header_name)
    if(DEFINED ${cache_value} AND NOT "${${cache_value}}" STREQUAL "")
        if(NOT EXISTS "${${cache_value}}")
            message(FATAL_ERROR
                "${cache_value} was set to '${${cache_value}}', but that path does not exist")
        endif()
        set(${out_var} "${${cache_value}}" PARENT_SCOPE)
        return()
    endif()

    set(_include_path "")
    if(DEFINED ${root_var} AND NOT "${${root_var}}" STREQUAL "")
        find_path(_include_path "${header_name}"
            HINTS "${${root_var}}"
            PATH_SUFFIXES include
            NO_DEFAULT_PATH
        )
    endif()

    set(${out_var} "${_include_path}" PARENT_SCOPE)
endfunction()

function(_brpc_find_local_protoc out_var)
    if(DEFINED PROTOBUF_PROTOC_EXECUTABLE AND NOT "${PROTOBUF_PROTOC_EXECUTABLE}" STREQUAL "")
        if(NOT EXISTS "${PROTOBUF_PROTOC_EXECUTABLE}")
            message(FATAL_ERROR
                "PROTOBUF_PROTOC_EXECUTABLE was set to '${PROTOBUF_PROTOC_EXECUTABLE}', but that executable does not exist")
        endif()
        set(${out_var} "${PROTOBUF_PROTOC_EXECUTABLE}" PARENT_SCOPE)
        return()
    endif()

    set(_protoc_path "")
    if(DEFINED PROTOBUF_ROOT AND NOT "${PROTOBUF_ROOT}" STREQUAL "")
        find_program(_protoc_path
            NAMES protoc "protoc-${BRPC_PROTOBUF_COMPILER_VERSION}" "protoc-${BRPC_PROTOBUF_VERSION}"
            HINTS "${PROTOBUF_ROOT}"
            PATH_SUFFIXES bin
            NO_DEFAULT_PATH
        )

        if(NOT _protoc_path)
            file(GLOB _protoc_candidates LIST_DIRECTORIES FALSE "${PROTOBUF_ROOT}/bin/protoc*")
            list(SORT _protoc_candidates)
            foreach(_candidate IN LISTS _protoc_candidates)
                if(EXISTS "${_candidate}" AND NOT IS_DIRECTORY "${_candidate}")
                    set(_protoc_path "${_candidate}")
                    break()
                endif()
            endforeach()
        endif()
    endif()

    set(${out_var} "${_protoc_path}" PARENT_SCOPE)
endfunction()

function(_brpc_warn_if_missing_hash dep_name hash_value)
    if("${hash_value}" STREQUAL "")
        message(STATUS
            "${dep_name} source archive has no URL_HASH configured; download integrity relies on the pinned release URL")
    endif()
endfunction()

function(_brpc_declare_archive dep url hash source_dir)
    set(_fetch_args
        URL "${DEPS_FETCH_PROXY}${url}"
        SOURCE_DIR "${source_dir}"
        BINARY_DIR "${CMAKE_BINARY_DIR}/_deps/${dep}-populate-build"
        SUBBUILD_DIR "${CMAKE_BINARY_DIR}/_deps/${dep}-populate-subbuild"
    )
    if(NOT "${hash}" STREQUAL "")
        list(APPEND _fetch_args URL_HASH "${hash}")
    endif()

    FetchContent_Declare(${dep} ${_fetch_args})
endfunction()

macro(_brpc_populate_source dep display_name source_dir)
    set(_brpc_source_marker "${ARGV3}")
    if("${_brpc_source_marker}" STREQUAL "")
        set(_brpc_source_marker "CMakeLists.txt")
    endif()

    FetchContent_GetProperties(${dep})
    if(EXISTS "${source_dir}/${_brpc_source_marker}")
        message(STATUS "Using cached ${display_name} sources from ${source_dir}")
        set(${dep}_SOURCE_DIR "${source_dir}")
        set(${dep}_POPULATED TRUE)
    elseif(NOT ${dep}_POPULATED)
        message(STATUS "Downloading ${display_name} sources into ${source_dir}")
        FetchContent_Populate(${dep})
    endif()
endmacro()

function(_brpc_add_ready_target target_name)
    add_custom_target(${target_name} DEPENDS ${ARGN})
endfunction()

function(_brpc_add_cmake_build_target target_name source_dir binary_dir install_dir)
    set(options)
    set(one_value_args SOURCE_SUBDIR LIBDIR)
    set(multi_value_args CMAKE_ARGS DEPENDS BYPRODUCTS POST_INSTALL_COMMANDS)
    cmake_parse_arguments(arg "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

    set(_configure_source_dir "${source_dir}")
    if(arg_SOURCE_SUBDIR)
        set(_configure_source_dir "${source_dir}/${arg_SOURCE_SUBDIR}")
    endif()

    set(_install_libdir "lib")
    if(arg_LIBDIR)
        set(_install_libdir "${arg_LIBDIR}")
    endif()

    add_custom_command(
        OUTPUT ${arg_BYPRODUCTS}
        COMMAND ${CMAKE_COMMAND} -E make_directory "${binary_dir}"
        COMMAND ${CMAKE_COMMAND} -E echo "[brpc-deps] configuring ${target_name}"
        COMMAND ${CMAKE_COMMAND}
            -S "${_configure_source_dir}"
            -B "${binary_dir}"
            -DCMAKE_BUILD_TYPE=RelWithDebInfo
            -DCMAKE_INSTALL_PREFIX=${install_dir}
            -DCMAKE_INSTALL_LIBDIR=${_install_libdir}
            -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=ON
            -DCMAKE_EXPORT_PACKAGE_REGISTRY=OFF
            -DCMAKE_POLICY_DEFAULT_CMP0090=NEW
            ${arg_CMAKE_ARGS}
        COMMAND ${CMAKE_COMMAND} -E echo "[brpc-deps] building ${target_name}"
        COMMAND ${CMAKE_COMMAND} --build "${binary_dir}" --parallel "${_brpc_build_jobs}"
        COMMAND ${CMAKE_COMMAND} -E echo "[brpc-deps] installing ${target_name} into ${install_dir}"
        COMMAND ${CMAKE_COMMAND} --install "${binary_dir}"
        ${arg_POST_INSTALL_COMMANDS}
        DEPENDS ${arg_DEPENDS}
        USES_TERMINAL
        VERBATIM
    )

    add_custom_target(${target_name} DEPENDS ${arg_BYPRODUCTS})
endfunction()

set(brpc_FOUND FALSE)

if(BRPC_ALLOW_SYSTEM_PACKAGE)
    find_package(brpc QUIET)
endif()

if(NOT brpc_FOUND)
    _brpc_find_local_library(BRPC_LIBRARY BRPC_LIBRARY BRPC_ROOT brpc)
    _brpc_find_local_path(BRPC_INCLUDE_DIR BRPC_INCLUDE_DIR BRPC_ROOT brpc/channel.h)

    if(BRPC_LIBRARY AND BRPC_INCLUDE_DIR)
        set(brpc_FOUND TRUE)
        message(STATUS "Using project-managed brpc installation: ${BRPC_LIBRARY}")
    endif()
endif()

if(brpc_FOUND)
    _brpc_find_local_library(GFLAGS_LIBRARY GFLAGS_LIBRARY GFLAGS_ROOT gflags)
    _brpc_find_local_path(GFLAGS_INCLUDE_DIR GFLAGS_INCLUDE_DIR GFLAGS_ROOT gflags/gflags.h)
    _brpc_find_local_library(PROTOBUF_LIBRARY PROTOBUF_LIBRARY PROTOBUF_ROOT protobuf)
    _brpc_find_local_library(PROTOBUF_PROTOC_LIBRARY PROTOBUF_PROTOC_LIBRARY PROTOBUF_ROOT protoc)
    _brpc_find_local_path(PROTOBUF_INCLUDE_DIR PROTOBUF_INCLUDE_DIR PROTOBUF_ROOT google/protobuf/message.h)
    _brpc_find_local_library(LEVELDB_LIBRARY LEVELDB_LIBRARY LEVELDB_ROOT leveldb)
    _brpc_find_local_path(LEVELDB_INCLUDE_DIR LEVELDB_INCLUDE_DIR LEVELDB_ROOT leveldb/db.h)
    _brpc_find_local_protoc(PROTOBUF_PROTOC_EXECUTABLE)

    if(NOT GFLAGS_LIBRARY OR NOT GFLAGS_INCLUDE_DIR OR NOT PROTOBUF_LIBRARY OR
       NOT PROTOBUF_PROTOC_LIBRARY OR NOT PROTOBUF_INCLUDE_DIR OR
       NOT PROTOBUF_PROTOC_EXECUTABLE OR NOT LEVELDB_LIBRARY OR NOT LEVELDB_INCLUDE_DIR)
        message(STATUS
            "Existing brpc installation is incomplete, falling back to project-managed source builds")
        set(brpc_FOUND FALSE)
    endif()
endif()

if(NOT brpc_FOUND)
    message(STATUS "brpc toolchain not found locally, preparing project-managed source downloads")

    _brpc_find_local_library(_resolved_gflags_library GFLAGS_LIBRARY GFLAGS_ROOT gflags)
    _brpc_find_local_path(_resolved_gflags_include GFLAGS_INCLUDE_DIR GFLAGS_ROOT gflags/gflags.h)
    set(_build_gflags FALSE)
    if(_resolved_gflags_library AND _resolved_gflags_include)
        set(GFLAGS_LIBRARY "${_resolved_gflags_library}")
        set(GFLAGS_INCLUDE_DIR "${_resolved_gflags_include}")
    else()
        set(_build_gflags TRUE)
        set(GFLAGS_LIBRARY "${GFLAGS_INSTALL_DIR}/lib/libgflags.a")
        set(GFLAGS_INCLUDE_DIR "${GFLAGS_INSTALL_DIR}/include")
    endif()

    _brpc_find_local_library(_resolved_protobuf_library PROTOBUF_LIBRARY PROTOBUF_ROOT protobuf)
    _brpc_find_local_library(_resolved_protoc_library PROTOBUF_PROTOC_LIBRARY PROTOBUF_ROOT protoc)
    _brpc_find_local_path(_resolved_protobuf_include PROTOBUF_INCLUDE_DIR PROTOBUF_ROOT google/protobuf/message.h)
    _brpc_find_local_protoc(_resolved_protoc_executable)
    set(_build_protobuf FALSE)
    if(_resolved_protobuf_library AND _resolved_protoc_library AND _resolved_protobuf_include AND _resolved_protoc_executable)
        set(PROTOBUF_LIBRARY "${_resolved_protobuf_library}")
        set(PROTOBUF_PROTOC_LIBRARY "${_resolved_protoc_library}")
        set(PROTOBUF_INCLUDE_DIR "${_resolved_protobuf_include}")
        set(PROTOBUF_PROTOC_EXECUTABLE "${_resolved_protoc_executable}")
    else()
        set(_build_protobuf TRUE)
        set(PROTOBUF_LIBRARY "${PROTOBUF_INSTALL_DIR}/lib64/libprotobuf.a")
        set(PROTOBUF_PROTOC_LIBRARY "${PROTOBUF_INSTALL_DIR}/lib64/libprotoc.a")
        set(PROTOBUF_INCLUDE_DIR "${PROTOBUF_INSTALL_DIR}/include")
        set(PROTOBUF_PROTOC_EXECUTABLE "${PROTOBUF_INSTALL_DIR}/bin/protoc-${BRPC_PROTOBUF_COMPILER_VERSION}")
    endif()

    _brpc_find_local_library(_resolved_leveldb_library LEVELDB_LIBRARY LEVELDB_ROOT leveldb)
    _brpc_find_local_path(_resolved_leveldb_include LEVELDB_INCLUDE_DIR LEVELDB_ROOT leveldb/db.h)
    set(_build_leveldb FALSE)
    if(_resolved_leveldb_library AND _resolved_leveldb_include)
        set(LEVELDB_LIBRARY "${_resolved_leveldb_library}")
        set(LEVELDB_INCLUDE_DIR "${_resolved_leveldb_include}")
    else()
        set(_build_leveldb TRUE)
        set(LEVELDB_LIBRARY "${LEVELDB_INSTALL_DIR}/lib/libleveldb.a")
        set(LEVELDB_INCLUDE_DIR "${LEVELDB_INSTALL_DIR}/include")
    endif()

    _brpc_warn_if_missing_hash("gflags" "${BRPC_GFLAGS_SOURCE_URL_HASH}")
    _brpc_warn_if_missing_hash("protobuf" "${BRPC_PROTOBUF_SOURCE_URL_HASH}")
    _brpc_warn_if_missing_hash("leveldb" "${BRPC_LEVELDB_SOURCE_URL_HASH}")
    _brpc_warn_if_missing_hash("brpc" "${BRPC_SOURCE_URL_HASH}")

    _brpc_declare_archive(gflags "${BRPC_GFLAGS_SOURCE_URL}" "${BRPC_GFLAGS_SOURCE_URL_HASH}" "${GFLAGS_SOURCE_DIR}")
    _brpc_declare_archive(protobuf "${BRPC_PROTOBUF_SOURCE_URL}" "${BRPC_PROTOBUF_SOURCE_URL_HASH}" "${PROTOBUF_SOURCE_DIR}")
    _brpc_declare_archive(leveldb "${BRPC_LEVELDB_SOURCE_URL}" "${BRPC_LEVELDB_SOURCE_URL_HASH}" "${LEVELDB_SOURCE_DIR}")
    _brpc_declare_archive(brpc "${BRPC_SOURCE_URL}" "${BRPC_SOURCE_URL_HASH}" "${BRPC_SOURCE_DIR}")

    if(_build_gflags)
        _brpc_populate_source(gflags "gflags" "${GFLAGS_SOURCE_DIR}")
        _brpc_add_cmake_build_target(
            brpc_gflags
            "${gflags_SOURCE_DIR}"
            "${CMAKE_BINARY_DIR}/_deps/gflags-subbuild"
            "${GFLAGS_INSTALL_DIR}"
            LIBDIR lib
            CMAKE_ARGS
                -DBUILD_SHARED_LIBS=OFF
                -DBUILD_TESTING=OFF
                -DREGISTER_BUILD_DIR=OFF
                -DREGISTER_INSTALL_PREFIX=OFF
            BYPRODUCTS
                "${GFLAGS_LIBRARY}"
        )
    else()
        _brpc_add_ready_target(brpc_gflags "${GFLAGS_LIBRARY}")
    endif()

    if(_build_protobuf)
        _brpc_populate_source(protobuf "protobuf" "${PROTOBUF_SOURCE_DIR}" "cmake/CMakeLists.txt")
        _brpc_add_cmake_build_target(
            brpc_protobuf
            "${protobuf_SOURCE_DIR}"
            "${CMAKE_BINARY_DIR}/_deps/protobuf-subbuild"
            "${PROTOBUF_INSTALL_DIR}"
            SOURCE_SUBDIR cmake
            LIBDIR lib64
            CMAKE_ARGS
                -DBUILD_SHARED_LIBS=OFF
                -DBUILD_TESTING=OFF
                -Dprotobuf_BUILD_TESTS=OFF
                -Dprotobuf_BUILD_SHARED_LIBS=OFF
            BYPRODUCTS
                "${PROTOBUF_LIBRARY}"
                "${PROTOBUF_PROTOC_LIBRARY}"
                "${PROTOBUF_PROTOC_EXECUTABLE}"
        )
    else()
        _brpc_add_ready_target(brpc_protobuf
            "${PROTOBUF_LIBRARY}"
            "${PROTOBUF_PROTOC_LIBRARY}"
            "${PROTOBUF_PROTOC_EXECUTABLE}"
        )
    endif()

    if(_build_leveldb)
        _brpc_populate_source(leveldb "leveldb" "${LEVELDB_SOURCE_DIR}")
        _brpc_add_cmake_build_target(
            brpc_leveldb
            "${leveldb_SOURCE_DIR}"
            "${CMAKE_BINARY_DIR}/_deps/leveldb-subbuild"
            "${LEVELDB_INSTALL_DIR}"
            LIBDIR lib
            CMAKE_ARGS
                -DBUILD_SHARED_LIBS=OFF
                -DLEVELDB_BUILD_TESTS=OFF
                -DLEVELDB_BUILD_BENCHMARKS=OFF
                -DHAVE_SNAPPY=0
            BYPRODUCTS
                "${LEVELDB_LIBRARY}"
        )
    else()
        _brpc_add_ready_target(brpc_leveldb "${LEVELDB_LIBRARY}")
    endif()

    _brpc_populate_source(brpc "brpc" "${BRPC_SOURCE_DIR}")
    set(BRPC_LIBRARY "${BRPC_INSTALL_DIR}/lib64/libbrpc.a")
    set(BRPC_INCLUDE_DIR "${BRPC_INSTALL_DIR}/include")
    _brpc_add_cmake_build_target(
        brpc
        "${brpc_SOURCE_DIR}"
        "${CMAKE_BINARY_DIR}/_deps/brpc-subbuild"
        "${BRPC_INSTALL_DIR}"
        LIBDIR lib64
        CMAKE_ARGS
            -DBUILD_SHARED_LIBS=OFF
            "-DCMAKE_PREFIX_PATH=${GFLAGS_INSTALL_DIR};${PROTOBUF_INSTALL_DIR};${LEVELDB_INSTALL_DIR}"
            -Dgflags_DIR=${GFLAGS_INSTALL_DIR}/lib/cmake/gflags
            -DProtobuf_DIR=${PROTOBUF_INSTALL_DIR}/lib64/cmake/protobuf
            -DProtobuf_INCLUDE_DIR=${PROTOBUF_INCLUDE_DIR}
            -DProtobuf_LIBRARY=${PROTOBUF_LIBRARY}
            -DProtobuf_PROTOC_LIBRARY=${PROTOBUF_PROTOC_LIBRARY}
            -DProtobuf_PROTOC_EXECUTABLE=${PROTOBUF_PROTOC_EXECUTABLE}
            -DPROTOC_LIB=${PROTOBUF_PROTOC_LIBRARY}
            -DLEVELDB_INCLUDE_PATH=${LEVELDB_INCLUDE_DIR}
            -DLEVELDB_LIB=${LEVELDB_LIBRARY}
        DEPENDS
            brpc_gflags
            brpc_protobuf
            brpc_leveldb
        BYPRODUCTS
            "${BRPC_LIBRARY}"
    )

    set(brpc_FOUND TRUE)
endif()

if(brpc_FOUND)
    if(NOT TARGET brpc)
        add_custom_target(brpc)
    endif()

    if(NOT BRPC_LIBRARY OR NOT BRPC_INCLUDE_DIR)
        message(FATAL_ERROR
            "brpc was enabled but BRPC_LIBRARY/BRPC_INCLUDE_DIR were not resolved")
    endif()
    if(NOT GFLAGS_LIBRARY OR NOT GFLAGS_INCLUDE_DIR)
        message(FATAL_ERROR
            "brpc requires gflags, but GFLAGS_LIBRARY/GFLAGS_INCLUDE_DIR were not resolved")
    endif()
    if(NOT PROTOBUF_LIBRARY OR NOT PROTOBUF_PROTOC_LIBRARY OR NOT PROTOBUF_INCLUDE_DIR OR
       NOT PROTOBUF_PROTOC_EXECUTABLE)
        message(FATAL_ERROR
            "brpc requires protobuf, but protobuf library/include/protoc paths were not resolved")
    endif()
    if(NOT LEVELDB_LIBRARY OR NOT LEVELDB_INCLUDE_DIR)
        message(FATAL_ERROR
            "brpc requires leveldb, but LEVELDB_LIBRARY/LEVELDB_INCLUDE_DIR were not resolved")
    endif()

    add_library(brpc_with_headers INTERFACE)
    target_include_directories(brpc_with_headers INTERFACE
        ${BRPC_INCLUDE_DIR}
        ${GFLAGS_INCLUDE_DIR}
        ${PROTOBUF_INCLUDE_DIR}
        ${LEVELDB_INCLUDE_DIR}
    )
    target_link_libraries(brpc_with_headers INTERFACE
        ${BRPC_LIBRARY}
        ${GFLAGS_LIBRARY}
        ${PROTOBUF_LIBRARY}
        ${PROTOBUF_PROTOC_LIBRARY}
        ${LEVELDB_LIBRARY}
        -lssl
        -lcrypto
        -ldl
        -lz
    )
else()
    message(WARNING "brpc not available, brpc_client will not be built")
endif()
