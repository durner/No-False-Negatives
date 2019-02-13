# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------

file(GLOB_RECURSE SRC_CPP src/*.cpp)
list(REMOVE_ITEM SRC_CPP ${CMAKE_CURRENT_SOURCE_DIR}/src/database.cpp)


# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------

add_library(database STATIC ${SRC_CPP} ${INCLUDE_HPP})
target_link_libraries(database Threads::Threads)

