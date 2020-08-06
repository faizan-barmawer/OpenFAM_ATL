add_definitions(-DFAM_ATOMIC_NATIVE)
set(ATL_SRC
  ${ATL_SRC}
  ${CMAKE_CURRENT_SOURCE_DIR}/fam_atl.cpp
  )
add_library(openfam_atl SHARED ${ATL_SRC})

target_link_libraries(openfam_atl )
set_target_properties(openfam_atl PROPERTIES SOVERSION ${VERSION_STR})

