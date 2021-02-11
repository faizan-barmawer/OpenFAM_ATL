
 #ifndef ATL_COUNTERS_H_
 #define ATL_COUNTERS_H_
 #include "string.h"
 #include <chrono>
 #include <iomanip>
 #include <unistd.h>

 using namespace std;
 using namespace chrono;
 using Fam_Time = boost::atomic_uint64_t;

 typedef __attribute__((unused)) uint64_t Fam_Profile_Time;

 typedef enum Fam_Counter_Enum {
 #undef ATL_COUNTER
 #define ATL_COUNTER(name) prof_##name,
 #include "atl_counters.tbl"
     fam_counter_max
 } Fam_Counter_Enum_T;

 typedef enum Fam_Counter_Type {
     ATL_CNTR_API,
     ATL_CNTR_ALLOCATOR,
     ATL_CNTR_OPS,
     ATL_CNTR_TYPE_MAX
 } Fam_Counter_Type_T;

 struct Fam_Counter_St {
     Fam_Time count;
     Fam_Time start;
     Fam_Time end;
     Fam_Time total;
 };
 #endif // ATL_COUNTERS_H_

