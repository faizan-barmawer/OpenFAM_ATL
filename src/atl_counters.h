/*
 * atl_counters.h
 * Copyright (c) 2021 Hewlett Packard Enterprise Development, LP. All rights
 * reserved. Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 *    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *    INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * See https://spdx.org/licenses/BSD-3-Clause
 *
 */

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

