/*
 * fam_atl.cpp
 * Copyright (c) 2020 Hewlett Packard Enterprise Development, LP. All rights
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

#include <iostream>
#include <fam/fam.h>
#include <fam/fam_exception.h>
#include <common/fam_options.h>
#include <common/fam_internal.h>
#include <map>
#include <string>
#include <cis/fam_cis_client.h>
#include <cis/fam_cis_direct.h>
#include <rdma/fabric.h>
#include <rdma/fi_atomic.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_rma.h>
#include <common/fam_context.h>
#include <common/fam_libfabric.h>
#include "fam_atl.h"
#ifdef ATL_PROFILE
#include "atl_counters.h"
#endif


using namespace std;
namespace openfam {

#define MIN_OBJ_SIZE 128
#define MAX_DATA_IN_MSG 128

#define THROW_ATL_ERR_MSG(exception, message_str)                              \
    do {                                                                       \
        std::ostringstream errMsgStr;                                          \
        errMsgStr << __func__ << ":" << __LINE__ << ":" << message_str;        \
        throw exception(errMsgStr.str().c_str());                              \
    } while (0)

#define TRY_CATCH_BEGIN try {
#define RETURN_WITH_FAM_EXCEPTION                                              \
    }                                                                          \
    catch (Fam_Exception & e) {                                                \
        throw e;                                                               \
    }

class ATLib::ATLimpl_ {

private:
    Fam_Options famOptions;
    Fam_CIS *famCIS;
    Fam_Thread_Model famThreadModel;
    Fam_Context_Model famContextModel;
    struct fi_info *fi;
    struct fid_fabric *fabric;
    struct fid_eq *eq;
    struct fid_domain *domain;
    struct fid_av *av;
    size_t serverAddrNameLen;
    void *serverAddrName;
    size_t selfAddrNameLen=0;
    void *selfAddrName;
    std::vector<fi_addr_t> *fiAddrs;
    Fam_Context *defaultCtx;
    std::map<uint64_t, uint32_t> *selfAddrLens;
    std::map<uint64_t, char *> *selfAddrs;
    std::map<uint64_t, Fam_Context *> *defContexts;
    uint32_t uid,gid;


 #ifdef ATL_PROFILE
     Fam_Counter_St profileData[fam_counter_max][ATL_CNTR_TYPE_MAX];
     uint64_t profile_time;
     uint64_t profile_start;
 #define OUTPUT_WIDTH 120
 #define ITEM_WIDTH OUTPUT_WIDTH / 5
     uint64_t fam_get_time() {
 #if 1
         long int time = static_cast<long int>(
             duration_cast<nanoseconds>(
                 high_resolution_clock::now().time_since_epoch())
                 .count());
         return time;
 #else // using intel tsc
         uint64_t hi, lo, aux;
         __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi), "=c"(aux));
         return (uint64_t)lo | ((uint64_t)hi << 32);
 #endif
     }

     uint64_t fam_time_diff_nanoseconds(Fam_Profile_Time start,
                                        Fam_Profile_Time end) {
         return (end - start);
     }
 #define ATL_PROFILE_START_TIME() profile_start = fam_get_time();
 #define ATL_PROFILE_INIT() fam_profile_init();
 #define ATL_PROFILE_END()                                                      \
     {                                                                          \
         profile_time = fam_get_time() - profile_start;                         \
         fam_dump_profile_banner();                                             \
         fam_dump_profile_data();                                               \
         fam_dump_profile_summary();                                            \
     }
 #define ATL_CNTR_INC_API(apiIdx) __ATL_CNTR_INC_API(prof_##apiIdx)
 #define ATL_PROFILE_START_ALLOCATOR(apiIdx) __ATL_PROFILE_START_ALLOCATOR()
 #define ATL_PROFILE_END_ALLOCATOR(apiIdx)                                      \
     __ATL_PROFILE_END_ALLOCATOR(prof_##apiIdx)
 #define ATL_PROFILE_START_OPS(apiIdx) __ATL_PROFILE_START_OPS()
 #define ATL_PROFILE_END_OPS(apiIdx) __ATL_PROFILE_END_OPS(prof_##apiIdx)


 #define __ATL_CNTR_INC_API(apiIdx)                                             \
     uint64_t one = 1;                                                          \
     profileData[apiIdx][ATL_CNTR_API].count.fetch_add(                         \
         one, boost::memory_order_seq_cst);
 #define __ATL_PROFILE_START_ALLOCATOR(apiIdx)                                  \
     Fam_Profile_Time startAlloc = fam_get_time();

 #define __ATL_PROFILE_START_OPS(apiIdx)                                        \
     Fam_Profile_Time startOps = fam_get_time();

 #define __ATL_PROFILE_END_ALLOCATOR(apiIdx)                                    \
     Fam_Profile_Time endAlloc = fam_get_time();                                \
     Fam_Profile_Time totalAlloc =                                              \
         fam_time_diff_nanoseconds(startAlloc, endAlloc);                       \
     fam_add_to_total_profile(ATL_CNTR_ALLOCATOR, apiIdx, totalAlloc);

 #define __ATL_PROFILE_END_OPS(apiIdx)                                          \
     Fam_Profile_Time endOps = fam_get_time();                                  \
     Fam_Profile_Time totalOps = fam_time_diff_nanoseconds(startOps, endOps);   \
     fam_add_to_total_profile(ATL_CNTR_OPS, apiIdx, totalOps);

     void fam_profile_init() { memset(profileData, 0, sizeof(profileData)); }

     void fam_add_to_total_profile(Fam_Counter_Type_T type, int apiIdx,
                                   Fam_Profile_Time total) {
         profileData[apiIdx][type].total.fetch_add(total,
                                                   boost::memory_order_seq_cst);
     }

     void fam_total_api_time(int apiIdx) {
         uint64_t total = profileData[apiIdx][ATL_CNTR_ALLOCATOR].total.load(
                              boost::memory_order_seq_cst) +
                          profileData[apiIdx][ATL_CNTR_OPS].total.load(
                              boost::memory_order_seq_cst);
         profileData[apiIdx][ATL_CNTR_API].total.fetch_add(
             total, boost::memory_order_seq_cst);
     }

     void fam_dump_profile_banner(void) {
         {
             string header = "ATL PROFILE DATA";
             cout << endl;
             cout << setfill('-') << setw(OUTPUT_WIDTH) << "-" << endl;
             cout << setfill(' ')
                  << setw((int)(OUTPUT_WIDTH - header.length()) / 2) << " ";
             cout << "ATL PROFILE DATA";
             cout << setfill(' ')
                  << setw((int)(OUTPUT_WIDTH - header.length()) / 2) << " "
                  << endl;
             cout << setfill('-') << setw(OUTPUT_WIDTH) << "-" << endl;
         }
 #define DUMP_HEADING1(name)                                                    \
     cout << std::left << setfill(' ') << setw(ITEM_WIDTH) << name;
         DUMP_HEADING1("Function");
         DUMP_HEADING1("Count");
         DUMP_HEADING1("Total Pct");
         DUMP_HEADING1("Total time(ns)");
         DUMP_HEADING1("Avg time/call(ns)");
         cout << endl;
 #define DUMP_HEADING2(name)                                                    \
     cout << std::left << setfill(' ') << setw(ITEM_WIDTH)                      \
          << string(strlen(name), '-');
         DUMP_HEADING2("Function");
         DUMP_HEADING2("Count");
         DUMP_HEADING2("Total Pct");
         DUMP_HEADING2("Total time(ns)");
         DUMP_HEADING2("Avg time/call(ns)");
         cout << endl;
     }

     void fam_dump_profile_data(void) {
 #define DUMP_DATA_COUNT(type, idx)                                             \
     cout << std::left << setbase(10) << setfill(' ') << setw(ITEM_WIDTH)       \
          << profileData[idx][type].count;

 #define DUMP_DATA_TIME(type, idx)                                              \
     cout << std::left << setbase(10) << setfill(' ') << setw(ITEM_WIDTH)       \
          << profileData[idx][type].total;

 #define DUMP_DATA_PCT(type, idx)                                               \
     {                                                                          \
         double time_pct = (double)((double)profileData[idx][type].total *      \
                                    100 / (double)profile_time);                \
         cout << std::left << setfill(' ') << setw(ITEM_WIDTH) << std::fixed    \
              << setprecision(2) << time_pct;                                   \
     }


 #define DUMP_DATA_AVG(type, idx)                                               \
     {                                                                          \
         uint64_t avg_time =                                                    \
             (profileData[idx][type].total / profileData[idx][type].count);     \
         cout << std::left << setfill(' ') << setw(ITEM_WIDTH) << avg_time;     \
     }

 #undef ATL_COUNTER
 #define ATL_TOTAL_API_TIME(apiIdx) fam_total_api_time(apiIdx);
 #define ATL_COUNTER(apiIdx) ATL_TOTAL_API_TIME(prof_##apiIdx)
 #include "atl_counters.tbl"
 #undef ATL_COUNTER
 #define ATL_COUNTER(name) __ATL_COUNTER(name, prof_##name)
 #define __ATL_COUNTER(name, apiIdx)                                            \
     if (profileData[apiIdx][ATL_CNTR_API].count) {                             \
         cout << std::left << setfill(' ') << setw(ITEM_WIDTH) << #name;        \
         DUMP_DATA_COUNT(ATL_CNTR_API, apiIdx);                                 \
         DUMP_DATA_PCT(ATL_CNTR_API, apiIdx);                                   \
         DUMP_DATA_TIME(ATL_CNTR_API, apiIdx);                                  \
         DUMP_DATA_AVG(ATL_CNTR_API, apiIdx);                                   \
         cout << endl;                                                          \
     }
 #include "atl_counters.tbl"
         cout << endl;
     }
     void fam_dump_profile_summary(void) {
         uint64_t fam_lib_time = 0;
         uint64_t fam_alloc_time = 0;
         uint64_t fam_ops_time = 0;
         cout << std::left << setfill(' ') << setw(ITEM_WIDTH) << "Summary"
              << endl;
         ;
         cout << std::left << setfill(' ') << setw(ITEM_WIDTH)
              << string(strlen("Summary"), '-') << endl;

 #define ATL_SUMMARY_ENTRY(name, value)                                         \
     cout << std::left << std::fixed << setprecision(2) << setfill(' ')         \
          << setw(ITEM_WIDTH) << name << setw(10) << ":" << value << " ns ("    \
          << value * 100 / profile_time << "%)" << endl;
 #undef ATL_COUNTER
 #undef __ATL_COUNTER
 #define ATL_COUNTER(name) __ATL_COUNTER(prof_##name)
 #define __ATL_COUNTER(apiIdx)                                                  \
     {                                                                          \
         fam_lib_time += profileData[apiIdx][ATL_CNTR_API].total;               \
         fam_alloc_time += profileData[apiIdx][ATL_CNTR_ALLOCATOR].total;       \
         fam_ops_time += profileData[apiIdx][ATL_CNTR_OPS].total;               \
     }
 #include "atl_counters.tbl"

         ATL_SUMMARY_ENTRY("Total time", profile_time);
         ATL_SUMMARY_ENTRY("OpenATL library", fam_lib_time);
         ATL_SUMMARY_ENTRY("Allocator", fam_alloc_time);
         ATL_SUMMARY_ENTRY("DataPath", fam_ops_time);
         cout << endl;
     }

 #else
 #define ATL_PROFILE_START_TIME()
 #define ATL_PROFILE_INIT()
 #define ATL_PROFILE_END()
 #define ATL_CNTR_INC_API(apiIdx)
 #define ATL_PROFILE_START_ALLOCATOR(apiIdx)
 #define ATL_PROFILE_END_ALLOCATOR(apiIdx)
 #define ATL_PROFILE_START_OPS(apiIdx)
 #define ATL_PROFILE_END_OPS(apiIdx)
 #endif

public:

ATLimpl_() {
    defContexts = new std::map<uint64_t, Fam_Context *>();
    selfAddrLens = new std::map<uint64_t,uint32_t>();
    selfAddrs = new std::map<uint64_t, char *>();
    memset((void *)&famOptions, 0, sizeof(Fam_Options));
};

~ATLimpl_() {
};

int populate_fam_options(fam *inp_fam) {
    famOptions.defaultRegionName = (char *)inp_fam->fam_get_option(strdup("DEFAULT_REGION_NAME"));
    famOptions.grpcPort = (char *)inp_fam->fam_get_option(strdup("GRPC_PORT"));
    famOptions.cisServer = (char *)inp_fam->fam_get_option(strdup("CIS_SERVER"));
    famOptions.libfabricProvider = (char *)inp_fam->fam_get_option(strdup("LIBFABRIC_PROVIDER"));
    famOptions.famThreadModel = (char *)inp_fam->fam_get_option(strdup("FAM_THREAD_MODEL"));
    if (strcmp(famOptions.famThreadModel, FAM_THREAD_SERIALIZE_STR) == 0)
        famThreadModel = FAM_THREAD_SERIALIZE;
    else if (strcmp(famOptions.famThreadModel, FAM_THREAD_MULTIPLE_STR) == 0)
        famThreadModel = FAM_THREAD_MULTIPLE;
    famOptions.cisInterfaceType = (char *)inp_fam->fam_get_option(strdup("CIS_INTERFACE_TYPE"));
    famOptions.openFamModel = (char *)inp_fam->fam_get_option(strdup("OPENFAM_MODEL"));
    famOptions.famContextModel =  (char *)inp_fam->fam_get_option(strdup("FAM_CONTEXT_MODEL"));
    if (strcmp(famOptions.famContextModel, FAM_CONTEXT_DEFAULT_STR) == 0)
        famContextModel = FAM_CONTEXT_DEFAULT;
    else if (strcmp(famOptions.famContextModel, FAM_CONTEXT_REGION_STR) == 0)
        famContextModel = FAM_CONTEXT_REGION;
    famOptions.runtime = (char *)inp_fam->fam_get_option(strdup("RUNTIME"));
    famOptions.numConsumer = (char *)inp_fam->fam_get_option(strdup("NUM_CONSUMER"));
    return 0;
}
int atl_initialize(fam *inp_fam) {
    std::ostringstream message;
    int ret = 0;
    char *memServerName = NULL;
    char *service = NULL;
    size_t memServerInfoSize;
    ATL_PROFILE_INIT();

    if ((ret = populate_fam_options(inp_fam)) < 0) {
        return ret;
    }

    fiAddrs = new std::vector<fi_addr_t>();
    if (strcmp(famOptions.cisInterfaceType, FAM_OPTIONS_RPC_STR) == 0) {
        try {
            famCIS = new Fam_CIS_Client(famOptions.cisServer,
                                        atoi(famOptions.grpcPort));
    	} catch (Fam_Exception &e) {
        THROW_ATL_ERR_MSG(ATL_Exception, e.fam_error_msg());
    	}
    }
    else {
	char *name = strdup("127.0.0.1");
	famCIS = new Fam_CIS_Direct(name);
    }
    uid = (uint32_t)getuid();
    gid = (uint32_t)getgid();

    if ((ret = fabric_initialize(memServerName, service, 0, famOptions.libfabricProvider,
                                 &fi, &fabric, &eq, &domain, famThreadModel)) < 0) {
        return ret;
    }

    // Initialize address vector
    if (fi->ep_attr->type == FI_EP_RDM) {
        if ((ret = fabric_initialize_av(fi, domain, eq, &av)) < 0) {
            return ret;
        }
    }

    try {
        memServerInfoSize = famCIS->get_memserverinfo_size();
    } catch(Fam_Exception &e) {
        message << "Fam allocator get_memserverinfo_size failed";
        THROW_ERRNO_MSG(Fam_Allocator_Exception, FAM_ERR_ALLOCATOR,
                        message.str().c_str());
    }

    if (memServerInfoSize) {
        void *memServerInfoBuffer = calloc(1, memServerInfoSize);
	try {
            famCIS->get_memserverinfo(memServerInfoBuffer);
        } catch(Fam_Exception &e) {
            message << "Fam Allocator get_memserverinfo failed";
            THROW_ERRNO_MSG(Fam_Allocator_Exception, FAM_ERR_ALLOCATOR,
                            message.str().c_str());
        }

        size_t bufPtr = 0;
        uint64_t nodeId;
        size_t addrSize;
        void *nodeAddr;
        uint64_t fiAddrsSize = fiAddrs->size();

        while (bufPtr < memServerInfoSize) {
            memcpy(&nodeId, ((char *)memServerInfoBuffer + bufPtr),
                   sizeof(uint64_t));
            bufPtr += sizeof(uint64_t);
            memcpy(&addrSize, ((char *)memServerInfoBuffer + bufPtr),
                   sizeof(size_t));
            bufPtr += sizeof(size_t);
            nodeAddr = calloc(1, addrSize);
            memcpy(nodeAddr, ((char *)memServerInfoBuffer + bufPtr),
                   addrSize);
            bufPtr += addrSize;

            // Initialize defaultCtx
            if (famContextModel == FAM_CONTEXT_DEFAULT) {
                defaultCtx =
                    new Fam_Context(fi, domain, famThreadModel);
                defContexts->insert({nodeId, defaultCtx});
                ret =
                    fabric_enable_bind_ep(fi, av, eq, defaultCtx->get_ep());
                if (ret < 0) {
                    // TODO: Log error
                    return ret;
                }
            }
            std::vector<fi_addr_t> tmpAddrV;
            ret = fabric_insert_av((char *)nodeAddr, av, &tmpAddrV);

            if (ret < 0) {
                // TODO: Log error
                return ret;
            }

            // Place the fi_addr_t at nodeId index of fiAddrs vector.
            if (nodeId >= fiAddrsSize) {
                // Increase the size of fiAddrs vector to accomodate
                // nodeId larger than the current size.
                fiAddrs->resize(nodeId + 512, FI_ADDR_UNSPEC);
                fiAddrsSize = fiAddrs->size();
            }
            fiAddrs->at(nodeId) = tmpAddrV[0];

            selfAddrNameLen = 0;
            ret = fabric_getname_len(defaultCtx->get_ep(), &selfAddrNameLen);
            if (selfAddrNameLen <= 0) {
            	message << "Fam libfabric fabric_getname_len failed: "
                    << fabric_strerror(ret);
                THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());

            }
            selfAddrName = calloc(1, selfAddrNameLen);
            ret = fabric_getname(defaultCtx->get_ep(), selfAddrName, (size_t *)&selfAddrNameLen);
            if (ret < 0) {
            	message << "Fam libfabric fabric_getname failed: "
            		<< fabric_strerror(ret);
            	THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
            }
            selfAddrLens->insert({nodeId, (uint32_t)selfAddrNameLen});
            selfAddrs->insert({nodeId, (char *)selfAddrName});
        }
    }

    ATL_PROFILE_START_TIME();

    return 0;
}

int atl_finalize() {
    ATL_PROFILE_END();

    if (fi) {
        fi_freeinfo(fi);
        fi = NULL;
    }

    if (fabric) {
        fi_close(&fabric->fid);
        fabric = NULL;
    }

    if (eq) {
        fi_close(&eq->fid);
        eq = NULL;
    }

    if (domain) {
        fi_close(&domain->fid);
        domain = NULL;
    }

    if (av) {
        fi_close(&av->fid);
        av = NULL;
    }

    if (selfAddrLens != NULL) {
        selfAddrLens->clear();
    }
    delete selfAddrLens;

    if (selfAddrs != NULL) {
        for (auto fam_ctx : *selfAddrs) {
            delete fam_ctx.second;
        }
        selfAddrs->clear();
    }
    delete selfAddrs;

    delete fiAddrs;

    if (serverAddrName) free(serverAddrName);

    if (defaultCtx != NULL)
        delete defaultCtx;
    return 0;
}

int validate_item(Fam_Descriptor *descriptor) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    if (key == FAM_KEY_UNINITIALIZED) {
    	Fam_Global_Descriptor globalDescriptor =
            descriptor->get_global_descriptor();
        uint64_t regionId = globalDescriptor.regionId & REGIONID_MASK ;
    	uint64_t offset = globalDescriptor.offset;
    	uint64_t memoryServerId = descriptor->get_memserver_id();
        Fam_Region_Item_Info info = famCIS->check_permission_get_item_info(
            regionId, offset, memoryServerId, uid, gid);
        descriptor->set_desc_status(DESC_INIT_DONE);
        descriptor->bind_key(info.key);
        descriptor->set_name(info.name);
        descriptor->set_perm(info.perm);
        descriptor->set_size(info.size);
        descriptor->set_base_address(info.base);
    }
    return 0;
}

Fam_Context *get_defaultCtx(uint64_t nodeId) {
    std::ostringstream message;
    auto obj = defContexts->find(nodeId);
    if (obj == defContexts->end()) {
	message << "Context for memory server id:" << nodeId << " not found";
	THROW_ATL_ERR_MSG(ATL_Exception,message.str().c_str());
    }
    return obj->second;
}


uint32_t get_selfAddrLen(uint64_t nodeId) {
    std::ostringstream message;
    auto obj = selfAddrLens->find(nodeId);
    if (obj == selfAddrLens->end()) {
	message << "AddrLen for self not found";
	THROW_ATL_ERR_MSG(ATL_Exception,message.str().c_str());
    }
    return obj->second;
}

char * get_selfAddr(uint64_t nodeId) {
    std::ostringstream message;
    auto obj = selfAddrs->find(nodeId);
    if (obj == selfAddrs->end()) {
        message << "Addr for self not found";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    return obj->second;
}

int fam_get_atomic(void *local, Fam_Descriptor *descriptor,
                        uint64_t offset,size_t nbytes) {

    int ret = 0;
    std::ostringstream message;
    fid_mr *mr = 0;
    uint64_t key = 0;
    int32_t retStatus = -1;
    Fam_Global_Descriptor globalDescriptor;

    ATL_CNTR_INC_API(fam_get_atomic);
    ATL_PROFILE_START_ALLOCATOR(fam_get_atomic);
    if ((local == NULL) || (descriptor == NULL) || (nbytes == 0)) {
	message << "Invalid Options";
	THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    ret = validate_item(descriptor);
    if ((offset + nbytes) > descriptor->get_size()) {
        message << "Invalid Options";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }

    ATL_PROFILE_END_ALLOCATOR(fam_get_atomic);
    ATL_PROFILE_START_OPS(fam_get_atomic);
    if (ret == 0) {
        // Read data from FAM region with this key
        globalDescriptor = descriptor->get_global_descriptor();
        uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;

        key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
        key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
        key |= 1;
        ret = fabric_register_mr(local, nbytes, &key,
                                     domain, 1, mr);
        if (ret < 0) {
            cout << "error: memory register failed" << endl;
            return -4;
        }

        uint64_t nodeId = descriptor->get_memserver_id();
        Fam_Context *ATLCtx = get_defaultCtx(nodeId);
        fi_context *ctx = fabric_post_response_buff(&retStatus,(*fiAddrs)[nodeId], ATLCtx,sizeof(retStatus));
        ret = famCIS->get_atomic(globalDescriptor.regionId & REGIONID_MASK,
				 globalDescriptor.offset, offset, nbytes,
				 key, get_selfAddr(nodeId), get_selfAddrLen(nodeId),
				 nodeId, uid, gid);
			
        if (ret == 0) {
            ret = fabric_completion_wait(ATLCtx, ctx, 1);
            fabric_deregister_mr(mr);
            if (ctx)
              delete ctx;
            ret = retStatus;
        }
    } //validate_item()
    ATL_PROFILE_END_OPS(fam_get_atomic);
    return ret;
}

int fam_put_atomic(void *local, Fam_Descriptor *descriptor,
                        uint64_t offset,size_t nbytes) {

    int ret = 0;
    std::ostringstream message;
    fid_mr *mr = 0;
    uint64_t key = 0;
    int32_t retStatus = -1;
    fi_context *ctx = NULL;
    Fam_Global_Descriptor globalDescriptor;
    ATL_CNTR_INC_API(fam_put_atomic);
    ATL_PROFILE_START_ALLOCATOR(fam_put_atomic);
    if ((local == NULL) || (descriptor == NULL) || (nbytes == 0)) {
        message << "Invalid Options";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    ret = validate_item(descriptor);
    if ((offset + nbytes) > descriptor->get_size()) {
        message << "Invalid Options";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }

    ATL_PROFILE_END_ALLOCATOR(fam_put_atomic);
    ATL_PROFILE_START_OPS(fam_put_atomic);
    if (ret == 0) {
        // Read data from FAM region with this key
        globalDescriptor = descriptor->get_global_descriptor();

        if (nbytes > MAX_DATA_IN_MSG) {
            uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;
            key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
            key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
            key |= 1;
            ret = fabric_register_mr(local, nbytes, &key,
                                     domain, 1, mr);
            if (ret < 0) {
                cout << "error: memory register failed" << endl;
                return -4;
            }
        }
        uint64_t nodeId = descriptor->get_memserver_id();
        Fam_Context *ATLCtx = get_defaultCtx(nodeId);
	if (nbytes > MAX_DATA_IN_MSG) 
            ctx = fabric_post_response_buff(&retStatus,(*fiAddrs)[nodeId], ATLCtx,sizeof(retStatus));

        ret = famCIS->put_atomic(globalDescriptor.regionId & REGIONID_MASK,
                                 globalDescriptor.offset, offset, nbytes,
                                 key, get_selfAddr(nodeId),get_selfAddrLen(nodeId),
                                 (const char *)local, nodeId, uid, gid);

	if ((ret == 0) && (nbytes > MAX_DATA_IN_MSG)) {
       	    ret = fabric_completion_wait(ATLCtx, ctx, 1);
	    ret = retStatus;
	}
	if (ctx) delete ctx;
	if (nbytes > MAX_DATA_IN_MSG)
	    fabric_deregister_mr(mr);

    } //validate_item
    ATL_PROFILE_END_OPS(fam_put_atomic);
    return ret;
}

int fam_scatter_atomic(void *local, Fam_Descriptor *descriptor,
                       uint64_t nElements, uint64_t firstElement,
                       uint64_t stride, uint64_t elementSize) {

  int ret = 0;
  std::ostringstream message;
  fid_mr *mr = 0;
  uint64_t key = 0;
  int32_t retStatus = -1;
  fi_context *ctx = NULL;
  Fam_Global_Descriptor globalDescriptor;
  //    FAM_CNTR_INC_API(fam_put_atomic);
  //    FAM_PROFILE_START_ALLOCATOR(fam_put_atomic);
  if ((local == NULL) || (descriptor == NULL) || (nElements == 0)) {
    message << "Invalid Options";
    THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
  }

  ret = validate_item(descriptor);
  //    FAM_PROFILE_END_ALLOCATOR(fam_put_atomic);
  //    FAM_PROFILE_START_OPS(fam_put_atomic);
  if (ret == 0) {
    // Read data from FAM region with this key
    globalDescriptor = descriptor->get_global_descriptor();

    uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;
    key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
    key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
    key |= 1;
    ret =
        fabric_register_mr(local, descriptor->get_size(), &key, domain, 1, mr);
    if (ret < 0) {
      message << "error: memory register failed";
      THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    uint64_t nodeId = descriptor->get_memserver_id();
    Fam_Context *ATLCtx = get_defaultCtx(nodeId);
    ctx = fabric_post_response_buff(&retStatus, (*fiAddrs)[nodeId], ATLCtx,
                                    sizeof(retStatus));

    ret = famCIS->scatter_strided_atomic(
        globalDescriptor.regionId & REGIONID_MASK, globalDescriptor.offset,
        nElements, firstElement, stride, elementSize, key, get_selfAddr(nodeId),
        get_selfAddrLen(nodeId), nodeId, uid, gid);

    if (ret == 0) {
      fabric_completion_wait(ATLCtx, ctx, 1);
      ret = retStatus;
      fabric_deregister_mr(mr);
    }
    //    FAM_PROFILE_END_OPS(fam_put_atomic);
  } // validate_item
  return ret;
}

int fam_gather_atomic(void *local, Fam_Descriptor *descriptor,
                      uint64_t nElements, uint64_t firstElement,
                      uint64_t stride, uint64_t elementSize) {

  int ret = 0;
  std::ostringstream message;
  fid_mr *mr = 0;
  uint64_t key = 0;
  int32_t retStatus = -1;
  fi_context *ctx = NULL;
  Fam_Global_Descriptor globalDescriptor;
  //    FAM_CNTR_INC_API(fam_put_atomic);
  //    FAM_PROFILE_START_ALLOCATOR(fam_put_atomic);
  if ((local == NULL) || (descriptor == NULL) || (nElements == 0)) {
    message << "Invalid Options";
    THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
  }

  ret = validate_item(descriptor);
  //    FAM_PROFILE_END_ALLOCATOR(fam_put_atomic);
  //    FAM_PROFILE_START_OPS(fam_put_atomic);
  if (ret == 0) {
    // Read data from FAM region with this key
    globalDescriptor = descriptor->get_global_descriptor();

    uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;
    key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
    key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
    key |= 1;
    ret =
        fabric_register_mr(local, descriptor->get_size(), &key, domain, 1, mr);
    if (ret < 0) {
      message << "error: memory register failed";
      THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    uint64_t nodeId = descriptor->get_memserver_id();
    Fam_Context *ATLCtx = get_defaultCtx(nodeId);
    ctx = fabric_post_response_buff(&retStatus, (*fiAddrs)[nodeId], ATLCtx,
                                    sizeof(retStatus));

    ret = famCIS->gather_strided_atomic(
        globalDescriptor.regionId & REGIONID_MASK, globalDescriptor.offset,
        nElements, firstElement, stride, elementSize, key, get_selfAddr(nodeId),
        get_selfAddrLen(nodeId), nodeId, uid, gid);

    if (ret == 0) {
      fabric_completion_wait(ATLCtx, ctx, 1);
      ret = retStatus;
      fabric_deregister_mr(mr);
    }
    //    FAM_PROFILE_END_OPS(fam_put_atomic);
  } // validate_item
  return ret;
}

int fam_scatter_atomic(void *local, Fam_Descriptor *descriptor,
                       uint64_t nElements, uint64_t *elementIndex,
                       uint64_t elementSize) {

  int ret = 0;
  std::ostringstream message;
  fid_mr *mr = 0;
  uint64_t key = 0;
  int32_t retStatus = -1;
  fi_context *ctx = NULL;
  Fam_Global_Descriptor globalDescriptor;
  //    FAM_CNTR_INC_API(fam_put_atomic);
  //    FAM_PROFILE_START_ALLOCATOR(fam_put_atomic);
  if ((local == NULL) || (descriptor == NULL) || (nElements == 0)) {
    message << "Invalid Options";
    THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
  }

  ret = validate_item(descriptor);
  //    FAM_PROFILE_END_ALLOCATOR(fam_put_atomic);
  //    FAM_PROFILE_START_OPS(fam_put_atomic);
  if (ret == 0) {
    // Read data from FAM region with this key
    globalDescriptor = descriptor->get_global_descriptor();

    uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;
    key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
    key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
    key |= 1;
    ret =
        fabric_register_mr(local, descriptor->get_size(), &key, domain, 1, mr);
    if (ret < 0) {
      message << "error: memory register failed";
      THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    // convert the elementIndex array into a comma separated string
    std::vector<uint64_t> index;
    ostringstream indexStr;
    for (uint64_t i = 0; i < nElements; i++)
      index.push_back(elementIndex[i]);
    copy(index.begin(), index.end() - 1,
         ostream_iterator<uint64_t>(indexStr, ","));
    indexStr << index.back();
    index.clear();

    uint64_t nodeId = descriptor->get_memserver_id();
    Fam_Context *ATLCtx = get_defaultCtx(nodeId);
    ctx = fabric_post_response_buff(&retStatus, (*fiAddrs)[nodeId], ATLCtx,
                                    sizeof(retStatus));

    ret = famCIS->scatter_indexed_atomic(
        globalDescriptor.regionId & REGIONID_MASK, globalDescriptor.offset,
        nElements, string(indexStr.str()).c_str(), elementSize, key,
        get_selfAddr(nodeId), get_selfAddrLen(nodeId), nodeId, uid, gid);

    if (ret == 0) {
      fabric_completion_wait(ATLCtx, ctx, 1);
      ret = retStatus;
      fabric_deregister_mr(mr);
    }
    //    FAM_PROFILE_END_OPS(fam_put_atomic);
  } // validate_item
  return ret;
}
int fam_gather_atomic(void *local, Fam_Descriptor *descriptor,
                      uint64_t nElements, uint64_t *elementIndex,
                      uint64_t elementSize) {

  int ret = 0;
  std::ostringstream message;
  fid_mr *mr = 0;
  uint64_t key = 0;
  int32_t retStatus = -1;
  fi_context *ctx = NULL;
  Fam_Global_Descriptor globalDescriptor;
  //    FAM_CNTR_INC_API(fam_put_atomic);
  //    FAM_PROFILE_START_ALLOCATOR(fam_put_atomic);
  if ((local == NULL) || (descriptor == NULL) || (nElements == 0)) {
    message << "Invalid Options";
    THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
  }

  ret = validate_item(descriptor);
  //    FAM_PROFILE_END_ALLOCATOR(fam_put_atomic);
  //    FAM_PROFILE_START_OPS(fam_put_atomic);
  if (ret == 0) {
    // Read data from FAM region with this key
    globalDescriptor = descriptor->get_global_descriptor();

    uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;
    key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
    key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
    key |= 1;
    ret =
        fabric_register_mr(local, descriptor->get_size(), &key, domain, 1, mr);
    if (ret < 0) {
      message << "error: memory register failed";
      THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    // convert the elementIndex array into a comma separated string
    std::vector<uint64_t> index;
    ostringstream indexStr;
    for (uint64_t i = 0; i < nElements; i++)
      index.push_back(elementIndex[i]);
    copy(index.begin(), index.end() - 1,
         ostream_iterator<uint64_t>(indexStr, ","));
    indexStr << index.back();
    index.clear();

    uint64_t nodeId = descriptor->get_memserver_id();
    Fam_Context *ATLCtx = get_defaultCtx(nodeId);
    ctx = fabric_post_response_buff(&retStatus, (*fiAddrs)[nodeId], ATLCtx,
                                    sizeof(retStatus));

    ret = famCIS->gather_indexed_atomic(
        globalDescriptor.regionId & REGIONID_MASK, globalDescriptor.offset,
        nElements, string(indexStr.str()).c_str(), elementSize, key,
        get_selfAddr(nodeId), get_selfAddrLen(nodeId), nodeId, uid, gid);

    if (ret == 0) {
      fabric_completion_wait(ATLCtx, ctx, 1);
      ret = retStatus;
      fabric_deregister_mr(mr);
    }
    //    FAM_PROFILE_END_OPS(fam_put_atomic);
  } // validate_item
  return ret;
}

}; //class

int ATLib::initialize(fam *inp_fam) {
    return pATLimpl_->atl_initialize(inp_fam);
}

void ATLib::fam_get_atomic(void *local, Fam_Descriptor *descriptor,
                           uint64_t offset, uint64_t nbytes) {
  int ret;
  std::ostringstream message;
  TRY_CATCH_BEGIN  
  ret = pATLimpl_->fam_get_atomic(local, descriptor, offset, nbytes);
  RETURN_WITH_FAM_EXCEPTION
  if (ret) {
    message << "Error in fam_get_atomic - Error code:" << ret;
    throw Fam_Exception(FAM_ERR_ATL, message.str().c_str());
  }
}
void ATLib::fam_put_atomic(void *local, Fam_Descriptor *descriptor,
                           uint64_t offset, uint64_t nbytes) {
  int ret;
  std::ostringstream message;
  TRY_CATCH_BEGIN
  ret = pATLimpl_->fam_put_atomic(local, descriptor, offset, nbytes);
  RETURN_WITH_FAM_EXCEPTION
  if (ret) {
    message << "Error in fam_put_atomic - Error code:" << ret;
    throw Fam_Exception(FAM_ERR_ATL, message.str().c_str());
  }
}
void ATLib::fam_scatter_atomic(void *local, Fam_Descriptor *descriptor,
                               uint64_t nElements, uint64_t firstElement,
                               uint64_t stride, uint64_t elementSize) {
  int ret;
  std::ostringstream message;
  TRY_CATCH_BEGIN
  ret = pATLimpl_->fam_scatter_atomic(local, descriptor, nElements,
                                      firstElement, stride, elementSize);
  RETURN_WITH_FAM_EXCEPTION
  if (ret) {
    message << "Error in fam_scatter_atomic - Error code:" << ret;
    throw Fam_Exception(FAM_ERR_ATL, message.str().c_str());
  }
}
void ATLib::fam_gather_atomic(void *local, Fam_Descriptor *descriptor,
                              uint64_t nElements, uint64_t firstElement,
                              uint64_t stride, uint64_t elementSize) {
  int ret;
  std::ostringstream message;
  TRY_CATCH_BEGIN
  ret = pATLimpl_->fam_gather_atomic(local, descriptor, nElements, firstElement,
                                     stride, elementSize);
  RETURN_WITH_FAM_EXCEPTION
  if (ret) {
    message << "Error in fam_gather_atomic - Error code:" << ret;
    throw Fam_Exception(FAM_ERR_ATL, message.str().c_str());
  }
}
void ATLib::fam_scatter_atomic(void *local, Fam_Descriptor *descriptor,
                               uint64_t nElements, uint64_t *elementIndex,
                               uint64_t elementSize) {
  int ret;
  std::ostringstream message;
  TRY_CATCH_BEGIN
  ret = pATLimpl_->fam_scatter_atomic(local, descriptor, nElements,
                                      elementIndex, elementSize);
  RETURN_WITH_FAM_EXCEPTION
  if (ret) {
    message << "Error in fam_scatter_atomic - Error code:" << ret;
    throw Fam_Exception(FAM_ERR_ATL, message.str().c_str());
  }
}
void ATLib::fam_gather_atomic(void *local, Fam_Descriptor *descriptor,
                              uint64_t nElements, uint64_t *elementIndex,
                              uint64_t elementSize) {
  int ret;
  std::ostringstream message;
  TRY_CATCH_BEGIN
  ret = pATLimpl_->fam_gather_atomic(local, descriptor, nElements, elementIndex,
                                     elementSize);
  RETURN_WITH_FAM_EXCEPTION
  if (ret) {
    message << "Error in fam_gather_atomic - Error code:" << ret;
    throw Fam_Exception(FAM_ERR_ATL, message.str().c_str());
  }
}

int ATLib::finalize() {
    return pATLimpl_->atl_finalize();
}
ATLib::ATLib() {
    pATLimpl_ = new ATLimpl_();
}
ATLib::~ATLib() {
    if (pATLimpl_)
        delete pATLimpl_;
}
} //namespace

