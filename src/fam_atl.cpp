/*
 * fam_atl.cpp
 * Copyright (c) 2019 Hewlett Packard Enterprise Development, LP. All rights
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
    uint64_t numMemoryNodes;
    uint64_t nodeId;

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
    numMemoryNodes = famCIS->get_num_memory_servers();
    if (numMemoryNodes == 0) {
        message << "Libfabric initialize: memory server name not specified";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }

    for (nodeId = 0; nodeId < numMemoryNodes; nodeId++) {
	serverAddrNameLen = famCIS->get_addr_size(nodeId);
        if (serverAddrNameLen <= 0) {
            message << "Fam allocator get_addr_size failed";
            THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());

        }
        serverAddrName = calloc(1, serverAddrNameLen);
	famCIS->get_addr(serverAddrName, nodeId);

        ret = fabric_insert_av((char *)serverAddrName, av, fiAddrs);
        if (ret < 0) {
            // TODO: Log error
            return ret;
        }
        if (famContextModel == FAM_CONTEXT_DEFAULT) {
            defaultCtx = new Fam_Context(fi, domain, famThreadModel);
            defContexts->insert({nodeId, defaultCtx});
            ret = fabric_enable_bind_ep(fi, av, eq, defaultCtx->get_ep());
            if (ret < 0) {
            // TODO: Log error
            	return ret;
            }
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
    } //nodeid

    return 0;
}
int atl_finalize() {
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
    	uint64_t regionId = globalDescriptor.regionId;
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

//    FAM_CNTR_INC_API(fam_get_atomic);
//    FAM_PROFILE_START_ALLOCATOR(fam_get_atomic);
    if ((local == NULL) || (descriptor == NULL) || (nbytes == 0)) {
	message << "Invalid Options";
	THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    if ((offset + nbytes) > descriptor->get_size()) {
        message << "Invalid Options";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }

    ret = validate_item(descriptor);
//    FAM_PROFILE_END_ALLOCATOR(fam_get_atomic);
//    FAM_PROFILE_START_OPS(fam_get_atomic);
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
        Fam_Context *ATLCtx = defaultCtx;
        fi_context *ctx = fabric_post_response_buff(&retStatus,(*fiAddrs)[nodeId], ATLCtx,sizeof(retStatus));
        ret = famCIS->get_atomic(globalDescriptor.regionId & REGIONID_MASK,
				 globalDescriptor.offset, offset, nbytes,
				 key, get_selfAddr(nodeId),get_selfAddrLen(nodeId),
				 nodeId, uid, gid);
			
        if (ret == 0) {
            fabric_recv_completion_wait(ATLCtx, ctx);
//            if (retStatus != 0)
//                cout << "Error in put_atomic" << endl;
//            else
//                cout << "Success in put_atomic" << endl;
            fabric_deregister_mr(mr);
            ret = retStatus;
        }
        return ret;
    } //validate_item()
//    FAM_PROFILE_END_OPS(fam_get_atomic);
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
//    FAM_CNTR_INC_API(fam_put_atomic);
//    FAM_PROFILE_START_ALLOCATOR(fam_put_atomic);
    if ((local == NULL) || (descriptor == NULL) || (nbytes == 0)) {
        message << "Invalid Options";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }
    if ((offset + nbytes) > descriptor->get_size()) {
        message << "Invalid Options";
        THROW_ATL_ERR_MSG(ATL_Exception, message.str().c_str());
    }

    ret = validate_item(descriptor);
//    FAM_PROFILE_END_ALLOCATOR(fam_put_atomic);
//    FAM_PROFILE_START_OPS(fam_put_atomic);
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
        Fam_Context *ATLCtx = defaultCtx;
	if (nbytes > MAX_DATA_IN_MSG) 
            ctx = fabric_post_response_buff(&retStatus,(*fiAddrs)[nodeId], ATLCtx,sizeof(retStatus));

        ret = famCIS->put_atomic(globalDescriptor.regionId & REGIONID_MASK,
                                 globalDescriptor.offset, offset, nbytes,
                                 key, get_selfAddr(nodeId),get_selfAddrLen(nodeId),
                                 (const char *)local, nodeId, uid, gid);

	if ((ret == 0) && (nbytes > MAX_DATA_IN_MSG)) {
            fabric_recv_completion_wait(ATLCtx, ctx);
//            if (retStatus != 0)
//                cout << "Error in put_atomic" << endl;
//            else
//                cout << "Success in put_atomic" << endl;
	    ret = retStatus;
	}
	if (nbytes > MAX_DATA_IN_MSG)
	    fabric_deregister_mr(mr);
        return ret;

    //    FAM_PROFILE_END_OPS(fam_put_atomic);
    } //validate_item
    return ret;

}
}; //class

int ATLib::initialize(fam *inp_fam) {
    return pATLimpl_->atl_initialize(inp_fam);
}

int ATLib::fam_get_atomic(void *local, Fam_Descriptor *descriptor,
                          uint64_t offset, uint64_t nbytes) {
    return pATLimpl_->fam_get_atomic(local, descriptor, offset, nbytes);
}
int ATLib::fam_put_atomic(void *local, Fam_Descriptor *descriptor,
                          uint64_t offset, uint64_t nbytes) {
    return pATLimpl_->fam_put_atomic(local, descriptor, offset, nbytes);
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

