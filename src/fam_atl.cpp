#include <iostream>
#include <fam/fam.h>
#include <fam/fam_exception.h>
#include <common/fam_options.h>
#include <common/fam_internal.h>
#include <map>
#include <string>
#include <rpc/fam_rpc_client.h>
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

using RpcClientMap = std::map<uint64_t, Fam_Rpc_Client *>;
using MemServerMap = std::map<uint64_t, std::string>;

class ATLib::ATLimpl_ {

private:
    Fam_Options famOptions;
    Fam_Thread_Model famThreadModel;
    Fam_Context_Model famContextModel;
    MemServerMap memoryServerList;
    RpcClientMap *rpcClients; // = new RpcClientMap();
    struct fi_info *fi;
    struct fid_fabric *fabric;
    struct fid_eq *eq;
    struct fid_domain *domain;
    struct fid_av *av;
    size_t fabric_iov_limit;
    size_t serverAddrNameLen;
    void *serverAddrName;
    size_t selfAddrNameLen=0;
    void *selfAddrName;
    std::vector<fi_addr_t> *fiAddrs;
    Fam_Context *defaultCtx;
    std::map<uint64_t, uint32_t> *selfAddrLens;
    std::map<uint64_t, char *> *selfAddrs;
    std::map<uint64_t, Fam_Context *> *defContexts;

public:

ATLimpl_() {
    rpcClients = new RpcClientMap();
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
    famOptions.memoryServer = (char *)inp_fam->fam_get_option(strdup("MEMORY_SERVER"));
    famOptions.libfabricPort = (char *)inp_fam->fam_get_option(strdup("LIBFABRIC_PORT"));
    famOptions.libfabricProvider = (char *)inp_fam->fam_get_option(strdup("LIBFABRIC_PROVIDER"));
    famOptions.famThreadModel = (char *)inp_fam->fam_get_option(strdup("FAM_THREAD_MODEL"));
    if (strcmp(famOptions.famThreadModel, FAM_THREAD_SERIALIZE_STR) == 0)
        famThreadModel = FAM_THREAD_SERIALIZE;
    else if (strcmp(famOptions.famThreadModel, FAM_THREAD_MULTIPLE_STR) == 0)
        famThreadModel = FAM_THREAD_MULTIPLE;
    famOptions.allocator = (char *)inp_fam->fam_get_option(strdup("ALLOCATOR"));
    famOptions.famContextModel =  (char *)inp_fam->fam_get_option(strdup("FAM_CONTEXT_MODEL"));
    if (strcmp(famOptions.famContextModel, FAM_CONTEXT_DEFAULT_STR) == 0)
        famContextModel = FAM_CONTEXT_DEFAULT;
    else if (strcmp(famOptions.famContextModel, FAM_CONTEXT_REGION_STR) == 0)
        famContextModel = FAM_CONTEXT_REGION;
    famOptions.runtime = (char *)inp_fam->fam_get_option(strdup("RUNTIME"));
    famOptions.numConsumer = (char *)inp_fam->fam_get_option(strdup("NUM_CONSUMER"));
    return 0;
}

Fam_Rpc_Client *get_rpc_client(uint64_t memoryServerId) {
    auto obj = rpcClients->find(memoryServerId);
    if (obj == rpcClients->end()) {
        throw Fam_Allocator_Exception(FAM_ERR_RPC_CLIENT_NOTFOUND,
                                      "RPC client not found");
    }
    return obj->second;
}

MemServerMap parse_memserver_list(std::string memServer,
                                  std::string delimiter1,
                                  std::string delimiter2) {
    MemServerMap memoryServerList;
    uint64_t prev1 = 0, pos1 = 0;
    do {
        pos1 = memServer.find(delimiter1, prev1);
        if (pos1 == string::npos)
            pos1 = memServer.length();
        std::string token = memServer.substr(prev1, pos1 - prev1);
        if (!token.empty()) {
            uint64_t prev2 = 0, pos2 = 0, count = 0, nodeid = 0;
            do {
                pos2 = token.find(delimiter2, prev2);
                if (pos2 == string::npos)
                    pos2 = token.length();
                std::string token2 = token.substr(prev2, pos2 - prev2);
                if (!token2.empty()) {
                    if (count % 2 == 0) {
                        nodeid = stoull(token2);
                    } else {
                        memoryServerList.insert({ nodeid, token2 });
                    }
                    count++;
                }
                prev2 = pos2 + delimiter2.length();
            } while (pos2 < token.length() && prev2 < token.length());
        }
        prev1 = pos1 + delimiter1.length();
    } while (pos1 < memServer.length() && prev1 < memServer.length());
    return memoryServerList;
}

int atl_initialize(fam *inp_fam) {
    std::ostringstream message;
    int ret = 0;
    uint64_t memoryServerCount;
//    FAM_PROFILE_INIT();
//    peCnt = (int *)malloc(sizeof(int));
//    peId = (int *)malloc(sizeof(int));
//    if (grpName)
//        groupName = strdup(grpName);
//    else {
//        message << "Fam Invalid Option specified: GroupName";
//        throw Fam_InvalidOption_Exception(message.str().c_str());
//    }
//    optValueMap = new std::map<std::string, const void *>();

//    optValueMap->insert({ supportedOptionList[VERSION], strdup("0.0.1") });

    memoryServerCount = 1;

//    if ((ret = validate_fam_options(options)) < 0) {
    if ((ret = populate_fam_options(inp_fam)) < 0) {
        return ret;
    }
    std::string memoryServer = famOptions.memoryServer;

    MemServerMap memoryServerList;
    std::string delimiter1 = ",";
    std::string delimiter2 = ":";

    memoryServerList =
        parse_memserver_list(memoryServer, delimiter1, delimiter2);

    if (memoryServerList.size() == 0) {
        throw Fam_InvalidOption_Exception("Memory server list not found");
    }

    memoryServerCount = memoryServerList.size();
    for (auto it=memoryServerList.begin(); it!=memoryServerList.end(); ++it) {
        if (it->first >= memoryServerCount ) {
            message << "Fam Invalid memory server ID specified: " << it->first
                   << " should be less than memory server count" ;
            throw Fam_InvalidOption_Exception(message.str().c_str());
        }
    }

    for (auto obj = memoryServerList.begin(); obj != memoryServerList.end(); ++obj) {
        Fam_Rpc_Client *client = new Fam_Rpc_Client((obj->second).c_str(), atoi(famOptions.grpcPort));
        rpcClients->insert({ obj->first, client });
    }

    fiAddrs = new std::vector<fi_addr_t>();

    uint64_t nodeId = 0;

    const char *memServerName = memoryServerList[nodeId].c_str();
    if ((ret = fabric_initialize(memServerName, famOptions.libfabricPort, 0, famOptions.libfabricProvider,
                                 &fi, &fabric, &eq, &domain, famThreadModel)) <
        0) {
        return ret;
    }

    // Initialize address vector
    if (fi->ep_attr->type == FI_EP_RDM) {
        if ((ret = fabric_initialize_av(fi, domain, eq, &av)) < 0) {
            return ret;
        }
    }
    for (nodeId = 0; nodeId < memoryServerList.size(); nodeId++) {
        Fam_Rpc_Client *rpcClient = get_rpc_client(nodeId);
        serverAddrNameLen = rpcClient->get_addr_size();
        if (serverAddrNameLen <= 0) {
            message << "Fam allocator get_addr_size failed";
            throw Fam_Allocator_Exception(FAM_ERR_ALLOCATOR,
                                      message.str().c_str());
        }
        serverAddrName = calloc(1, serverAddrNameLen);
        if (memcpy(serverAddrName, rpcClient->get_addr(), serverAddrNameLen) == NULL) {

            message << "Fam Allocator get_addr failed";
            throw Fam_Allocator_Exception(FAM_ERR_ALLOCATOR,
                                          message.str().c_str());
        }
        ret = fabric_insert_av((char *)serverAddrName, av, fiAddrs);
        if (ret < 0) {
            // TODO: Log error
            return ret;
        }
        if (famContextModel == FAM_CONTEXT_DEFAULT) {
        defaultCtx =
            new Fam_Context(fi, domain, famThreadModel);
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
            throw Fam_Datapath_Exception(message.str().c_str());
        }
        selfAddrName = calloc(1, selfAddrNameLen);
        ret = fabric_getname(defaultCtx->get_ep(), selfAddrName, (size_t *)&selfAddrNameLen);
        if (ret < 0) {
            message << "Fam libfabric fabric_getname failed: "
                    << fabric_strerror(ret);
            throw Fam_Datapath_Exception(message.str().c_str());
        }
        selfAddrLens->insert({nodeId, (uint32_t)selfAddrNameLen});
        selfAddrs->insert({nodeId, (char *)selfAddrName});
        }
    } //nodeid
    fabric_iov_limit = fi->tx_attr->rma_iov_limit;

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

    if (rpcClients != NULL) {
        for (auto rpc_client : *rpcClients) {
            delete rpc_client.second;
        }
        rpcClients->clear();
    }
    delete rpcClients;

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

    free(serverAddrName);

    if (defaultCtx != NULL)
        delete defaultCtx;
    return 0;
}

int validate_item(Fam_Descriptor *descriptor) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    Fam_Rpc_Client *rpcClient = get_rpc_client(descriptor->get_memserver_id());
    Fam_Region_Item_Info itemInfo;

    if (key == FAM_KEY_UNINITIALIZED) {
        itemInfo = rpcClient->check_permission_get_info(descriptor);
        descriptor->bind_key(itemInfo.key);
        descriptor->set_size(itemInfo.size);
        if (strcmp(famOptions.allocator, FAM_OPTIONS_NVMM_STR) == 0) {
            descriptor->set_base_address(itemInfo.base);
        }
    }

    if (key == FAM_KEY_INVALID) {
        message << "Invalid Key Passed" << endl;
        throw Fam_InvalidOption_Exception(message.str().c_str());
    }
    return 0;
}

uint32_t get_selfAddrLen(uint64_t nodeId) {
    auto obj = selfAddrLens->find(nodeId);
    if (obj == selfAddrLens->end())
        throw Fam_Datapath_Exception("Addrlen for self not found");
    else
        return obj->second;
};

char * get_selfAddr(uint64_t nodeId) {
    auto obj = selfAddrs->find(nodeId);
    if (obj == selfAddrs->end())
        throw Fam_Datapath_Exception("Addr for self not found");
    else
        return obj->second;
};

int fam_get_atomic(void *local, Fam_Descriptor *descriptor,
                        uint64_t offset,size_t nbytes) {

    int ret = 0;
    std::ostringstream message;
    fid_mr *mr = 0;
    uint64_t key = 0;
    int32_t retStatus = -1;


//    FAM_CNTR_INC_API(fam_get_atomic);
//    FAM_PROFILE_START_ALLOCATOR(fam_get_atomic);
    if ((local == NULL) || (descriptor == NULL) || (nbytes == 0)) {
        throw Fam_InvalidOption_Exception("Invalid Options");
    }
    if ((offset + nbytes) > descriptor->get_size()) {
        throw Fam_InvalidOption_Exception("Invalid Options");
    }

    ret = validate_item(descriptor);
//    FAM_PROFILE_END_ALLOCATOR(fam_get_atomic);
//    FAM_PROFILE_START_OPS(fam_get_atomic);
    if (ret == 0) {
        // Read data from FAM region with this key
        size_t dataSize = descriptor->get_size();
        Fam_Global_Descriptor globalDescriptor = descriptor->get_global_descriptor();
        uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;

        key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
        key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
        key |= 1;
        ret = fabric_register_mr(local, dataSize, &key,
                                     domain, 1, mr);
        if (ret < 0) {
            cout << "error: memory register failed" << endl;
            return -4;
        }

        uint64_t nodeId = descriptor->get_memserver_id();
        Fam_Context *ATLCtx = defaultCtx;
        fi_context *ctx = fabric_post_response_buff(&retStatus,(*fiAddrs)[nodeId], ATLCtx,sizeof(retStatus));
        Fam_Rpc_Client *rpcClient = get_rpc_client(nodeId);
        rpcClient->get_atomic(local, descriptor, offset, nbytes, key,nodeId,get_selfAddr(nodeId),get_selfAddrLen(nodeId));

        fabric_recv_completion_wait(ATLCtx, ctx);
        if (retStatus != 0)
            cout << "Error in get_atomic" << endl;
        else cout << "Success in get_atomic" << endl;
        ret = fabric_deregister_mr(mr);
        return 0;

    }
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

//    FAM_CNTR_INC_API(fam_put_atomic);
//    FAM_PROFILE_START_ALLOCATOR(fam_put_atomic);
    if ((local == NULL) || (descriptor == NULL) || (nbytes == 0)) {
        throw Fam_InvalidOption_Exception("Invalid Options");
    }
    if ((offset + nbytes) > descriptor->get_size()) {
        throw Fam_InvalidOption_Exception("Invalid Options");
    }

    ret = validate_item(descriptor);
//    FAM_PROFILE_END_ALLOCATOR(fam_put_atomic);
//    FAM_PROFILE_START_OPS(fam_put_atomic);
    if (ret == 0) {
        // Read data from FAM region with this key
        size_t dataSize = descriptor->get_size();

        if (nbytes > MAX_DATA_IN_MSG) {
            Fam_Global_Descriptor globalDescriptor = descriptor->get_global_descriptor();
            uint64_t dataitemId = globalDescriptor.offset / MIN_OBJ_SIZE;
            key |= (globalDescriptor.regionId & REGIONID_MASK) << REGIONID_SHIFT;
            key |= (dataitemId & DATAITEMID_MASK) << DATAITEMID_SHIFT;
            key |= 1;
            ret = fabric_register_mr(local, dataSize, &key,
                                     domain, 1, mr);
            if (ret < 0) {
                cout << "error: memory register failed" << endl;
                return -4;
            }
        }
        uint64_t nodeId = descriptor->get_memserver_id();
        Fam_Context *ATLCtx = defaultCtx;
        fi_context *ctx = fabric_post_response_buff(&retStatus,(*fiAddrs)[nodeId], ATLCtx,sizeof(retStatus));
        Fam_Rpc_Client *rpcClient = get_rpc_client(nodeId);

        ret = rpcClient->put_atomic(local, descriptor,offset,nbytes,key,nodeId,get_selfAddr(nodeId),get_selfAddrLen(nodeId));

        fabric_recv_completion_wait(ATLCtx, ctx);
        if (retStatus != 0)
            cout << "Error in put_atomic" << endl;
        else
            cout << "Success in put_atomic" << endl;
        if (nbytes > MAX_DATA_IN_MSG)
            ret = fabric_deregister_mr(mr);
        return 0;
    }
//    FAM_PROFILE_END_OPS(fam_put_atomic);
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

