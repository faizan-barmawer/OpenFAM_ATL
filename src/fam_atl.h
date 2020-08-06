#ifndef FAM_ATL_H_
#define FAM_ATL_H_
#include <fam/fam.h>
#include <cstddef>
namespace openfam {
class ATLib {
public:
    int initialize(fam * inp_fam);
    int finalize();

    int fam_get_atomic(void *local, Fam_Descriptor *descriptor,
                        uint64_t offset,size_t nbytes);
    int fam_put_atomic(void *local, Fam_Descriptor *descriptor,
                        uint64_t offset,size_t nbytes);

    ATLib(); 
    ~ATLib();
private:
    class ATLimpl_;
    ATLimpl_ *pATLimpl_;
};

}

#endif

