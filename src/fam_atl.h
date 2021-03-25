/*
 * fam_atl.h
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

#ifndef FAM_ATL_H_
#define FAM_ATL_H_
#include <fam/fam.h>
#include <cstddef>
#include <exception>
namespace openfam {
class ATLib {
public:
    int initialize(fam * inp_fam);
    int finalize();

    void fam_get_atomic(void *local, Fam_Descriptor *descriptor,
                        uint64_t offset, size_t nbytes);
    void fam_put_atomic(void *local, Fam_Descriptor *descriptor,
                        uint64_t offset, size_t nbytes);
    void fam_scatter_atomic(void *local, Fam_Descriptor *descriptor,
                            uint64_t nElements, uint64_t firstElement,
                            uint64_t stride, uint64_t elementSize);

    void fam_gather_atomic(void *local, Fam_Descriptor *descriptor,
                           uint64_t nElements, uint64_t firstElement,
                           uint64_t stride, uint64_t elementSize);

    void fam_scatter_atomic(void *local, Fam_Descriptor *descriptor,
                            uint64_t nElements, uint64_t *elementIndex,
                            uint64_t elementSize);

    void fam_gather_atomic(void *local, Fam_Descriptor *descriptor,
                           uint64_t nElements, uint64_t *elementIndex,
                           uint64_t elementSize);

    ATLib(); 
    ~ATLib();
private:
    class ATLimpl_;
    ATLimpl_ *pATLimpl_;
};

class ATL_Exception : public std::exception {
protected:
    std::string ATLErrMsg;
public:
    ATL_Exception(const char *msg) {
	ATLErrMsg = msg;
    }
};

}

#endif

