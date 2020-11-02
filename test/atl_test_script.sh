#!/bin/bash
# arg1 = libopenfam.so path, arg 2 = libopenfam_atl.so path
# Fam cis and memory server should be started
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$1:$2
~/OpenFAM/third-party/build/bin/mpirun -np 1 ./fam_atl_get_put
~/OpenFAM/third-party/build/bin/mpirun -np 1 ./fam_SG_ATL
~/OpenFAM/third-party/build/bin/mpirun -np 1 ./atl_multi_get_put
~/OpenFAM/third-party/build/bin/mpirun -np 1 ./atl_parallel_test 1 &
~/OpenFAM/third-party/build/bin/mpirun -np 1 ./atl_parallel_test 2 &

