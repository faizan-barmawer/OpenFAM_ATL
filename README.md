# OpenFAM_ATL
Large Atomic Transfer Library for OpenFAM

Refer [docs/README.md](https://github.com/OpenFAM/OpenFAM_ATL/blob/main/docs/README.md) for detail description of OpenFAM_ATL design and API speicification.

## Pre-Requisite
* Build and configure OpenFAM with memory server model.

## Building(Ubuntu 18.04 LTS)

1. Build and configure OpenFAM 

   a. Download OpenFAM source code and follow all the build instruction from OpenFAM [README.md](https://github.com/OpenFAM/OpenFAM/blob/master/README.md) file.
   (assuming the OpenFAM code is at directory ```$OpenFAM```)
   
   b. Build and configure OpenFAM manually by following steps 5a to 5f from OpenFAM README.md file. After executing step 5d, to set OPENFAM_ROOT, enable atl_threads in memory server. Set the value of "ATL_threads" option in corresponding config/fam_memoryserver_config.yaml configuration file to 1 or more.

2. Download the source code of OpenFAM_ATL by cloning the repository. If you wish to contribute changes back to OpenFAM_ATL, follow the [contribution guidelines](https://github.com/OpenFAM/OpenFAM_ATL/blob/main/CONTRIBUTING.md).

3. Change into the source directory (assuming the code is at directory ```$OpenFAM_ATL```):

```
$ cd $OpenFAM_ATL
```

4. Create and change into a build directory under the source directory ```$OpenFAM_ATL```.

```
$ cd $OpenFAM_ATL
$ mkdir build
$ cd build
```

5. Run cmake by pointing to the OpenFAM source code location.

```
$ cmake .. -DOPENFAM_DIR=$OpenFAM
```

The default build type is Release. To switch between Release, Debug and Coverage, use one of the below command options:

```
$ cmake .. -DCMAKE_BUILD_TYPE=Release -DOPENFAM_DIR=$OpenFAM
$ cmake .. -DCMAKE_BUILD_TYPE=Debug -DOPENFAM_DIR=$OpenFAM
$ cmake .. -DCMAKE_BUILD_TYPE=Coverage -DOPENFAM_DIR=$OpenFAM
```

6. Make OpenFAM_ATL under the current build directory.

```
$ make -j
```

7. Run OpenFAM_ATL test from the build directory.

```
$ make run-test
```
