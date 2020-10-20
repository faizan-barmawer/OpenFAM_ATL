#include <assert.h>
#include <chrono>
#include <fam/fam.h>
#include <fam/fam_exception.h>
#include <fam_atl.h>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
using namespace std;
using namespace openfam;
#define DATA_REGION "test"

#define TEST_OPENFAM_MODEL "memory_server"
#define TEST_CIS_INTERFACE_TYPE "rpc"
#define TEST_CIS_SERVER "127.0.0.1"
#define TEST_GRPC_PORT "8787"
#define TEST_LIBFABRIC_PROVIDER "sockets"
#define TEST_FAM_THREAD_MODEL "FAM_THREAD_SERIALIZE"
#define TEST_FAM_CONTEXT_MODEL "FAM_CONTEXT_DEFAULT"
#define TEST_RUNTIME "PMIX"

void init_fam_options(Fam_Options *famOpts) {
  memset((void *)famOpts, 0, sizeof(Fam_Options));

  famOpts->openFamModel = strdup(TEST_OPENFAM_MODEL);
  famOpts->cisInterfaceType = strdup(TEST_CIS_INTERFACE_TYPE);
  famOpts->cisServer = strdup(TEST_CIS_SERVER);
  famOpts->grpcPort = strdup(TEST_GRPC_PORT);
  famOpts->libfabricProvider = strdup(TEST_LIBFABRIC_PROVIDER);
  famOpts->famThreadModel = strdup(TEST_FAM_THREAD_MODEL);
  famOpts->famContextModel = strdup(TEST_FAM_CONTEXT_MODEL);
  famOpts->runtime = strdup(TEST_RUNTIME);
}

int main() {
  fam *my_fam = new fam();
  ATLib *myatlib = new ATLib();
  Fam_Options fam_opts;
  Fam_Region_Descriptor *dataRegion = NULL; //, *bufferRegion = NULL;
  Fam_Descriptor *item1 = NULL;
  int i;
  memset((void *)&fam_opts, 0, sizeof(Fam_Options));
  init_fam_options(&fam_opts);
  cout << "PID ready: gdb attach " << getpid() << endl;
  //  sleep(60);
  cout << " Calling fam_initialize" << endl;
  try {
    my_fam->fam_initialize("default", &fam_opts);
  } catch (Fam_Exception &e) {
    cout << "fam initialization failed" << endl;
    exit(1);
  }
  myatlib->initialize(my_fam);
  try {
    dataRegion = my_fam->fam_lookup_region(DATA_REGION);
  } catch (Fam_Exception &e) {
    cout << "data Region not found" << endl;
    dataRegion = my_fam->fam_create_region(DATA_REGION, 1048576, 0777, RAID1);
  }
  char msg1[200] = {0};
  char msg2[200] = {0};
  try {
    item1 = my_fam->fam_lookup("item1", DATA_REGION);
  } catch (Fam_Exception &e) {
    item1 = my_fam->fam_allocate("item1", 200, 0777, dataRegion);
  }
  cout << "Test 1: starting complete Get & Put atomic" << endl;
  for (i = 0; i < 200; i++)
    msg1[i] = 'X';
  auto start = std::chrono::high_resolution_clock::now();
  myatlib->fam_put_atomic((void *)msg1, item1, 0, 200); // strlen(msg1));
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed_seconds = end - start;
  cout << "put atomic elapsed time: " << elapsed_seconds.count() << endl;
  start = std::chrono::high_resolution_clock::now();
  myatlib->fam_get_atomic((void *)msg2, item1, 0, 200); // strlen(msg1));
  end = std::chrono::high_resolution_clock::now();
  elapsed_seconds = end - start;
  cout << "get atomic elapsed time: " << elapsed_seconds.count() << endl;
  cout << msg2 << endl;
  if (strcmp(msg1, msg2) != 0)
    cout << "Test1: Comparison of full string failed" << endl;
  else
    cout << "Test1: Comparison of full string successful" << endl;
  cout << "complete atomic Get & Put completed " << endl;
  for (i = 0; i < 200; i++)
    msg1[i] = 'Y';
  // sleep(30);
  cout << "Scatter atomic - strided" << endl;
  start = std::chrono::high_resolution_clock::now();
  myatlib->fam_scatter_atomic(msg1, item1, 5, 1, 2, 2);
  end = std::chrono::high_resolution_clock::now();
  elapsed_seconds = end - start;
  cout << "Scatter strided atomic elapsed time: " << elapsed_seconds.count()
       << endl;
  myatlib->fam_get_atomic((void *)msg2, item1, 0, 200);
  cout << msg2 << endl;
  cout << "Gather atomic - strided" << endl;
  memset(msg2, 0, 200);
  start = std::chrono::high_resolution_clock::now();
  myatlib->fam_gather_atomic(msg2, item1, 5, 1, 2, 2);
  end = std::chrono::high_resolution_clock::now();
  elapsed_seconds = end - start;
  cout << "Gather strided atomic elapsed time: " << elapsed_seconds.count()
       << endl;
  cout << msg2 << endl;
  if (strcmp(msg2, "YYYYYYYYYY") == 0)
    cout << "Gather strided test successful" << endl;
  else
    cout << "Gather strided test failed" << endl;
  cout << "Scatter atomic - indexed" << endl;
  for (i = 0; i < 200; i++)
    msg1[i] = 'Z';
  uint64_t indexes[] = {10, 17, 13, 15, 30};
  start = std::chrono::high_resolution_clock::now();
  myatlib->fam_scatter_atomic(msg1, item1, 5, indexes, 2);
  end = std::chrono::high_resolution_clock::now();
  elapsed_seconds = end - start;
  cout << "Scatter indexed atomic elapsed time: " << elapsed_seconds.count()
       << endl;
  myatlib->fam_get_atomic((void *)msg2, item1, 0, 200);
  cout << msg2 << endl;
  cout << "Gather atomic - indexed" << endl;
  memset(msg2, 0, 200);
  start = std::chrono::high_resolution_clock::now();
  myatlib->fam_gather_atomic(msg2, item1, 5, indexes, 2);
  end = std::chrono::high_resolution_clock::now();
  elapsed_seconds = end - start;
  cout << "Gather indexed atomic elapsed time: " << elapsed_seconds.count()
       << endl;
  cout << msg2 << endl;
  if (strcmp(msg2, "ZZZZZZZZZZ") == 0)
    cout << "Gather indexed test successful" << endl;
  else
    cout << "Gather indexed test failed" << endl;

  myatlib->finalize();
  my_fam->fam_finalize("default");
  cout << "fam finalize successful" << endl;
}
