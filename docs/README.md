## Atomic transfer library design
OpenFAM 1.0 left ensuring data consistency within FAM to the application programmer. In particular, if multiple PEs access the same part of FAM concurrently, it was the programmer's responsibility to ensure that split reads or writes do not occur in FAM. While SPMD (single program multiple data) applications are typically written to avoid these scenarios, the application programmer has to create FAM-based locks, place barriers, or make heavy use of atomics to avoid such cases if complex data structures need to be manipulated concurrently in FAM. If OpenFAM could ensure data consistency by avoiding torn reads (writes) when multiple PEs attempt to get (put) data from (to) the same part of memory, programming FAM would become much simpler.

With this in mind, we have added the following features in OpenFAM 2.0:
* Large transfer atomicity: Prevent torn reads or writes if two concurrent updates attempt to access an overlapping region of memory in some data item.
* Fault tolerance: Ensure that an update succeeds or fails, but does'nt partially succeed (i.e., if the sender dies in the middle of a transfer).

Note that it is not an objective of this design to provide a general-purpose transaction system that can support updates to multiple data items within a single transaction, since that is likely to incur substantial performance penalties. We only ensure atomicity and crash consistency for individual OpenFAM data path operations to a single data item.

## Atomic large data transfer support in OpenFAM

To provide the functionality of large transfers atomically on OpenFAM, we have implemented a library layered over OpenFAM.

![Figure](/docs/images/atl_structure.jpg)

The Atomic Transfer Library (ATL) uses buffers to provide atomic transfer support. A buffer is an OpenFAM data item that provides space on the memory server for data being transferred (to avoid over-writes in case of in-place updates), as well as for the metadata associated with the corresponding update. Space for the buffers is dynamically allocated from a predefined FAM region transparent to the application programmer. The buffer records the validity and state of the request (atomic get/put) at every step. APIs in the atomic transfer library provide atomic get and put functions for large transfers. The application calls the new APIs for the operations required to transfer the data. Conflict resolution, if any, is performed by the transfer library and the memory server, transparent to application programmer.

## APIs to support atomic transfers

The implementation provides the APIs listed in below table to support atomic transfers for large data. The fam_put_atomic/fam_scatter_atomic API returns to the caller once the data has been successfully copied from client's memory by the memory server, while the fam_get_atomic/fam_gather_atomic API returns once the data has been successfully received by the PE.

Atomic transfer API | Description
--------------------|------------
```void fam_put_atomic(void *local, Fam_Descriptor *descriptor, uint64_t offset, size_t bytes);``` | Creates a request record for the specified data item. It returns after queuing the record to server and successful data copy from client's memory by the server. The API is similar to OpenFAM [fam_put_blocking](https://openfam.github.io/fam_put.html).
```void fam_get_atomic(void *local, Fam_Descriptor *descriptor, uint64_t offset, size_t nbytes);``` | Retrieves the requested data item and returns. The API is similar to OpenFAM [fam_get_blocking](https://openfam.github.io/fam_get.html).
```void fam_scatter_atomic(void *local, Fam_Descriptor *descriptor, uint64_t nElements, uint64_t firstElement, uint64_t stride, uint64_t elementSize); ```| Creates a request record for the specified data item. It returns after queuing the record to server and successful data copy from client's memory by the server. The API is similar to OpenFAM [fam_scatter_blocking](https://openfam.github.io/fam_scatter.html), where data is scattered according to stride parameter.
```void fam_scatter_atomic(void *local, Fam_Descriptor *descriptor, uint64_t nElements, uint64_t *elementIndex, uint64_t elementSize);```| Creates a request record for the specified data item. It returns after queuing the record to server and successful data copy from client's memory by the server. The API is similar to OpenFAM [fam_scatter_blocking](https://openfam.github.io/fam_scatter.html), where data is scattered according to element index parameter.
```void fam_gather_atomic(void *local, Fam_Descriptor *descriptor, uint64_t nElements, uint64_t firstElement, uint64_t stride, uint64_t elementSize);```| Retrieves the requested data item and returns. The API is similar to OpenFAM [fam_gather_blocking](https://openfam.github.io/fam_gather.html), where the data is gathered according to stride parameter.
```void fam_gather_atomic(void *local, Fam_Descriptor *descriptor, uint64_t nElements, uint64_t *elementIndex, uint64_t elementSize);``` | Retrieves the requested data item and returns. The API is similar to OpenFAM [fam_gather_blocking](https://openfam.github.io/fam_gather.html), where data is gathered according to the element index parameter.
