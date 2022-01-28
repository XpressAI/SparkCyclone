/*
 * Sample program for VESHM open and close
 *
 * This example registers a VESHM as below.
 * Map on VHSAA, Read/Write, memory size 64MB, No sync.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/types.h>
#include <unistd.h>
#include <veshm.h>

int main(int argc, char *argv[]){
	int ret = 0;
	void *area, *start;
	uint64_t offset;
	uint64_t aligned_size;
	int i;
	int pid;

	int pci_pgsize = 0;
	int mode_flag = 0;

	int shared_size = (1*1024*1024);
	int opt_syncnum = -1;
	int opt_sleep   = 90;

	pid = getpid();
	printf ("Sleeping           : %d seconds\n", opt_sleep);
	printf ("Owner pid          : %d\n", pid);
	printf ("Requested Mem size : %#x(Byte)\n", shared_size);
	mode_flag = (mode_flag | VE_REGISTER_PCI);

	pci_pgsize = ve_get_pgmode(VE_ADDR_PCI, pid, NULL);
	if (pci_pgsize == -1){
		perror("Get PCI page size");
		return 1;
	}

	/* Align address and size.
	 * In this program, rounded down adress and rounded up memory size
	 * will be passed to VESHM open API.
	 */

	area   = (char *) malloc (shared_size);
	start  = (void *)((uint64_t) area & ~(pci_pgsize - 1));
	offset = (uint64_t) area - (uint64_t) start;

	aligned_size = (offset + shared_size + (unsigned int)pci_pgsize - 1)
		& ~((unsigned int)pci_pgsize - 1);

	printf ("\n");
	printf ("VESHM start address: %p\n", start);
	printf ("VESHM offset       : %#x\n", offset);
	printf ("VESHM size         : %#x(Byte)\n\n", aligned_size);

	*(long long *)area = 123456789;

	/* VESHM open request */
	ret = ve_shared_mem_open(start, aligned_size, opt_syncnum, mode_flag);
	printf ("VESHM Open: return:%d, errno:%d\n", ret, errno);

	for (i = 0; i < opt_sleep; i++){
		sleep(1);
	}

	/* VESHM close request */
	ret = ve_shared_mem_close(start, aligned_size, opt_syncnum, mode_flag);
	printf ("VESHM close: return:%d, errno:%d\n", ret, errno);

	free(area);
	return 0;
}
