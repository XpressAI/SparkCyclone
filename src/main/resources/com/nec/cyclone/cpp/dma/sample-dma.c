/*
 * Sample program of DMA transfer
 *
 * This example ataches a VESHM registered by sample-owner.c.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <veshm.h>
#include <vedma.h>

#define PG_64MB (64*1024*1024)


#define usage "-p pid -v address -o offset -m size(MB) "\
	"[-h]\n\
	-p : PID of VESHM owner \n \
	-v : Address of VESHM (VEMVA of owner process)\n \
	-o : Offset of VESHM (offset in the owner's VESHM)\n \
	-m : VESHM memory size (Byte) (Default=0x200000Byte)\n\
	[other] \n\
	-h : help"

int main(int argc, char *argv[])
{
	int i;
	int ret;
	void *area, *start;
	uint64_t offset;
	int aligned_size;
	int pid, opt_my_pid;
	uint64_t align = sysconf(_SC_PAGESIZE);

	uint64_t *remote_vehva = 0;

	int mode_flag;
	int mode_flag_attach;
	int mode_flag_detach;

	int result;
	int opt_owner_pid = -1;
	uint64_t opt_vemva = 0;
	uint64_t opt_remote_offset = 0;
	int shared_size = PG_64MB;
	int opt_syncnum = -1;

	uint64_t local_vehva = 0;

	/* option */
	while ((result=getopt(argc,argv, "m:v:o:p:s:h")) != -1){
		switch(result){
		case 'm':
			shared_size = strtol(optarg, NULL, 0);
			break;
		case 'v':
			opt_vemva = strtoll(optarg, NULL, 0);
			break;
		case 'o':
			opt_remote_offset = strtoll(optarg, NULL, 0);
			break;
		case 'p':
			opt_owner_pid = (int)strtol(optarg, NULL, 0);
			break;
		case 'h':
		default:
			printf ("Usage: %s %s\n", argv[0], usage);
			exit(0);
		}
	}
	opt_my_pid = pid = getpid();

	printf ("Pid		: %d\n", opt_my_pid);
	printf ("VESHM Owner pid: %d\n", opt_owner_pid);
	printf ("VESHM VEMVA    : 0x%"PRIx64"\n", opt_vemva);
	printf ("VESHM offset   : 0x%"PRIx64"\n", opt_remote_offset);
	printf ("Memory size    : %#x(Byte)\n", shared_size);


	/* Make mode_flag_attach */
	mode_flag_attach = (VE_REGISTER_VEHVA   /* flag of remote VESHM */
			    | VE_REGISTER_PCI); /* flag for this process */

	/* Make mode_flag_detach */
	mode_flag_detach = VE_REGISTER_VEHVA;


	/* ---------- Checking options END--------------------------- */


	/* Set up local VESHM */

	area   = (char *) malloc (shared_size);
	start  = (void *)((uint64_t) area & ~(align - 1));
	offset = (uint64_t) area - (uint64_t) start;

	aligned_size = (offset + shared_size + (unsigned int)align - 1)
		& ~((unsigned int)align - 1);

	printf ("Allocated address: %p\n", area);
	printf ("Aligned addres   : %#"PRIx64", aligned_size:%#x, pid:%d\n",
		opt_vemva, aligned_size, opt_my_pid);

	/* Register the local memory as VESHM (VE_MEM_LOCAL) using dma API */
	local_vehva = ve_register_mem_to_dmaatb(start, aligned_size);
	printf("[%d] MEM_LOCAL address= %#llx, errno=%d\n", opt_my_pid,
	       (long long)local_vehva, errno);
	if (local_vehva == (uint64_t)-1 || local_vehva == 0){
		free (area);
		return 1;
	}


	/* Attach a remote VESHM */
	remote_vehva = ve_shared_mem_attach(opt_owner_pid,
					  (void *)opt_vemva,
					  shared_size, opt_syncnum,
					  mode_flag_attach);
	printf("[%d] Remote VESHM remote_vehva = %#llx, errno=%d\n",
	       opt_my_pid, (long long)remote_vehva, errno);
	if ((int64_t)remote_vehva == -1 || (int64_t)remote_vehva == 0){
		printf("attach failed.\n");
		ve_unregister_mem_from_dmaatb(local_vehva);
		free(area);
		return 1;
	}

	/* Read data using DMA */
	if (ve_dma_init() != 0) {
		perror("ve_dma_init");
		ve_unregister_mem_from_dmaatb(local_vehva);
		free(area);
		return 1;
	}

	ret = ve_dma_post_wait(local_vehva+offset,
			       (uint64_t)remote_vehva+opt_remote_offset,
			       sizeof(long long));
	printf("ve_dma_post_wait(read): ret=%d\n", ret);
	printf("%ld\n", *(long long *)area);


	/* Detach VESHMs */

	ret = ve_shared_mem_detach((void *)remote_vehva, mode_flag_detach);
	printf("[%d] VESHM detach: ret = %d, errno=%d\n", pid, ret, errno);

	ve_unregister_mem_from_dmaatb(local_vehva);
	printf("[%d] MEM_LOCAL detach: ret = %d, errno=%d\n", pid, ret, errno);

	free(area);
	return 0;
}
