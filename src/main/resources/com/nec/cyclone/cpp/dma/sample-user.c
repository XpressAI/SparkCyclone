/*
 * Sample program for VESHM attach and detach
 *
 * This example ataches a VESHM as below.
 * Map on VHSAA, Read/Write, memory size 64MB, No sync.
 * It is registered by sample-owner.c.
 *
 * User must specify -p option and -v option which are
 * displayed by owner process (sample-owner.c).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <veshm.h>

#define PG_64MB (64*1024*1024)


#define usage "-p -v [-a] [-m size(MB)] [-s syncnum] [-z time] "\
	"[-h]\n\
	-p : PID of VESHM owner \n \
	-v : Address of VESHM (VEMVA of owner process)\n \
	-m : VESHM memory size (Byte) (Default=0x200000Byte)\n\
	[mode_flag for target VESHM] \n\
	-L : Target VESHM doesn't use PCIATB (Default=No (= using PCIATB))\n\
	-r : Specify Read Only permission (Default=Read/Write)\n\
	-s : Synchronization (Default=Not use sync)\n\
	[mode_flag for attached area] \n\
	-r : Specify Read Only permission (Default=Read/Write)\n\
	-a : Mapped on VEMVA (Default=VEHVA)\n\
	-l : validate VE_MEM_LOCAL,  (Default=No)\n\
	[other] \n\
	-z : sleep after invoking attach (Default=10sec)\n\
	-h : help"

int main(int argc, char *argv[])
{
	int i;
	int ret;
	int pid;
	void *area, *start;
	long long *write;
	uint64_t diff;
	uint64_t malloc_size;
	int write_size;
	int aligned_size;

	uint64_t *attch_addr = 0;
	uint64_t data = 0;

	int mode_flag;
	int mode_flag_attach;
	int mode_flag_detach;

	int result;
	int opt_owner_pid = -1;
	uint64_t opt_vemva = 0;
	int shared_size = PG_64MB;
	int opt_space = 0;
	int opt_pciatb = 1;
	int opt_syncnum = -1;
	int opt_readonly = 0;
	int opt_sleep = 10;

	/* option */
	while ((result=getopt(argc,argv, "aLm:v:p:s:z:h")) != -1){
		switch(result){
		case 'a':
			opt_space = 1;
			break;
		case 'm':
			shared_size = strtol(optarg, NULL, 0);
			break;
		case 'v':
			opt_vemva = strtoll(optarg, NULL, 0);
			break;
		case 'p':
			opt_owner_pid = (int)strtol(optarg, NULL, 0);
			break;
		case 'L':
			opt_pciatb = 0;
			break;
		case 's':
			opt_syncnum = 0;
			break;
		case 'r':
			opt_readonly = 1;
			break;
		case 'z':
			opt_sleep = strtol(optarg, NULL, 0);
			if (opt_sleep < 1){
				printf ("Usage: %s %s\n", argv[0], usage);
				return 1;
			}
			break;
		case 'h':
		default:
			printf ("Usage: %s %s\n", argv[0], usage);
			exit(0);
		}
	}
	sleep(1);
	pid = getpid();

	printf ("Sleeping       : %d seconds\n", opt_sleep);

	printf ("Pid		: %d\n", pid);
	printf ("VESHM Owner pid: %d\n", opt_owner_pid);
	printf ("VESHM VEMVA    : 0x%"PRIx64"\n", opt_vemva);
	printf ("Memory size    : %#x(Byte)\n", shared_size);

	printf ("Register PCIATB: %s\n", opt_pciatb? "yes" : "no");
	printf ("PCIATB sync    : %s \n",
		(opt_syncnum != -1)? "yes" : "no" );
	printf ("Read only perm : %s\n", opt_readonly ? "yes" : "no");

	printf ("Mapped on      : %s\n", opt_space ? "VEMVA" : "VEHVA");

	/* Make mode_flag_attach */
	mode_flag = 0;

	if (opt_space == 1){
		mode_flag = (mode_flag | VE_REGISTER_VEMVA);
	} else {
		mode_flag = (mode_flag | VE_REGISTER_VEHVA);
	}

	/* Specify a target VESHM setting */
	if (opt_pciatb == 1)
		mode_flag = (mode_flag | VE_REGISTER_PCI);
	else
		mode_flag = (mode_flag | VE_REGISTER_NONE);
	if (opt_syncnum != -1)
		mode_flag = (mode_flag | VE_PCISYNC);
	if (opt_readonly == 1)
		mode_flag = (mode_flag | VE_SHM_RO);

	mode_flag_attach =  mode_flag;


	/* Make mode_flag_detach */
	mode_flag = 0;

	if (opt_space == 1){
		mode_flag = (mode_flag | VE_REGISTER_VEMVA);
	} else {
		mode_flag = (mode_flag | VE_REGISTER_VEHVA);
	}

	mode_flag_detach =  mode_flag;


	/* ---------- Checking options END--------------------------- */

	/* Call VESHM API */
	attch_addr = ve_shared_mem_attach(opt_owner_pid,
					  (void *)opt_vemva,
					  shared_size, opt_syncnum,
					  mode_flag_attach);
	printf("[%d] VESHM attach: attch_addr = %#llx, errno=%d\n", pid,
	       (long long)attch_addr, errno);
	if ((int64_t)attch_addr == -1 || (int64_t)attch_addr == 0){
		printf("attach failed.\n");
		return 1;
	}

	printf("The VESHM attached.\n");
	sleep(opt_sleep);

	/************************************************************
	 * If the attached VESHM exists on a remote VE node;
	 * Attach a local memory as VESHM used for source or destination
	 * address of DMA transfer. Then send or receive data by DMA
	 * transfer.
	 *
	 * If the attached VESHM exists on the same VE node;
	 * Attached VESHM is mapped to VEMVA. The memcpy() can be used
	 * for data transfer.
	 ************************************************************/

	ret = ve_shared_mem_detach((void *)attch_addr, mode_flag_detach);
	printf("[%d] VESHM detach: ret = %d, errno=%d\n", pid, ret, errno);

	return 0;
}
