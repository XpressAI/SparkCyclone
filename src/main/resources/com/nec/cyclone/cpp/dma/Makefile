NCC = $(shell ls /opt/nec/ve/bin/ncc 2>/dev/null || echo gcc)
NCC_OPTS =
ALL = sample-dma sample-owner sample-user
all: $(ALL)
.PHONY: clean

sample-dma: sample-dma.c
	$(NCC) $(NCC_OPTS) -o $@ $^ -lveio -lpthread

sample-owner: sample-owner.c
	$(NCC) $(NCC_OPTS) -o $@ $^ -lveio -lpthread

sample-user: sample-user.c
	$(NCC) $(NCC_OPTS) -o $@ $^ -lveio -lpthread

clean:
	rm -f sample-dma sample-owner sample-user
