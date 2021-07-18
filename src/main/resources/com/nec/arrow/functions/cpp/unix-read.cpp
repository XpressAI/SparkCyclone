#undef UNICODE

#include <iostream>
#include <sys/socket.h>
#include <sys/un.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include "transfer-definitions.h"
#define sock_name "xsx"
#define client_sock_name "client_sock"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>


/** Based on input socket name specified in the first vector, connect to that socket, fetch the data,
then put it into output_data

The protocol is simple:
< [ size (int) ] [data of size ] | int 0 to signify end of stream >

**/
extern "C" long read_fully_2(non_null_c_bounded_string* input_sock_name, non_null_varchar_vector* output_data)
{
    output_data->data = (char*) malloc(0);
    output_data->offsets = (int*) malloc(4 * 2);
    output_data->offsets[0] = 0;
    output_data->count = 1;


    int clientFd = socket(AF_UNIX, SOCK_STREAM, 0);

    size_t BytesRecvd = 0;
    struct sockaddr_un serverAddr = { 0 };
    socklen_t addrLen = 0;

    serverAddr.sun_family = AF_UNIX;
    strncpy(serverAddr.sun_path, input_sock_name->data, input_sock_name->length);
    std::cout << "A, will read file" << serverAddr.sun_path << "\n" << std::flush;
	if( connect(clientFd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == -1 )
	{
		printf("Client: Error on connect call \n");
		return 1;
	}

    int sizeAvailable = 1;
    size_t size;
    FILE *stream;
    char* bp;
    stream = open_memstream (&bp, &size);
    char c;
    while (sizeAvailable > 0 && recv(clientFd, &sizeAvailable, sizeof(sizeAvailable), 0) != -1) {
        std::cout << "Received " << sizeAvailable << std::flush;
        if ( sizeAvailable != 0 ) {
            for ( int i = 0; i < sizeAvailable; i++ ) {
                // I'm aware this is not ideal :-F
                // Just need to get it working and hand off to the C++ gods to optimize
                recv(clientFd, &c, 1, 0);
                fwrite(&c, 1, 1, stream);
            }
        }
    }
    close(clientFd);
    fflush(stream);
    output_data->data = bp ;
    output_data->offsets[1] = size;
    output_data->size = size;
    fclose(stream);
    return 0;
}

extern "C" long read_fully(non_null_varchar_vector* input_sock_name, non_null_varchar_vector* output_data)
{
output_data->data = (char*) malloc(0);
output_data->offsets = (int*) malloc(4 * 2);
int i = 0;
for (i = 0; i < input_sock_name->size; i++ ) {
output_data->data = (char*)realloc(output_data->data, (i + 1));
output_data->data[i] = input_sock_name->data[i];
}
std::cout << i << "\n" << std::flush;
std::cout << input_sock_name->size << "\n" << std::flush;
output_data->offsets[0] = 0;
output_data->offsets[1] = i;
output_data->size = i;
output_data->count = 1;
return 0;
}
