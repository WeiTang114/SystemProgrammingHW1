#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define ERR_EXIT(a) { perror(a); exit(1); }
#define ACCT_PATH "./account_info"
#ifdef DEBUG
    #define DPRINTF(format, args...) printf("[%s:%d] "format, __FILE__, __LINE__, ##args)
#else
    #define DPRINTF(args...)
#endif
#define MAX_ACCT_ID 127

#define read_lock(fd, offset, whence, len) \
            lock_reg((fd), F_SETLK, F_RDLCK, (offset), (whence), (len))
#define write_lock(fd, offset, whence, len) \
            lock_reg((fd), F_SETLK, F_WRLCK, (offset), (whence), (len))
#define un_lock(fd, offset, whence, len) \
            lock_reg((fd), F_SETLK, F_UNLCK, (offset), (whence), (len))


typedef struct {
    char hostname[512];  // server's hostname
    unsigned short port;  // port to listen
    int listen_fd;  // fd to wait for a new connection
} server;

typedef struct {
    char host[512];  // client's host
    int conn_fd;  // fd to talk with client
    char buf[512];  // data sent by/to client
    size_t buf_len;  // bytes used by buf
    // you don't need to change this.
    int account;
    int wait_for_write;  // used by handle_read to know if the header is read or not.
    int fd_towrite;
} request;

typedef struct {
    int id;
    int money;
    int rw;  // when one deposits / withdraws, rw = -1; when one reads, rw ++
} Account;


server svr;  // server
request* requestP = NULL;  // point to a list of requests
int maxfd;  // size of open file descriptor table, size of request list
Account* accountP = NULL;


const char* accept_read_header = "ACCEPT_FROM_READ";
const char* accept_write_header = "ACCEPT_FROM_WRITE";
const char* reject_header = "REJECT\n";

// Forwards

static int handle_svrsock();
static int handle_clientsock(int conn_fd);
static long get_account_offset(int acct_id, FILE* acct_fp, int* acct_buf);
static int lock_account(int acct_id, int type, int acct_fd);
static int unlock_account(int acct_id, int type, int acct_fd);
static int read_account(int acct_id);
static int write_account(int acct_id, int dvalue, int fd_towrite);
static int acquire_write_acct(int acct_id);
int lock_reg(int fd, int cmd, int type, off_t offset, int whence, off_t len);

static void init_server(unsigned short port);
// initailize a server, exit for error

static void init_request(request* reqP);
// initailize a request instance

static void free_request(request* reqP);
// free resources used by a request instance

static int handle_read(request* reqP);
// return 0: socket ended, request done.
// return 1: success, message (without header) got this time is in reqP->buf with reqP->buf_len bytes. read more until got <= 0.
// It's guaranteed that the header would be correctly set after the first read.
// error code:
// -1: client connection error

int main(int argc, char** argv) {
    int i, ret;


    int conn_fd;  // fd for a new connection with client
    int file_fd;  // fd for file that we open for reading
    int buf_len;

    // Parse args.
    if (argc != 2) {
        fprintf(stderr, "usage: %s [port]\n", argv[0]);
        exit(1);
    }

    // Initialize server
    init_server((unsigned short) atoi(argv[1]));


    accountP = (Account*) calloc(MAX_ACCT_ID + 1, sizeof(Account));

    // Get file descripter table size and initize request table
    maxfd = getdtablesize();
    requestP = (request*) malloc(sizeof(request) * maxfd);
    if (requestP == NULL) {
        ERR_EXIT("out of memory allocating all requests");
    }
    for (i = 0; i < maxfd; i++) {
        init_request(&requestP[i]);
    }
    requestP[svr.listen_fd].conn_fd = svr.listen_fd;
    strcpy(requestP[svr.listen_fd].host, svr.hostname);

    // Loop for handling connections
    fprintf(stderr, "\nstarting on %.80s, port %d, fd %d, maxconn %d...\n", svr.hostname, svr.port, svr.listen_fd, maxfd);


    fd_set master_set;
    fd_set read_set;

    struct timeval timeout_interval;
    int read_max_fd = 0;


    FD_ZERO(&master_set);
    FD_SET(svr.listen_fd, &master_set);   
    read_max_fd = svr.listen_fd;

    while (1) {
        // TODO: Add IO multiplexing
        timeout_interval.tv_sec = 2;
        timeout_interval.tv_usec = 500000;
        //DPRINTF("read_max_fd=%d\n", read_max_fd);

        // select modifies read_set as the argument, so we need a clean one, master.
        read_set = master_set;

        ret = select(read_max_fd + 1, &read_set, NULL, NULL, &timeout_interval);// NULL);ss
        if (-1 == ret) {
            perror ("select");
            continue;   
        }
        else if (0 == ret) { //timeout
            continue;
        }

        DPRINTF("something selected, num = %d\n", ret);
        int res;
        int j;
        for (j = 0; j < 8 /*FD_SETSIZE*/; j++) {
            if (FD_ISSET(j, &read_set)) {
                if (j == svr.listen_fd) {
                    res = handle_svrsock();
                    if (res < 0) {
                        printf("handle_svrsock failed, return %d\n", res);
                    }
                    else { // new clients
                        FD_SET(res, &master_set);
                        read_max_fd = read_max_fd >= res ? read_max_fd : res;
                        DPRINTF("new client added to read_set: %d\n", res);
                    }
                    break;
                }
                else {
                    res = handle_clientsock(j);
                    if (res < 0) {
                        printf("handle_clientsock failed\n");
                    }
                    else if (0 == res) {
                        DPRINTF("closing client %d\n", j);
                        FD_CLR(j, &master_set);
                        close(j);
                        free_request(&requestP[j]);
                    }
                    else if (1 == res) {
                        DPRINTF("Wait for client..%d\n", j);
                    }
                }
            }
        }

    }
    free(requestP);
    return 0;
}




/**
 * server socket selected, must accept
 * @return positive number: fd accepted
 */
static int handle_svrsock()
{
    //TODO
 
    int clilen = 0;
    struct sockaddr_in cliaddr;  // used by accept()
    int conn_fd = 0;
    // Check new connection
    clilen = sizeof(cliaddr);
    conn_fd = accept(svr.listen_fd, (struct sockaddr*)&cliaddr, (socklen_t*)&clilen);
    if (conn_fd < 0) {
        if (errno == EINTR || errno == EAGAIN) return -1;  // try again
        if (errno == ENFILE) {
            (void) fprintf(stderr, "out of file descriptor table ... (maxconn %d)\n", maxfd);
            return -2;
        }
        ERR_EXIT("accept")
    }
    requestP[conn_fd].conn_fd = conn_fd;
    strcpy(requestP[conn_fd].host, inet_ntoa(cliaddr.sin_addr));
    fprintf(stderr, "getting a new request... fd %d from %s\n", conn_fd, requestP[conn_fd].host);

    return conn_fd;
}

static int handle_clientsock(int conn_fd)
{
    printf("handle_clientsock called, conn_fd = %d\n", conn_fd);

    //TODO
    int ret = 0;
    char buf[512];
    int retval = 0;
    
    ret = handle_read(&requestP[conn_fd]); // parse data from client to requestP[conn_fd].buf
    if (ret < 0) {
        fprintf(stderr, "bad request from %s\n", requestP[conn_fd].host);
        return -1;
    }
    DPRINTF("ret(handle_read) = %d\n", ret);
    int money;
    int input = atoi(requestP[conn_fd].buf);
#ifdef READ_SERVER
    //sprintf(buf,"%s : %s\n",accept_read_header,requestP[conn_fd].buf);
    money = read_account(input);
    if (money == -1) 
        sprintf(buf, "This account is occupied.\n");
    else
        sprintf(buf, "Balance: %d\n", money);
    write(requestP[conn_fd].conn_fd, buf, strlen(buf));
    retval = 0;
#else
    if (0 == requestP[conn_fd].wait_for_write) {
        int acquired_fd;
        if ((acquired_fd = acquire_write_acct(input)) > 0) {
            requestP[conn_fd].wait_for_write = 1;
            requestP[conn_fd].account = input;
            requestP[conn_fd].fd_towrite = acquired_fd;
            sprintf(buf, "This account is available.\n");
            DPRINTF("acquired account %d by %d, success\n", input, conn_fd);
            retval = 1;
        }
        else if (-1 == acquired_fd) {
            sprintf(buf, "This account is occupied.\n");
            DPRINTF("acquired account %d by %d, occupied\n", input, conn_fd);
            retval = 0;
        }
    }
    else {
        money = write_account(requestP[conn_fd].account, input, requestP[conn_fd].fd_towrite);
        if (-2 == money) {
            sprintf(buf, "Operation fail.\n");
            DPRINTF("Operation failed.\n");
        }
        else if (money >= 0) {
            buf[0] = '\0';
            DPRINTF("Write finish.\n");
        }
        else {
            sprintf(buf, "Unexpected error: money = %d\n", money);
            DPRINTF("Unexpected error: money = %d\n", money);
        }
        retval = 0;
    }
    write(requestP[conn_fd].conn_fd, buf, strlen(buf));
#endif

    return retval;
}


/**
 * [read_account description]
 * @param  acct_id [description]
 * @return         >=0: value  -1: occupied -2: other error
 */
static int read_account(int acct_id)
{
    int money = 0;
    int acct_fd = -1;
    FILE *fp;
    int acct_offset = -1;
    int acct_buf[2] = {0, 0};


    if ((acct_fd = open(ACCT_PATH, O_RDONLY)) == -1) {
        perror("Open account file for reading");
        return -2;
    }

    if (lock_account(acct_id, 0, acct_fd) != 0) {
        // failed
        DPRINTF("read_account: %d failed: occupied\n", acct_id);
        return -1;
    }

    fp = fdopen(acct_fd, "r");

    acct_offset = get_account_offset(acct_id, fp, acct_buf);
    if (acct_offset < 0) {
        printf("Error: acct_idx:%d not found\n", acct_offset);
        fclose(fp);
        return -2;
    }
   
    money = acct_buf[1];
    DPRINTF("money = %d\n", money);

    unlock_account(acct_id, 0, acct_fd);
    fclose(fp);

    return money;
}

static int acquire_write_acct(int acct_id)
{
    int acct_fd = -1;

    if ((acct_fd = open(ACCT_PATH, O_RDWR)) == -1) {
        perror("Open account file for reading");
        return -2;
    }

    if (lock_account(acct_id, 1, acct_fd) != 0) {
        // failed
        DPRINTF("acquire_write_account: %d failed: occupied\n", acct_id);
        return -1;
    }

    return acct_fd;
}


/**
 * [write_account description]
 * @param  acct_id [description]
 * @param  dvalue  [description]
 * @return         -1: occupied -2: op failed  -3:other fail
 */
static int write_account(int acct_id, int dvalue, int fd_towrite)
{
    int money = 0;
    int acct_fd = fd_towrite;
    FILE *fp;
    int acct_offset = -1;
    int acct_buf[2] = {0, 0};

    fp = fdopen(acct_fd, "r+");

    acct_offset = get_account_offset(acct_id, fp, acct_buf);
    if (acct_offset < 0) {
        printf("Error: acct_idx:%d not found\n", acct_offset);
        fclose(fp);
        return -3;
    }
   
    money = acct_buf[1];
    DPRINTF("money = %d\n", money);

    if (money + dvalue < 0) {
        DPRINTF("money(%d) + dvalue(%d) < 0\n", money, dvalue);
        unlock_account(acct_id, 1, acct_fd);
        fclose(fp);
        return -2;
    }

    money += dvalue;
    fseek(fp, acct_offset + 4, SEEK_SET);
    fwrite(&money, sizeof(int), 1, fp);
    DPRINTF("Write to account %d : %d  (acct_offset = %d)\n", acct_id, money, acct_offset);

    unlock_account(acct_id, 1, acct_fd);
    fclose(fp);
    return money;
}


/**
 * [lock_account description]
 * @param  acct_id [description]
 * @param  type    0:read 1:write
 * @return         0: success  -1: fail
 */
static int lock_account(int acct_id, int type, int acct_fd)
{
    int lockres = 0;
    int rwtmp = accountP[acct_id].rw;
    int acct_fd_dup = dup(acct_fd);
    FILE* fp = fdopen(acct_fd_dup, "r");
    int acct_offset = get_account_offset(acct_id, fp, NULL);
    fclose(fp);

    if (0 == type) {
        if (accountP[acct_id].rw >= 0) {
            lockres = read_lock(acct_fd, acct_offset, SEEK_SET, 8);
            accountP[acct_id].rw ++;
        }
        else {
            lockres = -1;    
        }
    }
    else if (1 == type) {
        if (accountP[acct_id].rw == 0) {
            lockres = write_lock(acct_fd, acct_offset, SEEK_SET, 8);
            accountP[acct_id].rw = -1;
            DPRINTF("write_lock on acct_id %d\n", acct_id);
        }
        else {
            lockres = -1;
        }
    }

    DPRINTF("lockres = %d\n", lockres);
    if (lockres != 0) {
        accountP[acct_id].rw = rwtmp;
        return -1;
    }
    return 0;
}

/**
 * [unlock_account description]
 * @param  acct_id [description]
 * @param  type    0:read  1:write
 * @return         0: success  -1:fail
 */
static int unlock_account(int acct_id, int type, int acct_fd)
{
    int lockres = 0;
    int acct_fd_dup = dup(acct_fd);
    FILE* fp = fdopen(acct_fd_dup, "r");
    int acct_offset = get_account_offset(acct_id, fp, NULL);
    fclose(fp);
    DPRINTF("acct_offset = %d\n", acct_offset);
    lockres = un_lock(acct_fd, acct_offset, SEEK_SET, 8);
    if (0 == type) {
        accountP[acct_id].rw --;
    }
    else if (1 == type) {
        accountP[acct_id].rw = 0;
    }

    if (lockres != 0)
        return -1;
    return 0;
}


static long get_account_offset(int acct_id, FILE* acct_fp, int* acct_buf)
{
    char *p = 0;
    int found = 0;
    long offset = 0;
    int buf[2] = {0,0};

    if (acct_id <= 0) 
        return -1;

    fseek(acct_fp, 0, SEEK_SET);
    while (fread(buf, sizeof(int), 2, acct_fp) != 0) {
        if (acct_id == buf[0]) {
            found = 1;
            if (acct_buf) {
                memcpy(acct_buf, buf, 2 * sizeof(int));
            }
            break;
        }
        offset += 8;
    }
    fseek(acct_fp, 0, SEEK_SET);
    if (!found){
        return -1;
    }
    return offset;
}

int lock_reg(int fd, int cmd, int type, off_t offset, int whence, off_t len)
{
    struct flock lock;
    lock.l_type = type;
    lock.l_start = offset;
    lock.l_whence = whence;
    lock.l_len = len;
    return (fcntl(fd, cmd, &lock));
}



// ======================================================================================================
// You don't need to know how the following codes are working
#include <fcntl.h>

static void* e_malloc(size_t size);


static void init_request(request* reqP) {
    reqP->conn_fd = -1;
    reqP->buf_len = 0;
    reqP->account = 0;
    reqP->wait_for_write = 0;
}

static void free_request(request* reqP) {
    /*if (reqP->filename != NULL) {
        free(reqP->filename);
        reqP->filename = NULL;
    }*/
    init_request(reqP);
}

// return 0: socket ended, request done.
// return 1: success, message (without header) got this time is in reqP->buf with reqP->buf_len bytes. read more until got <= 0.
// It's guaranteed that the header would be correctly set after the first read.
// error code:
// -1: client connection error
static int handle_read(request* reqP) {
    int r;
    char buf[512];

    // Read in request from client
    r = read(reqP->conn_fd, buf, sizeof(buf));
    if (r < 0) return -1;
    if (r == 0) return 0;
    char* p1 = strstr(buf, "\015\012");
    int newline_len = 2;
    // be careful that in Windows, line ends with \015\012
    if (p1 == NULL) {
        p1 = strstr(buf, "\012");
        newline_len = 1;
        if (p1 == NULL) {
            ERR_EXIT("this really should not happen...");
        }
    }
    size_t len = p1 - buf + 1;
    memmove(reqP->buf, buf, len);
    reqP->buf[len - 1] = '\0';
    reqP->buf_len = len-1;
    return 1;
}

static void init_server(unsigned short port) {
    struct sockaddr_in servaddr;
    int tmp;

    gethostname(svr.hostname, sizeof(svr.hostname));
    svr.port = port;

    svr.listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (svr.listen_fd < 0) ERR_EXIT("socket");

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(port);
    tmp = 1;
    if (setsockopt(svr.listen_fd, SOL_SOCKET, SO_REUSEADDR, (void*)&tmp, sizeof(tmp)) < 0) {
        ERR_EXIT("setsockopt");
    }
    if (bind(svr.listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
        ERR_EXIT("bind");
    }
    if (listen(svr.listen_fd, 1024) < 0) {
        ERR_EXIT("listen");
    }
}

static void* e_malloc(size_t size) {
    void* ptr;

    ptr = malloc(size);
    if (ptr == NULL) ERR_EXIT("out of memory");
    return ptr;
}

