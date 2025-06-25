// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>   // isnan
// system
#include <time.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
// C++
#include <string>
#include <vector>
//this are teh predefined headers
#include "common.h"
#include "avl.h"
#include "hashtable.h"
#include "zset.h"
#include "list.h"
#include "heap.h"
#include "threads.h"

static void msg(const char *s){
    fprintf(stderr," %s \n",s);
}

static void msg_errno(const char *msg){
    fprintf(stderr,"{errno - %d} %s\n",errno,msg);
}

static void die(const char *msg){
    fprintf(stderr,"{errno %d} %s",errno,msg);
    abort();
}

static uint64_t get_monotonic_msec(){
    struct timespec tv = {0,0};
    clock_gettime(CLOCK_MONOTONIC,&tv);
    return uint64_t(tv.tv_sec) *1000 + tv.tv_nsec/1000 /1000;
}

//sets the connection file discriptor to the non blocking mode

static void fd_set_nb(int fd){
    errno =0 ;
    int flags = fcntl(fd,F_GETFL,0);
    if(errno){
        die("fcntl error");
        return ;
    }
    
    flags |= O_NONBLOCK;

    errno = 0;
    (void) fcntl(fd,F_SETFL,flags);
    if(errno){
        die("fcntl() error");
        return ;
    }
}

const size_t k_max_msg = 32<<20; //this is the buffer and it is likely larger than the kernel buffer

using Buffer = std::vector<uint8_t>;
//append to the back 
static void buf_append(Buffer &buf,const uint8_t *data,size_t len){
    buf.insert(buf.end(),data,data+len);
}

//remove the data from the front so that it follows FIFO order
static void buf_consume(Buffer &buf,size_t len){
    buf.erase(buf.begin(),buf.begin()+len);
}

// a structure conection which consists of all teh members required for making the connection
struct Conn{
    int fd = -1;
    //applicaiton intention and the data required for the poll
    bool want_close = false;
    bool want_read = false;
    bool want_write= false;
    //the buffers for teh incoming and the outgoing data
    Buffer incoming;
    Buffer outgoing;
    //the data members for the timers 
    uint64_t last_active_ms = 0;
    DList idle_node;
};

//global data bases 
static struct {
    HMap db;
    //a map of all client conection keyed by the fd
    std::vector<Conn *> fd2conn;
    //timers for the idle connections
    DList idle_list;
    //timers for the TTLs
    std::vector<HeapItem > heap ;
    //the thread pool
    ThreadPool thread_pool;
}g_data;

//the function for the application call back when the socket is ready
static int32_t handle_accept(int fd){
    //accept the conection 
    struct sockaddr_in client_addr = {};
    socklen_t addr_len = sizeof(client_addr);
    int connfd  =accept(fd,(struct sockaddr *)&client_addr,&addr_len);
    if(connfd<0){
        //error handling of the accept
        msg_errno("accept() error");
        return -1;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        htons(client_addr.sin_port)
    );

    //set the new connection fd to the non blocking mode 
    fd_set_nb(connfd);

    //craete a struct Con
    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g_data.idle_list,&conn->idle_node);

    //put that connection structure into the map
    if(g_data.fd2conn.size() <= (size_t)conn->fd){
        g_data.fd2conn.resize(conn->fd+1);
    }
    assert(!g_data.fd2conn[conn->fd]);
    g_data.fd2conn[conn->fd] = conn;
    return 0;
}


static void conn_destroy(Conn *conn){
    (void)close(conn->fd);
    g_data.fd2conn[conn->fd] = NULL;
    dlist_detach(&conn->idle_node);
    delete conn;
}

const size_t k_max_args = 200*1000;

static bool read_u32(const uint8_t *&cur,const uint8_t *end,uint32_t &out){
    if(cur+4 > end) return false;
    memcpy(&out,cur,4);
    cur+=4;
    return true;
}

static bool read_str(const uint8_t *&cur,const uint8_t *end,size_t n,std::string &out){
    if(cur+n > end) return false;
    
    out.assign(cur,cur+n);
    cur+=n;
    return true;
}

static int32_t parse_req(const uint8_t *data,size_t size,std::vector<std::string> &out){
    const uint8_t *end = data+size;
    uint32_t nstr = 0; //this keeps the track of number of string in the request 
    if(!read_u32(data,end,nstr)) return -1;

    if(nstr > k_max_args) return -1; //this is the safety limiy 

    while(out.size() < nstr){
        uint32_t len = 0 ;
        if(!read_u32(data,end,len))return -1;

        out.push_back(std::string());
        if(!read_str(data,end,len,out.back())) return -1;
    }
    if(data != end) return -1; //because that indicates that there is atrailing garbage 

    return 0;
}
// error code for TAG_ERR
enum {
    ERR_UNKNOWN = 1,    // unknown command
    ERR_TOO_BIG = 2,    // response too big
    ERR_BAD_TYP = 3,    // unexpected value type
    ERR_BAD_ARG = 4,    // bad arguments
};


//enum codes for the tag errors
enum{
    TAG_NIL,
    TAG_ERR,
    TAG_STR,
    TAG_INT,
    TAG_DBL,
    TAG_ARR,
};

//helper functions for the serialisation 
static void buf_append_u8(Buffer &out,uint8_t data){
    out.push_back(data);
}
static void buf_append_u32(Buffer &buf,uint32_t data){
    buf_append(buf,(const uint8_t *)&data,4);
}
static void buf_append_i64(Buffer &buf,int64_t data){
    buf_append(buf,(const uint8_t *)&data,8);
}
static void buf_append_dbl(Buffer &buf,double val){
    buf_append(buf,(const uint8_t *)&val,8);
}

//append the serialised data to the blocks of the biffer
static void out_nil(Buffer &out){
    buf_append_u8(out,TAG_NIL);
}

static void out_str(Buffer &buf,const char *str,size_t size){
    buf_append_u8(buf,TAG_STR);
    buf_append_u32(buf,(uint32_t)size);
    buf_append(buf,(const uint8_t *)str,size);
}

static void out_int(Buffer &buf,int64_t val){
    buf_append_u8(buf,TAG_INT);
    buf_append_i64(buf,val);
}
static void out_dbl(Buffer &buf,double val){
    buf_append_u8(buf,TAG_DBL);
    buf_append_dbl(buf,val);
}
static void out_err(Buffer &buf,uint32_t code,const std::string &msg){
    buf_append_u8(buf,TAG_ERR);
    buf_append_u32(buf,code);
    buf_append_u32(buf,(uint32_t )msg.size());
    buf_append(buf,(const uint8_t *)msg.data(),msg.size());
}
static void out_arr(Buffer &buf,uint32_t n){
    buf_append_u8(buf,TAG_ARR);
    buf_append_u32(buf,n);
}

static size_t out_begin_arr(Buffer &out){
    out.push_back(TAG_ARR);
    buf_append_u32(out,0);
    return out.size()-4;    //this si the ctx argument of the next function
}
static void out_end_arr(Buffer &out,size_t ctx,uint32_t n){
    assert(out[ctx-1] == TAG_ARR);
    memcpy(&out[ctx],&n,4);
}

//the enum for the value types
enum {
    T_INIT = 0,
    T_STR = 1,  //this is the type for the string 
    T_ZSET = 2, //this is the type for the zset
};

//KV PAIR for the top level hashtable
struct Entry{
    struct HNode node; //this is the hahs table node
    std::string key;
    //for the TTL (TIME TO LIVE)
    size_t heap_idx  =-1; //this is the reference to the cooresponding heap index
    //value 
    uint32_t type = 0;
    //one of the following 
    std::string str;
    ZSet zset;     
};

static Entry *entry_new(uint32_t type){
    Entry *ent = new Entry();
    ent->type  = type;
    return ent;
}


static void entry_set_ttl(Entry *ent,int64_t ttl_ms);

static void entry_del_sync(Entry *ent){
    if(ent->type == T_ZSET) zset_clear(&ent->zset);
    delete ent;
}
static void entry_del_func(void *args){
    entry_del_sync((Entry *)args);
}

static void entry_del(Entry *ent){
    //unlink it from any other data structures before removifn it 
    entry_set_ttl(ent,-1); //it removes the ttl and unlink it from the heap
    //now run the destructor in a threadpool for large data structures deleting 
    size_t set_size = (ent->type==T_ZSET ) ? hm_size(&ent->zset.hmap) : 0;
    const size_t k_large_container_size = 1000;
    if(set_size > k_large_container_size) thread_pool_queue(&g_data.thread_pool,&entry_del_func,ent);
    else entry_del_sync(ent); //this willl avoidthe context switches
}
//for the easiest way of looking for the key in the db
struct LookupKey{
    struct HNode node; //this is the hash table node
    std::string key;
};

//equality comparison for the top level hash table 
static bool entry_eq(HNode *node,HNode *key){
    struct Entry *ent = container_of(node,struct Entry,node);
    struct LookupKey *keydata = container_of(key,struct LookupKey,node);
    return ent->key == keydata->key;
}

//now processing the logci for the execution of the commands
static void do_get(std::vector<std::string> &cmd,Buffer &out){
    //the usage of the dummy structure for the look up 
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());
    //now looking for the node in the hash table 
    HNode *node = hm_lookup(&g_data.db,&key.node,&entry_eq);
    if(!node) return out_nil(out);
    //if the key is there then copy its bvalue 
    Entry *ent = container_of(node,Entry,node);
    if(ent->type!=T_STR){
        return out_err(out,ERR_BAD_TYP,"Not a string value");
    }
    return out_str(out,ent->str.data(),ent->str.size());
}
static void do_set(std::vector<std::string>&cmd,Buffer &out){
    //a dummy structure for the  lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());
    //look for the key in the hash table 
    HNode *node  = hm_lookup(&g_data.db,&key.node,&entry_eq);
    if(node){
        //if the key sis foud then update the key value   
        Entry *ent = container_of(node,Entry,node);
        if(ent->type != T_STR) return out_err(out,ERR_BAD_TYP,"a non string value exists");
        ent->str.swap(cmd[2]); //if it is a string value then swap it 

    }else{
        //if ot foudn then create and allocate space for it 
        Entry *ent = entry_new(T_STR);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->str.swap(cmd[2]);
        hm_insert(&g_data.db,&ent->node);
    }
    return out_nil(out);
}

static void do_del(std::vector<std::string> &cmd,Buffer &out){
    //a dummy structure for the lookup 
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());
    //delet it from hash table 
    HNode *node = hm_delete(&g_data.db,&key.node,&entry_eq);
    if(node) entry_del(container_of(node,Entry,node)); //deallocate the pair 
    return out_int(out,node ? 1: 0);
}
static void heap_delete(std::vector<HeapItem> &a,size_t pos){
    //swap the erased item with the last item 
    a[pos] = a.back();
    a.pop_back();
    //update the swapped item
    if(pos<a.size()) heap_update(a.data(),pos,a.size());
}

static void heap_upsert(std::vector<HeapItem> &a,size_t pos,HeapItem t){
    if(pos<a.size()) a[pos] = t; //update the existing key
    else{
        pos = a.size();
        a.push_back(t); //or add a new item
    }
    heap_update(a.data(),pos,a.size());
}
//set or remove the TTL 
static void entry_set_ttl(Entry *ent,int64_t ttl_ms){
    if(ttl_ms<0 && ent->heap_idx != (size_t)-1){
        //here setting the negative ttl means removing the ttl
        heap_delete(g_data.heap,ent->heap_idx);
    }else if(ttl_ms>=0){
        //then add or update the heap structure 
        uint64_t expire_at = get_monotonic_msec() + (uint64_t)ttl_ms;
        HeapItem item = {expire_at,&ent->heap_idx};
        heap_upsert(g_data.heap,ent->heap_idx,item);
    }
}

static bool str2int(const std::string &s,int64_t &val){
    char *endP = NULL;
    val = strtoll(s.c_str(),&endP,10);
    return endP == s.c_str()+s.size();
}
//PEXPIRE key ttl_ms
static void do_expire(std::vector<std::string> &cmd,Buffer &out){
    int64_t ttl_ms = 0;
    if(!str2int(cmd[2],ttl_ms)) return out_err(out,ERR_BAD_ARG,"expect int 64");

    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());

    HNode *node = hm_lookup(&g_data.db,&key.node,&entry_eq);
    if(node){
        Entry *ent = container_of(node,Entry,node);
        entry_set_ttl(ent,ttl_ms);
    }
    return out_int(out,node ? 1 : 0);
}

//PTTL KEY
static void do_ttl(std::vector<std::string> &cmd,Buffer &out){
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());

    HNode *node = hm_lookup(&g_data.db,&key.node,&entry_eq);
    if(!node){
        return out_int(out,-2); //not found
    }

    Entry *ent = container_of(node,Entry,node);
    if(ent->heap_idx == (size_t)-1) return out_int(out,-1) ;//null or no ttl exists

    uint64_t expire_at =g_data.heap[ent->heap_idx].val;
    uint64_t now_ms = get_monotonic_msec();
    return out_int(out,expire_at > now_ms ? (expire_at-now_ms) : 0);
}
static bool cb_keys(HNode *node,void *args){
    Buffer &out = *(Buffer *)args;
    const std::string &key = container_of(node,Entry,node)->key;
    out_str(out,key.data(),key.size());
    return true;
}

static void do_keys(std::vector<std::string> &,Buffer &out){
    out_arr(out,(uint32_t)hm_size((&g_data.db)));
    hm_foreach(&g_data.db,&cb_keys,(void *)&out);
}

static bool str2dbl(const std::string &s,double &out){
    char *endP = NULL;
    out = strtod(s.c_str(),&endP);
    return endP ==s.c_str()+s.size() && isnan(out);
}

//zadd zset score name
static void do_zadd(std::vector<std::string> &cmd,Buffer &out){
    double score =0;
    if(!str2dbl(cmd[2],score)) return out_err(out,ERR_BAD_ARG,"expected float value for the score ");
    //Look up for the key else create a new key
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());
    HNode *hnode = hm_lookup(&g_data.db,&key.node,&entry_eq);

    Entry *ent = NULL;
    if(!hnode){
        ent= entry_new(T_ZSET);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        hm_insert(&g_data.db,&ent->node);
    }else{
        ent = container_of(hnode,Entry,node);
        //check for the existin key nad then udpdate it 
        if(ent->type != T_ZSET){
            return out_err(out,ERR_BAD_TYP,"expect zset");
        }
    }
    //add or update the tuple 
    const std::string &name = cmd[3];
    bool added =  zset_insert(&ent->zset,name.data(),name.size(),score);
    return out_int(out,(int64_t)added);
}
static const ZSet k_empty_zset;

static ZSet *expect_zset(std::string &s){
    LookupKey key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());

    HNode *hnode = hm_lookup(&g_data.db,&key.node,&entry_eq);
    if(!hnode) return (ZSet *)&k_empty_zset; //always a nin empty key is  treated as a non empty zset
    Entry *ent  = container_of(hnode,Entry,node);
    return ent->type == T_ZSET ? &ent->zset :  NULL;
}

//zrem zset name
static void do_zrem(std::vector<std::string> &cmd,Buffer &out){
    ZSet *zset = expect_zset(cmd[1]);
    if(!zset) return out_err(out,ERR_BAD_TYP,"expect zset");

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(zset,name.data(),name.size());
    if(znode) zset_delete(zset,znode);
    return out_int(out,znode ? 1: 0);
}

//zscore zset name
static void do_zscore(std::vector<std::string> &cmd,Buffer &out){
    ZSet *zset = expect_zset(cmd[1]);
    if(!zset) return out_err(out,ERR_BAD_TYP,"Expect zset");

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(zset,name.data(),name.size());
    return znode ? out_dbl(out,znode->score) : out_nil(out);
}

//zquery zset zscore name offset limit
static void do_zquery(std::vector<std::string> &cmd,Buffer &out){
    //parsing the arguments 
    double score = 0;
    if(!str2dbl(cmd[2],score)) return  out_err(out,ERR_BAD_ARG,"Expected float number ");

    const std::string &name = cmd[3];
    int64_t offset = 0,limit = 0;
    if(!str2int(cmd[4],offset)|| !str2int(cmd[5],limit)) return out_err(out,ERR_BAD_ARG,"expect int");

    //acquiring the corresponding zset
    ZSet *zset = expect_zset(cmd[1]);
    if(!zset) return out_err(out,ERR_BAD_TYP,"expect zset");

    //now seek to the key;
    if(limit <=0) return out_arr(out,0);

    ZNode *znode = zset_seekge(zset,score,name.data(),name.size());
    znode = znode_offset(znode,offset);

    //output 
    size_t ctx = out_begin_arr(out);
    int64_t n =0;
    while(znode && n<limit){
        out_str(out,znode->name,znode->len);
        out_dbl(out,znode->score);
        znode = znode_offset(znode,+1);
        n+=2;
    }
    out_end_arr(out,ctx,(uint32_t)n);
}
static void do_request(std::vector<std::string> &cmd,Buffer &out){
     if (cmd.size() == 2 && cmd[0] == "get") {
        return do_get(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "set") {
        return do_set(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "del") {
        return do_del(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "pexpire") {
        return do_expire(cmd, out);
    } else if (cmd.size() == 2 && cmd[0] == "pttl") {
        return do_ttl(cmd, out);
    } else if (cmd.size() == 1 && cmd[0] == "keys") {
        return do_keys(cmd, out);
    } else if (cmd.size() == 4 && cmd[0] == "zadd") {
        return do_zadd(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zrem") {
        return do_zrem(cmd, out);
    } else if (cmd.size() == 3 && cmd[0] == "zscore") {
        return do_zscore(cmd, out);
    } else if (cmd.size() == 6 && cmd[0] == "zquery") {
        return do_zquery(cmd, out);
    } else {
        return out_err(out, ERR_UNKNOWN, "unknown command.");
    }
}
static void response_begin(Buffer &out,size_t *header){
    *header = out.size() ; //message header postion
    buf_append_u32(out,0); //reserving the space for the header length
}

static size_t response_size(Buffer &out,size_t header){
    return out.size()-header-4;
}
static void response_end(Buffer &out,size_t header){
    size_t msg_size = response_size(out, header);
    if(msg_size>k_max_msg){
        out.resize(header+4);
        out_err(out,ERR_TOO_BIG,"response too big");
        msg_size = response_size(out,header);
    }
    //message header [position]
    uint32_t len = (uint32_t)msg_size;
    memcpy(&out[header],&len,4);
}

//process one request one at a time 
static bool try_one_request(Conn *conn){
    //try to parese the request by following the protocol
    if(conn->incoming.size()<4) return false;   //this means the size of incoming buffer is less than 1 byte 

    uint32_t len = 0;
    memcpy(&len,conn->incoming.data(),4); 
    if(len>k_max_msg){
        msg("too long ");
        conn->want_close = true;
        return false;
    }
    //message body 
    if(4+len > conn->incoming.size()) return false;

    const uint8_t *request = &conn->incoming[4];
     //assumption of got one requet now doing the applicaitom logic 
    std::vector<std::string> cmd;
    if(parse_req(request,len,cmd) <0){
        msg("bad request ");
        conn->want_close = true;
        return false;
    }

    size_t header_pos = 0;
    response_begin(conn->outgoing,&header_pos);
    do_request(cmd,conn->outgoing);
    response_end(conn->outgoing,header_pos);

    //the logic is done now removinng the request from teh buffer
    buf_consume(conn->incoming,4+len);

    return true;
}

//now the call back of the application when the soket is writable 
static void handle_write(Conn *conn){
    assert(conn->outgoing.size() >0);
    ssize_t rv = write(conn->fd,&conn->outgoing[0],conn->outgoing.size());

    if(rv<0 && errno == EAGAIN){
        return;
    }

    if(rv <0){
        msg_errno("write() error");
        conn->want_close =true;
        return ;

    }
    //remove the written buffer from the outgoing 
    buf_consume(conn->outgoing,(size_t)rv);

    //now update the readiness intention
    if(conn->outgoing.size() == 0){
        //all data is written 
        conn->want_read = true;
        conn->want_write = false;
    }
}

//the call back of the applicaiton when the soceket is readable 
static void handle_read(Conn *conn){
    //read some data 
    uint8_t rbuf[64*1024];
    ssize_t rv = read(conn->fd,rbuf,sizeof(rbuf));
    if(rv<0 && errno == EAGAIN) return ;

    if(rv  <0){
        msg_errno("read() error");
        conn->want_close = true;
        return ;    //want close 
    }
    //now handling teh end of the file 
    if(rv==0){
        if(conn->incoming.size() == 4){
            msg("client closed");
        }else{
            msg("Unexpected end of the file ");
        }
        conn->want_close = true;
        return ;
    }
    //noew append the data to teh buf
    buf_append(conn->incoming,rbuf,(size_t)rv);

    //now parse and generate the responses for the request 
    while(try_one_request(conn)){}

    //update the readiness intention
    if(conn->outgoing.size()>0){
        //has a response 
        conn->want_read = false;
        conn->want_write = true;
        //handle the write of the socket 
        return handle_write(conn);
    }
}


const  uint64_t k_idle_timeout_ms = 180*1000; //this keeps the  server alive for 3 minutes without removing the idle connections

static uint32_t next_timer_ms(){
    uint64_t now_ms = get_monotonic_msec();
    uint64_t next_ms = (uint64_t) -1;

    //idle timers using the linekd list 
    if(!dlist_empty(&g_data.idle_list)){
        Conn *conn = container_of(g_data.idle_list.next,Conn,idle_node);
        next_ms = conn->last_active_ms + k_idle_timeout_ms;
    }
    //TTL tinemrs using the heap
    if (!g_data.heap.empty() && g_data.heap[0].val < next_ms) {
        next_ms = g_data.heap[0].val;
    }
    //time out value 
    if(next_ms == (uint64_t)-1) return -1; //this means no timers nad n timeout s

    if(next_ms <= now_ms) return 0; //missed

    return (uint32_t)(next_ms - now_ms);
}

static bool hnode_same(HNode *node,HNode *key){
    return node==key;
}

static void process_timers(){
    uint64_t now_ms = get_monotonic_msec();
    //idle timers using the linked list
    while(!dlist_empty(&g_data.idle_list)){
        Conn *conn = container_of(g_data.idle_list.next,Conn,idle_node);
        uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
        if(next_ms >= now_ms) break; //not expired
        
        fprintf(stderr,"removing the idle connections: %d\n",conn->fd);
        conn_destroy(conn);
    }
    //TTL timers using a heap
    const size_t k_max_works = 2000;
    size_t nwork= 0;
    const std::vector<HeapItem> &heap = g_data.heap;
    while(!heap.empty() && heap[0].val<now_ms){
        Entry *ent= container_of(heap[0].ref,Entry,heap_idx);
        HNode *node = hm_delete(&g_data.db,&ent->node,&hnode_same);
        assert(node == &ent->node);
        
        entry_del(ent);
        if(nwork++ >=k_max_works) break;
        //dont stall the server if too many keys are expiring at once
    }
}

int main(){
    //initialissaiton
    dlist_init(&g_data.idle_list);
    thread_pool_init(&g_data.thread_pool,4);

    //listening socke t
    int fd = socket(AF_INET,SOCK_STREAM,0);
    if(fd<0) die("socket()");

    int val = 1;
    setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&val,sizeof(val));

    //now bindit
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr= htonl(0); //wild card ip address 0.0.0.0
    int rv = bind(fd,(const sockaddr *)&addr,sizeof(addr));
    if(rv) die("bind()");

    //set the fd to the non blockign mode 
    fd_set_nb(fd);
    //listen to the connections
    rv = listen(fd,SOMAXCONN);
    if(rv) die("listen()");

    //the event loop 
    std::vector<struct pollfd> poll_args;
    while(true){
        //preparae the arguments of the poll()
        poll_args.clear();
        //put the listening sockets int the first position 
        struct pollfd pfd = {fd,POLLIN,0};
        poll_args.push_back(pfd);
        //the rest are teh connecction sockets
        for(Conn *conn : g_data.fd2conn){
            if(!conn) continue;
            //always poll() for the erro 
            struct pollfd pfd = {conn->fd , POLLERR,0};
            //poll() flags from the application intent
            if(conn->want_read) pfd.events |= POLLIN;

            if(conn->want_write)  pfd.events |= POLLOUT;

            poll_args.push_back(pfd);
        }
        //wait for the readiness 
        int32_t timeout_ms = next_timer_ms();
        //mow the socket need not to wait for the infinite time for the connection rather thatn that wait for the timeout connection time nad then break the client or the server
        int rv = poll(poll_args.data(),(nfds_t)poll_args.size(),timeout_ms);
        if(rv<0 &&errno == EINTR)continue;
        if(rv<0) die("Poll()");

        //handle the listening sockets 
        if(poll_args[0].revents) handle_accept(fd);
        //now handling the conectio sockets 
        for(size_t i=1;i<poll_args.size();++i){
            uint32_t ready = poll_args[i].revents;
            if(ready==0) continue; //this means the socket is not ready so skip teh current itereation
            Conn *conn = g_data.fd2conn[poll_args[i].fd];

            //update the idle timers by moving the conn to the end of the list 
            conn->last_active_ms = get_monotonic_msec();
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g_data.idle_list,&conn->idle_node);
            

            //jhandling the io 
            if(ready & POLLIN){
                assert(conn->want_read);
                handle_read(conn);
            }

            if(ready & POLLOUT){
                assert(conn->want_write);
                handle_write(conn);
            }
            //close the socket if there is socket error or the application error 
            if((ready & POLLERR) ||conn->want_close) conn_destroy(conn);
        }
        //handle timers 
        process_timers();
    }
    return 0;
}
