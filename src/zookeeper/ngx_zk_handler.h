#ifndef NGX_UPSYNC_ZK_HANDLER
#define NGX_UPSYNC_ZK_HANDLER

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>

#include "zookeeper.jute.h"
#include "proto.h"
#include "recordio.h"

#define COMPLETION_WATCH -1
#define COMPLETION_VOID 0
#define COMPLETION_STAT 1
#define COMPLETION_DATA 2
#define COMPLETION_STRINGLIST 3
#define COMPLETION_STRINGLIST_STAT 4
#define COMPLETION_ACLLIST 5
#define COMPLETION_STRING 6
#define COMPLETION_MULTI 7

/* predefined xid's values recognized as special by the server */
#define WATCHER_EVENT_XID -1
#define PING_XID -2
#define AUTH_XID -4
#define SET_WATCHES_XID -8

/* zookeeper state constants */
#define EXPIRED_SESSION_STATE_DEF -112
#define AUTH_FAILED_STATE_DEF -113
#define CONNECTING_STATE_DEF 1
#define ASSOCIATING_STATE_DEF 2
#define CONNECTED_STATE_DEF 3
#define NOTCONNECTED_STATE_DEF 999

/* zookeeper event type constants */
#define CREATED_EVENT_DEF 1
#define DELETED_EVENT_DEF 2
#define CHANGED_EVENT_DEF 3
#define CHILD_EVENT_DEF 4
#define SESSION_EVENT_DEF -1
#define NOTWATCHING_EVENT_DEF -2

#define HANDSHAKE_REQ_SIZE 44
#define HANDSHAKE_RPLY_SIZE 40 /*added by zj*/

/* connect request */
struct connect_req {
	int32_t protocolVersion;
	int64_t lastZxidSeen;
	int32_t timeOut;
	int64_t sessionId;
	int32_t passwd_len;
	char passwd[16];
};

/* the connect response */
struct prime_struct {
	int32_t len;
	int32_t protocolVersion;
	int32_t timeOut; 
	int64_t sessionId;
	int32_t passwd_len;
	char passwd[16];
};

const int ZOO_EPHEMERAL = 1 << 0;
const int ZOO_SEQUENCE = 1 << 1;

const int ZOOKEEPER_WRITE = 1 << 0;
const int ZOOKEEPER_READ = 1 << 1;

const int ZOO_EXPIRED_SESSION_STATE = EXPIRED_SESSION_STATE_DEF;
const int ZOO_AUTH_FAILED_STATE = AUTH_FAILED_STATE_DEF;
const int ZOO_CONNECTING_STATE = CONNECTING_STATE_DEF;
const int ZOO_ASSOCIATING_STATE = ASSOCIATING_STATE_DEF;
const int ZOO_CONNECTED_STATE = CONNECTED_STATE_DEF;
const int ZOO_GETTING_DATA_STATE = 100; /* added by zj */
const int ZOO_CLOSE_CONNECTED_STATE = 101; /* added by zj */

static volatile int zoo_conn_state = CONNECTING_STATE_DEF;

int32_t get_xid()
{
	static volatile int32_t xid = -1;
	if (xid == -1) {
		xid = time(0);
	}
	return xid++;
}

int isValidPath(const char* path, const int flags) {
	int len = 0;
	char lastc = '/';
	char c;
	int i = 0;

	if (path == 0)
		return 0;
	len = strlen(path);
	if (len == 0)
		return 0;
	if (path[0] != '/')
		return 0;
	if (len == 1) // done checking - it's the root
		return 1;
	if (path[len - 1] == '/' && !(flags & ZOO_SEQUENCE))
		return 0;

	i = 1;
	for (; i < len; lastc = path[i], i++) {
		c = path[i];

		if (c == 0) {
			return 0;
		} else if (c == '/' && lastc == '/') {
			return 0;
		} else if (c == '.' && lastc == '.') {
			if (path[i-2] == '/' && (((i + 1 == len) && !(flags & ZOO_SEQUENCE))
						|| path[i+1] == '/')) {
				return 0;
			}   
		} else if (c == '.') {
			if ((path[i-1] == '/') && (((i + 1 == len) && !(flags & ZOO_SEQUENCE))
						|| path[i+1] == '/')) {
				return 0;
			}   
		} else if (c > 0x00 && c < 0x1f) {
			return 0;
		}   
	}

	return 1;
}


static int serialize_prime_connect(struct connect_req *req, char* buffer){
	//this should be the order of serialization
	int offset = 0;
	req->protocolVersion = htonl(req->protocolVersion);
	memcpy(buffer + offset, &req->protocolVersion, sizeof(req->protocolVersion));
	offset = offset +  sizeof(req->protocolVersion);

	req->lastZxidSeen = zoo_htonll(req->lastZxidSeen);
	memcpy(buffer + offset, &req->lastZxidSeen, sizeof(req->lastZxidSeen));
	offset = offset +  sizeof(req->lastZxidSeen);

	req->timeOut = htonl(req->timeOut);
	memcpy(buffer + offset, &req->timeOut, sizeof(req->timeOut));
	offset = offset +  sizeof(req->timeOut);

	req->sessionId = zoo_htonll(req->sessionId);
	memcpy(buffer + offset, &req->sessionId, sizeof(req->sessionId));
	offset = offset +  sizeof(req->sessionId);

	req->passwd_len = htonl(req->passwd_len);
	memcpy(buffer + offset, &req->passwd_len, sizeof(req->passwd_len));
	offset = offset +  sizeof(req->passwd_len);

	memcpy(buffer + offset, req->passwd, sizeof(req->passwd));

	return 0;
}

static int deserialize_prime_response(struct prime_struct *req, char* buffer){
	//this should be the order of deserialization
	int offset = 0;
	memcpy(&req->len, buffer + offset, sizeof(req->len));
	offset = offset +  sizeof(req->len);

	req->len = ntohl(req->len);
	memcpy(&req->protocolVersion, buffer + offset, sizeof(req->protocolVersion));
	offset = offset +  sizeof(req->protocolVersion);

	req->protocolVersion = ntohl(req->protocolVersion);
	memcpy(&req->timeOut, buffer + offset, sizeof(req->timeOut));
	offset = offset +  sizeof(req->timeOut);

	req->timeOut = ntohl(req->timeOut);
	memcpy(&req->sessionId, buffer + offset, sizeof(req->sessionId));
	offset = offset +  sizeof(req->sessionId);

	req->sessionId = zoo_htonll(req->sessionId);
	memcpy(&req->passwd_len, buffer + offset, sizeof(req->passwd_len));
	offset = offset +  sizeof(req->passwd_len);

	req->passwd_len = ntohl(req->passwd_len);
	memcpy(req->passwd, buffer + offset, sizeof(req->passwd));
	return 0;
}

/*
static void
ngx_http_upsync_zk_send_handler(ngx_event_t *event)
{

	int len = 0;
	int nlen = 0;
	int size = 0;
	struct oarchive *oa = NULL;
	char request[ngx_pagesize] = [0];

	ngx_connection_t                         *c;
	ngx_upsync_conf_t                        *upsync_type_conf;
	ngx_http_upsync_ctx_t                    *ctx;
	ngx_http_upsync_server_t                 *upsync_server;
	ngx_http_upsync_srv_conf_t               *upscf;

	if (ngx_http_upsync_need_exit()) {
		return;
	}

	c = event->data;
	upsync_server = c->data;
	upscf = upsync_server->upscf;
	upsync_type_conf = upscf->upsync_type_conf;
	ctx = &upsync_server->ctx;
	ctx->send.pos = buffer_req;

	ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0, "upsync_send");

	if(zoo_conn_state == ZOO_CONNECTING_STATE){
		struct connect_req req;
		req.protocolVersion = 0;
		//每次建立新的连接,赋值sessionId等于0
		req.sessionId = 0;
		//置password为null
		req.passwd_len = sizeof(req.passwd);
		memset(req.passwd, 0, sizeof(req.passwd));
		//超时
		req.timeOut = 3000;
		req.lastZxidSeen = 0;

		len = HANDSHAKE_REQ_SIZE;
		serialize_prime_connect(&req, request+sizeof(nlen));
		//zoo_conn_state = ZOO_ASSOCIATING_STATE;

	} else if(zoo_conn_state == ZOO_CONNECTED_STATE) {
		//request临时用来存储upsync path
		memcpy(request, upscf->upsync_send.data, upscf->upsync_send.len);
		request[upscf->upsync_send.len] = '\0';
		if ( !isValidPath(request, 0) )
		{
			//goto upsync_send_fail;
		}
		//create_buffer_oarchive()创建缓存，用完buff后调用close_buffer_oarchive()释放缓存
		oa = create_buffer_oarchive();

		//初始化请求
		struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type , ZOO_GETCHILDREN_OP)};
		//watch设为0表示不设置watcher
		struct GetChildrenRequest req = { STRUCT_INITIALIZER  ( path, request ), STRUCT_INITIALIZER ( watch, 0 ) };

		//序列化请求
		rc = serialize_RequestHeader(oa, "header", &h);
		rc = rc < 0 ? rc : serialize_GetChildrenRequest(oa, "req", &req);
		if(rc < 0) {
			//goto upsync_send_fail;
		}
		len = get_buffer_len(oa)
		memcpy(request+sizeof(nlen), get_buffer(oa), len);
		close_buffer_oarchive(&oa, 1);

	} else if(zoo_conn_state == ZOO_CLOSE_CONNECTED_STATE) {
		//create_buffer_oarchive()创建缓存，用完buff后调用close_buffer_oarchive()释放缓存
		oa = create_buffer_oarchive();

		struct RequestHeader h = { STRUCT_INITIALIZER (xid , get_xid()), STRUCT_INITIALIZER (type , ZOO_CLOSE_OP)};
		rc = serialize_RequestHeader(oa, "header", &h);
		if(rc < 0) {
			//goto upsync_send_fail;
		}
		//应该先发送数据缓冲区长度
		len = get_buffer_len(oa)
		memcpy(request+sizeof(nlen), get_buffer(oa), len);
		close_buffer_oarchive(&oa, 1);
		//zoo_conn_state = ZOO_CONNECTING_STATE;
	} else {
		return;
	}

	nlen = htonl(len);
	memcpy(request, &nlen, sizeof(nlen));
	ctx->send.last = ctx->send.pos + sizeof(nlen) + len;

	while (ctx->send.pos < ctx->send.last) {
		size = c->send(c, ctx->send.pos, ctx->send.last - ctx->send.pos);

#if (NGX_DEBUG)
		{   
			ngx_err_t  err;

			err = (size >=0) ? 0 : ngx_socket_errno;
			ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, err,
					"upsync_send: send size: %z, total: %z",
					size, ctx->send.last - ctx->send.pos);
		}   
#endif

		if (size > 0) {
			ctx->send.pos += size;

		} else if (size == 0 || size == NGX_AGAIN) {
			return;

		} else {
			c->error = 1;
			goto upsync_send_fail;
		}
	}

	if (ctx->send.pos == ctx->send.last) {
		ngx_log_debug0(NGX_LOG_DEBUG_HTTP, c->log, 0,
				"upsync_send: send done.");
	}

	if(zoo_conn_state == ZOO_CONNECTING_STATE){
		zoo_conn_state = ZOO_ASSOCIATING_STATE;
	} else if(zoo_conn_state == ZOO_CONNECTED_STATE) { 
		zoo_conn_state = ZOO_GETTING_DATA_STATE;
	} else if (zoo_conn_state == ZOO_CLOSE_CONNECTED_STATE) {
		zoo_conn_state = ZOO_CONNECTING_STATE;
		c->write->handler = ngx_http_upsync_send_empty_handler;
		upsync_type_conf->clean(upsync_server);
	} else {
	}

	return;

upsync_send_fail:
	ngx_log_error(NGX_LOG_ERR, event->log, 0,
			"upsync_send: send error with upsync_server: %V",
			upsync_server->pc.name);

	ngx_http_upsync_clean_event(upsync_server);
}

static void
ngx_http_upsync_zk_recv_handler(ngx_event_t *event)
{
	int len = 0;
	//char prime_storage_buffer[40] = {0};

	ssize_t                                size, n;
	ngx_pool_t                            *pool;
	ngx_connection_t                      *c;
	ngx_http_upsync_ctx_t                 *ctx;
	ngx_http_upsync_server_t              *upsync_server;

	if (ngx_http_upsync_need_exit()) {
		return;
	}

	c = event->data;
	upsync_server = c->data;
	ctx = &upsync_server->ctx;

	if (ctx->pool == NULL) {
		pool = ngx_create_pool(NGX_DEFAULT_POOL_SIZE, ngx_cycle->log);
		if (pool == NULL) {
			ngx_log_error(NGX_LOG_ERR, event->log, 0,
					"upsync_recv: recv not enough memory");
			return;
		}
		ctx->pool = pool;

	} else {
		pool = ctx->pool;
	}

	if (ctx->recv.start == NULL) {
		ctx->recv.start = ngx_pcalloc(pool, ngx_pagesize);
		if (ctx->recv.start == NULL) {
			goto upsync_recv_fail;
		}

		ctx->recv.last = ctx->recv.pos = ctx->recv.start;
		ctx->recv.end = ctx->recv.start + ngx_pagesize;
	}

	while (1) {
		n = ctx->recv.end - ctx->recv.last;

		if (n =tatic ngx_int_t 0) {
			size = ctx->recv.end - ctx->recv.start;
			new_buf = ngx_pcalloc(pool, size * 2);
			if (new_buf == NULL) {
				goto upsync_recv_fail;
			}
			ngx_memcpy(new_buf, ctx->recv.start, size);

			ctx->recv.pos = ctx->recv.start = new_buf;
			ctx->recv.last = new_buf + size;
			ctx->recv.end = new_buf + size * 2;

			n = ctx->recv.end - ctx->recv.last;
		}

		size = c->recv(c, ctx->recv.last, n);

#if (NGX_DEBUG)
		{
			ngx_err_t  err;

			err = (size >= 0) ? 0 : ngx_socket_errno;
			ngx_log_debug2(NGX_LOG_DEBUG, c->log, err,
					"upsync_recv: recv size: %z, upsync_server: %V ",
					size, upsync_server->pc.name);
		}
#endif

		if (size > 0) {
			ctx->recv.last += size;
			continue;
		} else if (size == 0) {
			break;
		} else if (size == NGX_AGAIN) {
			return;
		} else {
			c->error = 1;
			goto upsync_recv_fail;
		}
	}

	if (ctx->recv.last != ctx->recv.pos) {
		if (zoo_conn_state == ZOO_ASSOCIATING_STATE)
		{
			struct prime_struct prime_storage;
			memset(&prime_storage, 0, sizeof(prime_storage));
			//len = HANDSHAKE_RPLY_SIZE;
			deserialize_prime_response(&prime_storage, ctx->recv.pos);
			ctx->recv.pos += HANDSHAKE_RPLY_SIZE;
			ngx_log_debug2(NGX_LOG_DEBUG_HTTP, c->log, 0,
					"zk_recv_handler: sessionID: %#lld, timeout: %d",
					prime_storage.sessionId, prime_storage.timeOut);
			//printf("sessionID: %#lld, timeout: %d\n", prime_storage.sessionId, prime_storage.timeOut);
			zoo_conn_state = ZOO_CONNECTED_STATE;

		} else if (zoo_conn_state == ZOO_GETTING_DATA_STATE) {
			ngx_http_upsync_process(upsync_server);

			zoo_conn_state = ZOO_CLOSE_CONNECTED_STATE;
			c->read->handler = ngx_http_upsync_recv_empty_handler;

		} else {
		}
	}
	return;

upsync_recv_fail:
	ngx_log_error(NGX_LOG_ERR, event->log, 0,
			"upsync_recv: recv error with upsync_server: %V",
			upsync_server->pc.name);

	ngx_http_upsync_clean_event(upsync_server);
}

static ngx_int_t
ngx_http_upsync_zk_parse_result(void *data)
{
	int len = 0;
	struct ReplyHeader hdr;
	struct iarchive 				*ia = NULL;

	ngx_int_t                       max_fails=2, backup=0, down=0;
	ngx_http_upsync_ctx_t          *ctx;
	ngx_http_upsync_conf_t         *upstream_conf = NULL;
	ngx_http_upsync_server_t       *upsync_server = data;

	ctx = &upsync_server->ctx;

	if (ngx_array_init(&ctx->upstream_conf, ctx->pool, 16,
				sizeof(*upstream_conf)) != NGX_OK)
	{
		ngx_log_error(NGX_LOG_ERR, ngx_cycle->log, 0,
					"upsync_parse_json: array init error");
		return NGX_ERROR;
	}

	ngx_memcpy(&len, ctx->recv.pos, sizeof(len));
	ctx->recv.pos += sizeof(len);
	len = ntohl(len);

	ia = create_buffer_iarchive(ctx->recv.pos, len);

	deserialize_ReplyHeader(ia, "hdr", &hdr);

	//因为发送的请求只有ZOO_GETCHILDREN_OP操作，所以操作结果不会是PING_XID、WATCHER_EVENT_XID、SET_WATCHES_XID、AUTH_XID，结果类型也只有COMPLETION_STRINGLIST
	if (hdr.xid == PING_XID) {

	} else if (hdr.xid == WATCHER_EVENT_XID) {

	} else if (hdr.xid == SET_WATCHES_XID) {

	} else if (hdr.xid == AUTH_XID){

	} else {
		//如果结果类型是COMPLETION_STRINGLIST
		if(1) {
			struct GetChildrenResponse res;

			deserialize_GetChildrenResponse(ia, "reply", &res);

			int i = 0;
			char ** b = res.children.data;
			for(; i < res.children.count; i++) {
				//printf("children%d:	%s\n", i, b[i]);
				if (ngx_http_upsync_check_key(*(b+i)) != NGX_OK) {
					continue;
				}
				upstream_conf = ngx_array_push(&ctx->upstream_conf);
				ngx_memzero(upstream_conf, sizeof(*upstream_conf));
				ngx_sprintf(upstream_conf->sockaddr, "%*s", ngx_strlen(*(b+i)), *(b+i));
				//TODO: 获取存储了upstream属性的节点数据
				upstream_conf->weight = 1;
				upstream_conf->max_fails = 2;
				upstream_conf->fail_timeout = 10;
				upstream_conf->down = 0;
				upstream_conf->backup = 0;
			}

			deallocate_GetChildrenResponse(&res);
		}
	}

	if(ia != NULL) {
		close_buffer_iarchive(&ia);
	}
	return NGX_OK;
}
*/

#endif
