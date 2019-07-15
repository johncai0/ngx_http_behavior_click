/*
 * * Copyright (C) Eric Cai
 * */
#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <uuid/uuid.h>
#include "rdkafka.h"
#include "rdposix.h"
#define MAXLEN 4096
/* Module config */
typedef struct {
    ngx_str_t  serid;
    ngx_str_t  datadir;
    ngx_str_t  kafka;
    ngx_str_t  topic;
} ngx_http_behavior_click_loc_conf_t;
static FILE *logfp;
static ngx_http_behavior_click_loc_conf_t *cfg;
static rd_kafka_t *rk;         /* Producer instance handle */
static rd_kafka_topic_t *rkt;  /* Topic object */
static char *ngx_http_behavior_click(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);
static void *ngx_http_behavior_click_create_loc_conf(ngx_conf_t *cf);
static char *ngx_http_behavior_click_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child);
static void ngx_http_behavior_click_worker_exit(ngx_cycle_t *cycle);
static ngx_int_t ngx_http_behavior_click_worker_init(ngx_cycle_t *cycle);

char *getUUID(char *str)
{
    uuid_t uuid;
    uuid_generate(uuid);
    uuid_unparse(uuid, str);
    return str;
}

/* Directives */
void ngx_mylog(const char* format, ... )
{
    va_list args;
    va_start (args, format);
    vfprintf (logfp, format, args);
    fflush(logfp);
    va_end (args);
}
void ngx_fmylog(const char* format, ... )
{
    va_list args;
    va_start (args, format);
    FILE *fp=fopen("/tmp/john.log","a+");
    vfprintf (fp, format, args);
    fflush(fp);
    va_end (args);
    fclose(fp);
    fp=NULL;
}

static u_char *getCookie(ngx_list_t *h) {
	ngx_uint_t y;
	ngx_list_part_t *part = &(h->part);
	ngx_table_elt_t *header = part->elts;
	for (y = 0; ; y++) {
        	if (y >= part->nelts) {
            		if (part->next == NULL) {
                		break;
            		}
			part = part->next;
			header = part->elts;
			y = 0;
		}
		if (ngx_strcmp(header[y].key.data,"Cookie") == 0) {
			//ngx_mylog("cookie: %s len: %d\n",header[y].value.data,header[y].value.len);
			return header[y].value.data;
		}
	}
	return (u_char *)"";
}
long int getMicroseconds() {
	struct timeval tv;
	if (gettimeofday(&tv,NULL) == -1)
		return -1;
	return  tv.tv_sec*1000000 + tv.tv_usec;
}
static ngx_command_t  ngx_http_behavior_click_commands[] = {
    { 
	ngx_string("behavior_click_ser"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_behavior_click,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_behavior_click_loc_conf_t, serid),
        NULL
    },
    { 
	ngx_string("behavior_click_datadir"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_behavior_click,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_behavior_click_loc_conf_t, datadir),
        NULL
    },
    { 
	ngx_string("behavior_click_kafka"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_behavior_click,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_behavior_click_loc_conf_t, kafka),
        NULL
    },
    { 
	ngx_string("behavior_click_topic"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_http_behavior_click,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_behavior_click_loc_conf_t, topic),
        NULL
    },
        ngx_null_command
};
/* Http context of the module */
static ngx_http_module_t  ngx_http_behavior_click_module_ctx = {
    NULL,                                  /* preconfiguration */
    NULL,                                  /* postconfiguration */
    NULL,                                  /* create main configuration */
    NULL,                                  /* init main configuration */
    NULL,                                  /* create server configuration */
    NULL,                                  /* merge server configuration */
    ngx_http_behavior_click_create_loc_conf,         /* create location configration */
    ngx_http_behavior_click_merge_loc_conf           /* merge location configration */
};
/* Module */
ngx_module_t  ngx_http_behavior_click_module = {
    NGX_MODULE_V1,
    &ngx_http_behavior_click_module_ctx,             /* module context */
    ngx_http_behavior_click_commands,                /* module directives */
    NGX_HTTP_MODULE,                       /* module type */
    NULL,                                  /* init master */
    NULL,                                  /* init module */
    ngx_http_behavior_click_worker_init,                                  /* init process */
    NULL,                                  /* init thread */
    NULL,                                  /* exit thread */
    ngx_http_behavior_click_worker_exit,                                  /* exit process */
    NULL,                                  /* exit master */
    NGX_MODULE_V1_PADDING
};

static void ngx_http_behavior_click_worker_exit(ngx_cycle_t *cycle)
{
	if (rk != NULL) {
		rd_kafka_flush(rk, 1000 /* wait for max 10 seconds */);
		rd_kafka_topic_destroy(rkt);
		rd_kafka_destroy(rk);
	}
    	if (logfp != NULL) {
		fclose(logfp);
		//ngx_fmylog("[process exit] logfp: %p closed\n",logfp);
		logfp=NULL;
	}
}
static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
        if (rkmessage->err)
                ngx_fmylog("%% Message delivery failed: %s\n",rd_kafka_err2str(rkmessage->err));
}
static int connectKafka(const u_char *kafka,const u_char *topic) {
        rd_kafka_conf_t *conf;  /* Temporary configuration object */
	char errstr[512];       /* librdkafka API error reporting buffer */
	conf = rd_kafka_conf_new();
	if (rd_kafka_conf_set(conf, "bootstrap.servers", (char *)kafka, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                ngx_fmylog("%s\n", errstr);
                return 1;
        }
	rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        if (!rk) {
                ngx_fmylog("%% Failed to create new producer: %s\n", errstr);
                return 1;
        }
	rkt = rd_kafka_topic_new(rk, (char *)topic, NULL);
	if (!rkt) {
                ngx_fmylog("%% Failed to create topic object: %s\n",rd_kafka_err2str(rd_kafka_last_error()));
                rd_kafka_destroy(rk);
                return 1;
        }
	return 0;
}
static int priductKafkaForstr(char *message) {
        int i=0;
        if (strlen(message) == 0) return 0;
retry:
        if (rd_kafka_produce(rkt,RD_KAFKA_PARTITION_UA,RD_KAFKA_MSG_F_COPY,message,strlen(message),NULL, 0,NULL) == -1) {
                ngx_fmylog("%% Failed to produce to topic %s: %s\n",rd_kafka_topic_name(rkt),rd_kafka_err2str(rd_kafka_last_error()));
                if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                        rd_kafka_poll(rk, 1000);
                        if (i++ >5) return -1;
                        goto retry;
                }
        }
        rd_kafka_poll(rk, 0);
        return 0;
}
static ngx_int_t ngx_http_behavior_click_worker_init(ngx_cycle_t *cycle) {
    u_char path[MAXLEN];
    time_t tmt=time(NULL);
    struct tm *tmp=localtime(&tmt);
	ngx_fmylog("[ERROR] %s/%s\n",cfg->kafka.data,cfg->topic.data);
    ngx_sprintf(path,"%s/behavior_click_%s_%d%02d%02d.log%c",
		cfg->datadir.data,cfg->serid.data,
		tmp->tm_year+1900,tmp->tm_mon+1,tmp->tm_mday,'\0');
    if ((logfp=fopen((char *)path,"a+")) == NULL)
	return NGX_ERROR;
    if (connectKafka(cfg->kafka.data,cfg->topic.data) == 1)
	return NGX_ERROR;
    return NGX_OK;
}

int getValue(u_char *source,u_char *key, u_char *value) {
        int i=0;
        u_char *http=(u_char *)strstr((char *)source,(char *)"HTTP/");
        u_char *p=(u_char *)strstr((char *)source,(char *)key);
        if (p != NULL && (p == source || *(p-1) == '&')) {
                p=p+strlen((char *)key)+1;
                for (i=0;*(p+i) != '&'&& p+i != http-1;i++)
                        value[i]=*(p+i);
        }
        value[i]='\0';
        return i;
}
/* Handler function */
static ngx_int_t ngx_http_behavior_click_handler(ngx_http_request_t *r)
{
    if(!(r->method & (NGX_HTTP_GET|NGX_HTTP_POST)))
    {
        return NGX_HTTP_NOT_ALLOWED;
    }
    ngx_chain_t out;
    char arg_str[MAXLEN]={'\0'};
    char ref_str[MAXLEN]={'\0'};
    long tmlog=getMicroseconds();
    char alldata[MAXLEN*2]={'\0'};
    r->headers_out.content_type.len = sizeof("text/html") - 1;
    r->headers_out.content_type.data = (u_char *) "text/html";
    r->headers_out.status = NGX_HTTP_OK;

    ngx_uint_t i,alliplen=0;
    ngx_table_elt_t **arr=r->headers_in.x_forwarded_for.elts;
    u_char allip[MAXLEN]={'\0'};
    for (i=0;i<r->headers_in.x_forwarded_for.nelts;i++) {
	if (i == 0) {
		strcpy((char *)allip,(char *)arr[i]->value.data);
		alliplen=arr[i]->value.len;
	} else {
		strcat((char *)allip,(char *)",");
		strcpy((char *)allip,(char *)arr[i]->value.data);
		alliplen=alliplen+arr[i]->value.len+1;
	}
    }
    if (i!=0) {
    	allip[alliplen+1]='\0';
    }
    //client real ip output
    u_char *rip;
    if (r->headers_in.x_real_ip != NULL) {
	rip=r->headers_in.x_real_ip->value.data;
    } else {
	r->connection->addr_text.data[r->connection->addr_text.len]='\0';
	rip=r->connection->addr_text.data;
    }
    ngx_str_t mystr=ngx_string("ok");
    if (r->args.len > 0) {
    	strcpy(arg_str,(char *)r->args.data);
    	arg_str[r->args.len]='\0';
    }
    if (r->headers_in.referer != NULL) {
    	strcpy(ref_str,(char *)r->headers_in.referer->value.data);
    	ref_str[r->headers_in.referer->value.len+1]='\0';
    }
	int newuser=0;
	u_char *cookie=getCookie(&r->headers_in.headers);
	if (!ngx_strstr(cookie,"BeUid")) {
		ngx_table_elt_t* coks=ngx_list_push(&r->headers_out.headers);
		coks->hash = 1;
		char cookiestr[36];
		char cookieskv[43];
		getUUID(cookiestr);
		sprintf(cookieskv,"BeUid=%s",cookiestr);
    		ngx_str_set(&coks->key, "Set-Cookie");
    		//ngx_str_set(&coks->value, cookieskv);
		coks->value.data=(u_char *)&cookieskv;
		coks->value.len=ngx_strlen(cookieskv);
		newuser=1;
    		
		sprintf(alldata,"%s%c%s%c%s%c%ld%c%s%c%s%c%s%c%d%c%s",
			r->headers_in.server.data,22,
        		rip,22,
        		ref_str,22,
        		tmlog,22,
        		allip,22,
        		r->headers_in.user_agent->value.data,22,
        		cookieskv,22,
			newuser,22,
        		arg_str
    		);
	} else {
    		sprintf(alldata,"%s%c%s%c%s%c%ld%c%s%c%s%c%s%c%d%c%s",
			r->headers_in.server.data,22,
        		rip,22,
        		ref_str,22,
        		tmlog,22,
        		allip,22,
        		r->headers_in.user_agent->value.data,22,
        		cookie,22,
			newuser,22,
        		arg_str
    		);
	}
    ngx_mylog("%s\n",alldata);
    int kafka_status=priductKafkaForstr(alldata);
    if (kafka_status == -1) {
	ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Failed Send message to kafka, work process will exit.");
	ngx_http_behavior_click_worker_exit(NULL);
	exit(1);
    }
//==================================================
    r->headers_out.content_length_n = 2;
    out.buf = ngx_pcalloc(r->pool, sizeof(ngx_buf_t));
    if(out.buf == NULL)
    {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0, "Failed to allocate response buffer.");
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
    }
    out.next = NULL;
    out.buf->pos = mystr.data;
    out.buf->last = mystr.data+2;
    out.buf->memory = 1;
    out.buf->last_buf = 1;
    if(ngx_http_send_header(r) != NGX_OK)
        return NGX_HTTP_INTERNAL_SERVER_ERROR;
//ngx_mylog("http_in_Cookie: %s\n",getCookie2(&r->headers_in.headers,1));
//ngx_mylog("http_out_Cookie: %s\n",getCookie2(&r->headers_out.headers,2));
    return ngx_http_output_filter(r, &out);
}
static char *
ngx_http_behavior_click(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
    ngx_http_core_loc_conf_t  *clcf;
    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    clcf->handler = ngx_http_behavior_click_handler;
    ngx_conf_set_str_slot(cf,cmd,conf);
    return NGX_CONF_OK;
}
static void *
ngx_http_behavior_click_create_loc_conf(ngx_conf_t *cf)
{
    ngx_http_behavior_click_loc_conf_t  *conf;
    conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_behavior_click_loc_conf_t));
    if (conf == NULL) {
        return NGX_CONF_ERROR;
    }
    conf->serid.len = 0;
    conf->serid.data = NULL;
    conf->datadir.len = 0;
    conf->datadir.data = NULL;
    conf->topic.len = 0;
    conf->topic.data = NULL;
    conf->kafka.len = 0;
    conf->kafka.data = NULL;
    return conf;
}
static char *
ngx_http_behavior_click_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
    ngx_http_behavior_click_loc_conf_t *prev = parent;
    ngx_http_behavior_click_loc_conf_t *conf = child;
    ngx_conf_merge_str_value(conf->serid, prev->serid, "");
    ngx_conf_merge_str_value(conf->datadir, prev->datadir, "");
    ngx_conf_merge_str_value(conf->kafka, prev->kafka, "");
    ngx_conf_merge_str_value(conf->topic, prev->topic, "");
    cfg=conf;
    return NGX_CONF_OK;
}
