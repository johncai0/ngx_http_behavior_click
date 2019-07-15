# ngx_http_behavior_click
一个用户http的用户行为日志收集服务，支持实时日志接收到kafka和落盘。

编译nginx的时候新增第三方模块即可
./configure --prefix=./install  --add-module=./ngx_http_behavior_click_module --with-http_realip_module

编译完成安装配置
location /behavior_click {
    behavior_click_ser "SER1";  //为了方式日志名重复，加个serverid，如果多台收集可以避免这个问题。
		behavior_click_datadir "/tmp/datadir"; //落盘日志的目录
    behavior_click_kafka  "local:9200,127.0.0.1:9200"; //支持kafak集群，多个地址都好隔开
    behavior_click_topic  "test"; //kafka的topic
 }
 
 最终落盘日志文件命名如下：
 behavior_click_[serid]_[YYYYmmDD].log
 
 内容说明：
 1. 当前主机名：当前请求的域名
 2. 请求IP：请求的来源ip
 3. referer：行为数据产生的页面
 4. 请求时间：请求到达服务器的时间
 5. 所有的ip：如果前段有代理，或者多层代理的话，需要加上ip转发，可以获取到所有代理ip
 6. userAgent：ua
 7. cookie：如果来的请求cookie中不带BeUid，本次请求响应时中上BeUid。标识是否同一个用户（如果日志请求服务和行为收集站点用的不是同一个域名，且https协议环境下需要注意第三方coookie的问题）。
 8. 是否新用户：如果BeUid时候新生成的话，会标示这个用户时新用户，值为1，否则为0
 9. 自定义参数：根据自己的业务传想要的指标，这部分数据是请求的参数，原封不动记录下来
