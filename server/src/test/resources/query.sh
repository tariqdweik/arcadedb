#!/bin/sh
#ab -n1000000 -p post.txt -k -c32 http://127.0.0.1:2480/sql/graph/select%20from%20V%20limit%201
wrk -c 32 -d 10 -t 32 -H "Connection: keep-alive" -s post.lua http://127.0.0.1:2480/sql/graph/select%20from%20V%20limit%201
