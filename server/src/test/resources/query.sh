#!/bin/sh
#wrk -c 32 -d 10 -t 32 -H "Connection: keep-alive" -s post.lua http://127.0.0.1:2480/query/graph/select%20from%20V%20limit%201
ab -n1000000 -k -c32 -A root:root http://127.0.0.1:2480/query/performance/select%20from%20V%20limit%201
