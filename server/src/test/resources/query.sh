#!/bin/sh
ab -n1000000 -p post.txt -k -c10 http://127.0.0.1:2480/command/graph/select%20from%20V%20limit%201
