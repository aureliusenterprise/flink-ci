#!/bin/bash

curl -g -v -X POST -u "admin:admin" \
                -H "Content-Type: multipart/form-data" \
                -H "Cache-Control: no-cache" \
                -F data=@data/response.zip \
                "http://localhost:21000/admin/import"
