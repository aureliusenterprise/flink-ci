#!/bin/bash

TOKEN=`curl -d 'client_id=m4i_public' -d "username=$KEYCLOAK_ATLAS_USER_USERNAME" -d "password=$KEYCLOAK_ATLAS_ADMIN_PASSWORD" -d 'grant_type=password' \
                "${KEYCLOAK_SERVER_URL}realms/m4i/protocol/openid-connect/token" | jq .access_token`

curl -g -v -X POST -H "Authorization: Bearer ${TOKEN:1:-1}" \
                -H "Content-Type: multipart/form-data" \
                -H "Cache-Control: no-cache" \
                -F data=@response.zip \
                "${ATLAS_EXTERNAL_URL}/admin/import"