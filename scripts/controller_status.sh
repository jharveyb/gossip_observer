#!/bin/bash

# TODO: put output into a JSON array
grpcurl -plaintext do-medium:50061 controllerrpc.ControllerService/Status | jq '.statuses[].info | .uuid, .pubkey.pubkey, .balances'