#!/bin/bash

set -euo pipefail

just build-prod

cp target/release/gossip_collector infra/ansible/binaries/current/gossip_collector
cp target/release/gossip_archiver infra/ansible/binaries/current/gossip_archiver