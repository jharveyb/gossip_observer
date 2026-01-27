#!/bin/bash

pg_dump -h localhost -p 5432 -U postgres -F c -d gossip_observer -W --verbose --file db_dumps/initial_$(date +%Y%m%d).dump

# Example restore cmd:

# pg_restore -d gossip_observer --verbose db_dumps/$FILENAME