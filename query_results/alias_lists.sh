#!/bin/bash

less data/node_info.txt | jq '.[] | .alias' > data/node_alias_list.txt

less data/node_alias_list.txt | sed -E 's/\.(com|org|net|io|de|xyz|co|biz|uk|us|ca|jp|cz|network|me)"$//' > data/alias_list_filtered.txt

less data/alias_list_filtered.txt | sed -E 's/^"LNT\.+.+$//' > data/aliases_no_lnt.txt

# worth trying all combinations of these 3 common delimiters
logmine -d "\s+" -d "\." -d "\-"