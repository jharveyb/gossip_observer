#!/bin/bash

echo "Node # with localhost addrs:"
cat "$1" | rg -F -e '127.0.0.1' -e 'localhost' -e '0.0.0.0' -e '[::]' | wc -l

cat "$1" | rg -v -F -e '127.0.0.1' -e 'localhost' -e '0.0.0.0' -e '[::]' > "$2"