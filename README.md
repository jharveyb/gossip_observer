# Gossip Observer

Try and characterize the LN gossip network by deploying a distrubuted set of
LDK nodes with specific peer connections, and collecting info on unfiltered
received traffic.

## Usage

Run locally, or on fly.io.

<https://tailscale.com/kb/1132/flydotio>

You can pull connection strings for nodes in a specific location from <https://1ml.com>.

## Results

From PlebFi 2025:

On testnet, comparing a node in Miami connected to a node in Australia, and
a node in Mumbai connected to a node in the U.S., we observe a time difference
in receiving specific messages of ~22 seconds.

This cound be caused by some default timer values in the forked LDK project,
or just similar defaults across the network.