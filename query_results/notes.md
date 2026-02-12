# LN Graph Analysis Tools

## Dependencies

We used `uv` to generate the initial plots of collected data.
However, for LN graph analysis, the graph is large enough to justify using
libraries like `graph-tool`. The dependency on C++ libraries seems to prevent,
or at least complicate using this dependency AND managing the other dependencies with `uv`.

Debian testing packages:

- python3-graph-tool
- python3-pandas
- pyhton3-numpy

For the (old) plotting script:

- python3-matplotlib

## Approach

We have two goals:

- Identify _communities_ of LN nodes, based on basic information about the graph.
I.e. channels and channel capacity. Once we have a reasonable list of communities,
we can have individual collector instances connect to specific communities.
- _Classify_ LN nodes, given the information we have about each node
(node alias, clearnet/tor/both, AS number, edge degree, total capacity, age, first_seen, closed channel count, etc.) We could also mix in data from other
entities like Amboss or Terminal. Once we have nodes classified, we can see which
classes of nodes occupy specific communities, and make sure we connect to a variety
of node classes. We can also run queries over specific node types vs. all nodes.

### Community detection

The common approaches seem to be algorithms based on modularity (Leiden, Louvain)
and more complex techniques (statistical block modelling / SBM). Further reading:

<https://skewed.de/lab/posts/modularity-harmful/>

SBM can also be run with additional vertex and edge properties, like edge weights
for example.

## Results

### SBM, no edge weights

Detected 15 hierarchical levels.
  Level 0: 70 communities (finer → coarser)
  Level 1: 13 communities (finer → coarser)
  Level 2: 6 communities (finer → coarser)
  Level 3: 3 communities (finer → coarser)
  Level 4: 1 communities (finer → coarser)
  Level 5: 1 communities (finer → coarser)
  Level 6: 1 communities (finer → coarser)
  Level 7: 1 communities (finer → coarser)
  Level 8: 1 communities (finer → coarser)
  Level 9: 1 communities (finer → coarser)
  Level 10: 1 communities (finer → coarser)
  Level 11: 1 communities (finer → coarser)
  Level 12: 1 communities (finer → coarser)
  Level 13: 1 communities (finer → coarser)
  Level 14: 1 communities (finer → coarser)

Hierarchical Model Description Length: 182314.27

Saving hierarchical results (15 levels)...
  Level 0 saved to ./data/ln_communities_nested_level_0.csv

  Top 5 Largest Communities at Level 0:
  community_id
1545    1891
7710    1422
1537    1053
8492     902
1098     864

Conclusions from level 0:

1ML node is a hub for community 1545
LNT.chengdu is a hub for community 8492
LNT.Thailand is a hub for community 1537
CoinGate is a hub for community 10948
node01.fulmo.org is a hub for community 10835

### SBM with edge weights

Detected 15 hierarchical levels.
  Level 0: 22 communities (finer → coarser)
  Level 1: 9 communities (finer → coarser)
  Level 2: 2 communities (finer → coarser)
  Level 3: 1 community (finer → coarser)

Hierarchical Model Description Length: 244823.54

Community 10023: Size: 5697
Community 1880: Size: 754
Community 1008: Size: 85
Community 7153: Size: 832
Community 9696: Size: 78
Community 10420: Size: 901
Community 8165: Size: 1113
Community 10959: Size: 130
Community 495: Size: 24
Community 2079: Size: 1
Community 7150: Size: 69
Community 11489: Size: 24
Community 5631: Size: 65
Community 6636: Size: 61
Community 6269: Size: 1066
Community 4200: Size: 431
Community 9854: Size: 73
Community 9203: Size: 100
Community 10772: Size: 18
Community 10395: Size: 1
Community 4231: Size: 70

### Manual Community Analysis

Working backwards from node aliases and other information:

We found an entity by poking around mempool.space, before ever running any
community detection:

<https://btclnt.com/#/>

LNT Periphery:
Level 1 community 19:
2079, 1 -> LNT.chengdu, a hub node for BTCLNT project
10395, 1 -> LNT.Thailand, second hub for BTCLNT project

Level 1 community 20:
6269, 1066 -> Light Pink -> Spokes hanging off of LNT.Thailand
10420, 901  -> Light Yellow -> Spokes hanging off of LNT.chengdu

So BTCLNT is ~2000 nodes already spoken for.

Suburbs:
Level 1 community 0:
10023, 5697 -> Grey -> "Opening Menu", many low-degree nodes that connecting in to the center, not necessarily each other; lots of Start9s, Alby Hub instances, etc.
8165, 1113 -> Light Brown -> "Level 2", lightning.emzy.de, moneydevkit, Netherlands Amsterdam LN Hub

Midtown:
Level 1 community 4:
6636, 61 -> Light Purple -> "Well Connected", includes 1ML, CoinGate, gameb_1, aantonop, one LQwD and Megalithic node, Start9 hub
5631, 65 -> Blood Orange -> "Midsize", Mullvad, Blocktank, lightning-roulette, WalletOfSatoshi, jb55
1008, 85 -> Light Purple-Blue -> "Central-Adjacent"

Downtown / the Core:
Level 1 community 2:
11489, 24 -> Dark Purple -> "Balanced Hubs", ACINQ, Podcast Index, citadel21, Moon, Bitrefill Routing
9696, 78 -> Light Pink -> "Wizened", fulmo, lnbits, Fedi, Blockstream Store, Amboss.Space, stacker.news
10772, 18 -> Neon Blue -> "Big Chungus", block, OpenNode, okx, Kraken, bfx, 4 LNBiG nodes, Strike

Brooklyn:
Level 1 community 12:
7153, 832 -> Dark Brown -> PubkeyMain, El Salvador Pupusa Guy
7150, 69 -> Lightest Purple -> LOOP, Rizful, routing.blinkbtc.com, bipa, speed1, Megalithic, Noones, IBEX
10959, 130 -> Lighteset Blue -> BCash_Is_Trash, Play-asia.com, BTC Nigeria

Lower East Side:
Level 1 community 8:
4200, 431 -> Green -> Voltage, rompert.hashposition.com, nerdminerstore.com, coinos

Long Island City:
Level 1 community 6:
9203, 100 -> Pink -> scarce-city, cloakedwireless, satsquares
1880, 754 -> Orange -> lnrouter, mempool.space nodes, Einundzwanzig

Jersey City / Business Park:
Level 1 community 7:
9854, 73 -> Dark Grey -> "Team Corn", looks like this South Korean wallet: <https://team.oksu.su/>
495, 24 -> Dark Blue -> zaphq, River, Bitnob
4231, 70 -> Light Teal -> Binance

## Opening Channels

Aim to cover different second-level communities. Chronological order of opening:

03abd82259d259b0acecd1e8ae1d35b04e3bf4270ba2dbb9416ad1b150f255e371
Community 8165 with lightning.emzy.de - 1028429300572553216

03c926a800929fcc99826c23dd1a39a4e1253c96f6249595bab7620c3d810f3662
Community 4200 with SchroedingersCat -1028571137643905025

02be86fcfd15193884d72332ba5a7112c2b929dd388c87e2f6bb36b3e0fc1af7c8
Community 10023 with Star Service - 1028571137643839489

029ab8668a756d0bcfa11e76c8f2e5e4de27121acf417bedaa3e38044361b51ecd
Community 9696 with Amboss.Space - 1028579933714841601

026396c83abf5eedd7d7b3d4047790ab46b875e9918501810fc031d1f374e914ae
Community 11489 with Megalithic [small channels] - 1028578834279497729

022398a4472f19fb55e5cbfca722579b69b65c323f508abd9030afef4056d3107f
Community 9203 with satsquares - 1028964762817527809

02590d4fac69d138a88a2e3684f196236e8664e40b941acf80bfaf1c57f2c2472a
Community 6636 with CoinGate - 1028964762816937984
