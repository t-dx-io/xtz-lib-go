# xtz-lib-go

The provided code is part of the TAURUS EXPLORER product (https://www.taurusgroup.ch/en/products/explorer).

It monitors the Tezos blockchain and store relevant transactions in a datastore.
It contains the following packages:
* `client`: handle the interactions with the Tezos node.
* `job`: jobs that are scheduled periodically. The `block-fetcher` fetches and processes new Tezos blocks and the `broadcaster` job manages reliable broadcast.
* `model`: data model.
* `store`: datastore.

Please note that this code is provided for informational purposes only and is part of a bigger project, that is not available as open source code.

