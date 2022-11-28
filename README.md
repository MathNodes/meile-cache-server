# meile-cache-server
Meile Cache Server for Sentinel dVPN Nodes

## About
Initially started under the Meile TUI, the cache server represents a streamlined version of Sentinel blockchian node data. The concept is instead of querying the blockchain for node data, a cache server is used, which is updated nightly, to maintain Sentinel node data. This creates a supercharged version of Meile when retrieving node data instead of having to query the blockchain each time which takes 15 seconds. 

Meile cache server uses an API backend which Meile queries for the node data. These scripts are the nightly runs to populate the Meile node database on the server backend. 

The API backend and Hot Wallet (used for FIAT Gateway and later incentives) is available only to the Sentinel team and anyone within the Keybase organization. Eventually we will make this public, but for now the source is available only to team members. 
