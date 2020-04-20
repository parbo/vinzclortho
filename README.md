== Vinz Clortho

=== Warning - not for production use
Vinz Clortho was was written by me as part of the recruitment process at a well known company providing streaming media services. Their programming test was to write a distributed key-value store accessible over HTTP. Yes, they seem serious about finding people who are actually able to program stuff.

I have not tested this under any serious loads, so please use with caution.

=== What is it?
It is a distributed key-value store (or a NOSQL database if you will) with an HTTP API. It is implemented in pure Python without any additional dependencies. It is inspired by [[http://www.allthingsdistributed.com/2007/10/amazons_dynamo.html|Amazon's Dynamo paper]] and of other open source Dynamo clones, but mostly by [[https://wiki.basho.com/display/RIAK/Riak|Riak]].

=== Features
* Distributed key-value store. Keys and values can be any data.
* RESTful HTTP API
* No SPOF, all nodes are equal in the cluster
* Consistent hashing is used to be able to add nodes with a minimum of key ownership change
* Data is replicated on N nodes, quorum reads (R) and writes (W) are used to provide the desired level of consistency. Currently, N=3, R=2, W=2 is hardwired. This setting provides read-your-writes consistency (since R+W > N, see the [[http://www.allthingsdistributed.com/2007/10/amazons_dynamo.html|Dynamo paper]]). It also means that one replica can be down without affecting availability. 
* Vector clocks for versioning of values and cluster metadata
* Read-repair of stale/missing data to recover from transient unavailability of nodes
* Gossip protocol for cluster membership and metadata
* No dependencies, uses only Python standard libs
* Multiple store types available (in memory, Berkeley DB, SQLite). Currently you have to patch the source to change it though, Berkeley DB is used by default.
* The nodes can be heterogenous in capacity, since each node's claim on the consistent hash ring is tunable

=== Deficiencies / bugs
* Nodes can't leave the cluster. They can set their claim so that they don't handle any data, but not leave.
* Replication factor is hardwired to 3
* Quorums are not tunable
* Uses pickle to serialize data, which has bugs regarding 32-bit/64-bit versions of Python. Please don't mix 32-bit and 64-bit machines in your cluster.
* No hinted handoff, so when replicas are down the replication factor is not maintained. Read-repair is the only recovery mechanism. 
* No replica synchronization. Since merkle trees are not implemented, replica synchronization is not implemented.
* Failure of nodes is not gossiped to other nodes
* The stored vector clocks are never pruned

=== Design
* Any node can handle a request, just put any load balancer between the cluster and your application
* All communication uses HTTP (so that I didn't have to write multiple clients/servers)
* Consistent hashing is implemented using fixed-size partitions, to facilitate transfer of data when nodes are added. (Called strategy 3 in the [[http://www.allthingsdistributed.com/2007/10/amazons_dynamo.html|Dynamo paper]].)
* SHA1 hash is used for consistent hashing. Keys with the same hash are considered the same. SHA1 is 160 bits, so the likelihood of a collision is very small. 
* Gossip protocol is used for membership, which should scale up to a couple of hundred nodes //(citation needed)//.
* Both client and server are single threaded and asynchronous (implemented on top of asyncore/asynchat). The calls to the underlying db's are handled by a thread pool. The code uses the "deferred"-concept of chained callbacks, borrowed from Twisted.

=== HTTP API
This is heavily influenced by the [[https://wiki.basho.com/display/RIAK/REST+API|Riak API]].

==== Store API

**Note:** All requests to /store should include a X-VinzClortho-ClientId header. This can be any string that uniquely identifies the client. It is used in the vector clock of a value to track versions.

{{{GET /store/mykey}}}

Responses: 
*200 OK
*300 Multiple Choices
*404 Not Found - the object could not be found (on enough partitions)

Important headers:
*X-VinzClortho-Context - An opaque context object that should be provided on subsequent Put or Delete operations

If the response status is 300, then there are concurrent versions of the value. Each version is provided as one part of a multipart/mixed response. The client is responsible for reconciling the versions.

{{{PUT /store/mykey}}}

Responses: 
*200 OK
*404 Not Found - the object could not be found (on enough partitions)

{{{DELETE /store/mykey}}}

Responses: 
*200 OK
*404 Not Found - the object could not be found (on enough partitions)

//Note: PUSH is a synonym for PUT//

====Admin API
{{{GET /admin/claim}}}

Responses: 
*200 OK

The body is the number of partitions claimed by the node

{{{PUT /admin/claim}}}

Responses: 
*200 OK
*400 Bad Request - the data was not a string that could be converted to an integer

Sets the wanted claim to the value in the body. Note that the actual claim may become something else due to replication constraints. Read it with GET.

==== Internal API

The internal communication between nodes also uses HTTP. The internal uri's all start with an underscore. Don't call these yourself.

{{{
/_localstore/mykey
/_handoff
/_metadata
}}} 

=== Installation
Download and unpack, then issue this command (as root or using sudo):\\
{{{python setup.py install}}}

=== Setting up a cluster, an example
This example sets up an 8 node cluster with 1024 partitions on a local machine
Starting the first node:\\
{{{
vinzclortho -a mymachine:8880 -p 1024 &
}}}

Starting subsequent nodes:\\
{{{
vinzclortho -a mymachine:8881 -j mymachine_1:8880 &
vinzclortho -a mymachine:8882 -j mymachine_1:8880 &
vinzclortho -a mymachine:8883 -j mymachine_1:8880 &
vinzclortho -a mymachine:8884 -j mymachine_1:8880 &
vinzclortho -a mymachine:8885 -j mymachine_1:8880 &
vinzclortho -a mymachine:8886 -j mymachine_1:8880 &
vinzclortho -a mymachine:8887 -j mymachine_1:8880 &
}}}

Note that the databases and log files will appear in the directory where you issued the vinzclortho command, and will be named vc_store_partition_address:port.db and vc_log_address:port.log.

Test that it works:\\

{{{
me@mymachine:~$ curl -i -X PUT -d "testvalue" -H "X-VinzClortho-ClientID: myclientid" http://mymachine:8883/store/testkey
HTTP/1.1 200 OK
Server: Tangled/0.1 Python/2.6.5
Date: Sun, 25 Jul 2010 11:06:57 GMT

me@mymachine:~$ curl -i http://mymachine:8887/store/testkey
HTTP/1.1 200 OK
Server: Tangled/0.1 Python/2.6.5
Date: Sun, 25 Jul 2010 11:08:11 GMT
X-VinzClortho-Context: QlpoOTFBWSZTWaRkmXoAACXfgAAQAMF/4AkhGQCev98gIABkRMj1KPTRqaaY1NqPUBvVMhBBNNNNB6gAADRa+EUsjbXtKf1yyLCK1oWBFf36JfmtlBQEAbENMgjdDyPHSRYmzPXyk3bz/B1zWCI+QPYyu1Mnqu7Q/ntBKuEPCZUVDTkGIGikYcIIksADBHVq7mwQ5IqWdKsPvxdyRThQkKRkmXo=
Content-Length: 9

testvalue
}}}

=== Performance and scalability estimates
I haven't been able to test this using a physical machine for each node, so my numbers may be off. I have signed up for an AWS account to test with an EC2 cluster, but haven't had the time yet.

A four node cluster on my fairly recent four core AMD machine seem to be able to handle 300-500 requests per second (tested with small keys and values) to one node. It handles about 1000 raw requests per node, and due to quorum reads each request becomes 3-4 requests depending on if the node taking the request is the owner of the key or not.

The cluster should scale linearly with size, just add more nodes for more storage and more requests per second. Caveat: the partition size cannot be changed, so you must choose this value to be >> than the maximum number of nodes you foresee in your cluster.

Note that the time to disseminate membership metadata is O(log n) due to the gossip protocol. The bandwidth used for gossiping grows linearly with cluster size. This means that you probably should consider some other solution if you need more than a couple of hundred nodes. (Or ask me to implement [[http://en.wikipedia.org/wiki/Chord_(peer-to-peer)|Chord]] or [[http://en.wikipedia.org/wiki/Kademlia|Kademlia]] in Vinz Clortho)

**Example:** use Vinz Clortho to store user created playlists.

Playlists are assumed to be a list of keys into another Vinz Clortho cluster that stores the song metadata, plus some metadata (the time a song was added etc.). It should fit in about a kilobyte per playlist.

*Number of users: 5 million
*Number of playlists per user (on average): 20
*Number of playlists views/edits per user per day: 250 (10 or so views of the user page per day times 20 playlists plus a number of songs added to playlists every day)

This amounts to 250 * 5000000 = 1250000000 requests per day. This is 14467 requests per seconds. This could be handled by 48 nodes serving 300 req/s each. It is probably wise to use a 64 node cluster in this case, since requests are probably not uniformly distributed during the day. Such a cluster should be able to handle 19200 to 32000 requests per second.

Note that this assumes that a load balancer is put in front of the cluster. [[http://blog.rightscale.com/2010/04/01/benchmarking-load-balancers-in-the-cloud/|This test]] seems to indicate that an EC2 cluster with Amazon's ELB balancer should be able to handle that amount of requests.

The storage needed is 5000000 * 20 * 1KB ~ 100 GB, which is just 1.6 GB per node. Note that due to replication it would be almost 5 GB of storage per node. Still, it's peanuts. It may also mean that the databases are cached in RAM which should be good for read performance.

=== Why Vinz Clortho?
Isn't it obvious? Vinz Clortho is the [[http://www.gbfans.com/ghostbusters/characters/keymaster/|keymaster]] demon in Ghostbusters.
