# GoogleFileSystem
Implement the basic imprint of Google File System


Let there be seven data servers, S0; S1; : : : S6 and five clients, C0; C1; : : : ;C4. There exist communication
channels between all servers, and from each client to all the servers. All communication channels are FIFO and reliable
when they are operational. Occasionally, a channel may be disrupted in which case no message can be communicated
across that channel. There exists a hash function, H, such that for each object, Ok, H(Ok) yields a value in the range
0 􀀀 6.
 When a client, Ci has to insert/update an object, Ok, it performs the write at three servers numbered: H(Ok),
H(Ok)+1 modulo 7, and H(Ok)+2 modulo 7.
 When a client, Cj has to read an object, Ok, it can read the value from any one of the three servers: H(Ok),
H(Ok)+1 modulo 7, or H(Ok)+2 modulo 7.
In this project you are required to implement the following:
1. A client should be able to randomly choose any of the three replicas of an object when it wishes to read the value
of the object. If a client tries to read an object that is not present (has not been inserted earlier by any client), the
operation should return with an error code.
2. When a client wishes to update/insert an object into the data repository, it should be able to successfully perform
the operation on at least two, and if possible all the three servers that are required to store the object.
3. If a client is unable to access at least two out of the three servers that are required to store an object, then the
client does not perform updates to any replica of that object.
4. If two or more clients try to concurrently write to the same object and at least two replicas are available, the
writes must be performed in the same order at all the replicas of the object.
You will need to demonstrate that you have implemented all the requirements mentioned above. For instance, you
may need to selectively disable some channel(s) for a brief period of time so that a client is only able to access two or
one replica of an object. If at least two replicas of the object are accessible from a client, updates to that object should
be allowed. However, if only one replica of the object is accessible from a client, the client should abort the update
and output a corresponding message.
You must also selectively disable some channels so that the clients and servers get partitioned into two components,
each with a subset of clients and servers such that some writes are permitted in one partition, while other updates are
permitted in the other partition.
