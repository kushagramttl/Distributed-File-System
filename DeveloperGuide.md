Master server communication and paxos implementation
---

- The server class will provide the list of servers which are partner master servers to the FileSytem
- It uses server identifier to structure the corresponding information about those servers
- The file currently contains the example method for coordinating with the other servers
- The maps or list currently used for managing the chunk server or other metadata need 
to be consistent
- Paxos in master server helps with that a commit on any one of the master servers
will ensure that the rest of them also update their metadata
- Once metadata is consistent or after the proposer in this case has achived the max consesnsu
it will initiate the actual process that is push to the database or storage
- Paxos is just used for ensuring consistency across all the master servers
- As of now recovery is not supported, it is assumed that if a server dies it stays dead

How to start multiple master servers
- You will have 10 seconds window to start all the servers
- After that the server will start reaching out to the servers in the list provided to it.


Chunk servers and the remaining backend
---
Files cannot exceed 16MB due to mongoDB constraint.

Add this to your server program VM options:

-Dmongodb.uri="mongodb+srv://admin:<password>>@cluster0.giyol.mongodb.net/myFirstDatabase?retryWrites=true&w=majority
