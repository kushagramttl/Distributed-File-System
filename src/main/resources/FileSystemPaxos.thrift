namespace cpp com.thrift
namespace java com.thrift

namespace java server

typedef i32 int

service FileSystemPaxos{
    binary getFile(1:string name),
    void uploadFile(1:string name, 2:binary file),
    void updateFile(1:string name, 2:binary file),
    void deleteFile(1:string name),
    bool registerChunk(1:int port, 2:int replicaPort),

    string PREPARE(1:double pid),
    string ACCEPT(1:double pid, 2:string value),
    string LEARN(1:double pid, 2:string value)
}
