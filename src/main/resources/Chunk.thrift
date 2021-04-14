namespace java chunk

typedef i32 int

service Chunk{
    binary getFile(1:string name),
    bool containsFile(1:string name),
    void deleteFile(1:string name),
    binary getMetadata(),
    void uploadFile(1:string name, 2:binary file),
    void updateFile(1:string name, 2:binary file),
}