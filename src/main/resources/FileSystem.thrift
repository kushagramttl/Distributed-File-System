namespace java server

typedef i32 int

service FileSystem{
    binary getFile(1:string name),
    void uploadFile(1:string name, 2:binary file),
    void updateFile(1:string name, 2:binary file),
    void deleteFile(1:string name),
}