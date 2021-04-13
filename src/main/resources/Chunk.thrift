typedef i32 int

service Chunk{
    void uploadFile(),
    bool containsFile(),
    void updateFile(),
    void deleteFile(),
    void getFile(),
    void getMetadateTable(),
}