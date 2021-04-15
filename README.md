# DistributedStorage

Files cannot exceed 16MB due to mongoDB constraint.

Add this to your server program VM options:

 -Dmongodb.uri="mongodb+srv://admin:<password>>@cluster0.giyol.mongodb.net/myFirstDatabase?retryWrites=true&w=majority