namespace cpp com.thrift
namespace java com.thrift

service CoordinatorService {

    string PREPARE(1:string pid),

    string ACCEPT(1:string pid, 2:string value),

    string LEARN(1:string pid, 2:string value)

}
