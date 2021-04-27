package client;

import java.util.Arrays;
import java.util.List;


public class Request {

  Operation operation;
  String fileName;
  String filePath;

  @Override
  public String toString() {
    return operation + "," + fileName + "," + filePath;
  }

  public static Request fromString(String requestString) {

    if (!isValidRequestString(requestString)) {
      return null;
    }

    Request request = new Request();

    List<String> requestData = Arrays.asList(requestString.split(","));
    switch (requestData.get(0).toUpperCase()) {
      case "GET": {
        request.operation = Operation.GET;
        break;
      }
      case "UPLOAD": {
        request.operation = Operation.UPLOAD;
        break;
      }
      case "DELETE": {
        request.operation = Operation.DELETE;
      }
      case "UPDATE": {
        request.operation = Operation.UPDATE;
      }
    }

    request.fileName = requestData.get(1);

    request.filePath = requestData.size() > 2 ? requestData.get(2) : "";

    return request;
  }

  public static boolean isValidRequestString(String requestString) {
    List<String> requestData = Arrays.asList(requestString.split(","));

    if (requestData.size() < 2 || requestData.size() > 3)
      return false;

    switch (requestData.get(0).toUpperCase()) {
      case "UPLOAD":
      case "UPDATE": {
        if (requestData.size() != 3)
          return false;
        break;
      }
      case "GET":
      case "DELETE": {
        if (requestData.size() != 2)
          return false;
        break;
      }
      default:
        return false;
    }
    return true;

  }

}
