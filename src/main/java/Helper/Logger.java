package Helper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * This class implements a console and text file logger, it implements two methods logInfo and
 * logErr that print a timestamped info or error message to the console and to an output file. The
 * message header indicates its timestamp, its producer, and the thread ID.
 */
public class Logger {
  private final String name;
  private File output;

  public Logger(String owner) {
    this.name = owner;
    try {
      output = new File(String.format("logs/[%s] %s.txt", owner, getCurrentTime()));
      if (output.createNewFile())
        logInfo("Started new log file of name: " + output.getName());
    } catch (IOException e) {
      logErr("An error occurred while creating the log file.");
      e.printStackTrace();
    }
  }

  private String getCurrentTime() {
    Date date = new Date(System.currentTimeMillis());
    DateFormat formatter = new SimpleDateFormat("MM:d:y HH:mm:ss.SSS");
    formatter.setTimeZone(TimeZone.getTimeZone("EST"));
    return formatter.format(date);
  }

  public void logInfo(String message) {
    String logMessage = String.format("%s %s info message from Thread %d:\n %s\n", getCurrentTime(), name, Thread.currentThread().getId(), message);
    System.out.print(logMessage);
    try {
      FileWriter myWriter = new FileWriter(output.getName(), true);
      myWriter.write(logMessage);
      myWriter.close();
    } catch (IOException e) {
      logErr("An error occurred while writing to the log file.");
      e.printStackTrace();
    }

  }

  public void logErr(String message) {
    String logMessage = String.format("%s %s error message from Thread %d:\n %s\n", getCurrentTime(), name, Thread.currentThread().getId(), message);
    System.out.print(logMessage);
    try {
      FileWriter myWriter = new FileWriter(output.getName(), true);
      myWriter.write(logMessage);
      myWriter.close();
    } catch (IOException e) {
      logErr("An error occurred while writing to the log file.");
      e.printStackTrace();
    }
  }
}