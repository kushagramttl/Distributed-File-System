package paxos.master;

public class ServerIdentifier {
    private String hostname;
    private int port;


    public ServerIdentifier(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }


}
