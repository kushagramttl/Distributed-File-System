package paxos.client;

import Helper.Logger;


public class Main {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.print("usage <host-name> <port-number>\n");
            System.exit(0);
        }
        try {
            int port = Integer.parseInt(args[1]);
            String hostName;
            if (!(hostName = args[0]).equals("")) {
                Logger logger = new Logger("RPC Client (" + port + ")");
                Client client = new Client(port, logger, hostName);
                client.sendRequest();

            }
            else {
                System.out.print("Please provide a valid hostname\n");
            }

        }
        catch (NumberFormatException e) {
            System.out.print("Please provide an Integer as port number\n");
            System.exit(0); // to not trigger shutdown hook
        }


    }

}
