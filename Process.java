// Stores Process Information for Clients and Servers
public class Process {

    //Variable Declaration
    public String processId, ipAddress;
    public int portNumber;

    //List of Valid Files in the Servers
    static String[] listOfFiles = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};

    //Client Constructor
    public Process(String procId) {
        this.processId = procId;
    }

    //Server Constructor
    public Process(String procId, String ipAdd, int pNum) {
        this.processId = procId;
        this.ipAddress = ipAdd;
        this.portNumber = pNum;
    }
}