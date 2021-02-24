import java.io.*;
import java.net.*;

//Allows us to keep track of all routes between servers and clients and have the read and write buffer ready for them to send or receive messages.
public class Route {

    //Variable Declaration
    String processId;
    Socket socket;
    PrintWriter pwriter;
    BufferedReader breader;

    //Constructor
    Route(String ipAddress, int portNumber) throws IOException, UnknownHostException {
        this.socket = new Socket(ipAddress, portNumber);
        this.pwriter = new PrintWriter(this.socket.getOutputStream(), true);
        this.breader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
    }

    //Constructor
    Route(String ipAddress, int portNumber, String procId) throws IOException, UnknownHostException {
        this.processId = procId;
        this.socket = new Socket(ipAddress, portNumber);
        this.pwriter = new PrintWriter(this.socket.getOutputStream(), true);
        this.breader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
    }

    //Constructor
    Route(Socket psocket) throws IOException, UnknownHostException {
        this.socket = psocket;
        this.pwriter = new PrintWriter(this.socket.getOutputStream(), true);
        this.breader = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
    }

    //Sends Message on the Route
    public void sendMessage(String message) {
        this.pwriter.println(message);
    }

    //Receive Message from the Route
    public String receiveMessage() throws IOException {
        return this.breader.readLine();
    }

    //Clost the Route
    public void closeRoute() throws IOException {
        this.socket.close();
    }
}