import java.net.*;
import java.security.InvalidParameterException;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.time.*;

//Client Class that inherits from Process
public class Client extends Process {

    //Variable Declaration
    //List of All Servers are maintained
    public List<Process> listOfServers = new ArrayList<Process>();

    //Constructor
    public Client(String processId) {
        super(processId);
    }

    //Entry point for Client
    public static void main(String[] args) throws Exception {
        
        //Variable Declaration
        //List of All Objects on which operations are done
        String[] listOfFiles = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"};

        //Components for message passing
        PrintWriter pWriter;
        BufferedReader bReader;
        Socket Socket = null;
        
        //Counters
        int countOfWrites = 0;
        int totalNumberOfRequests = 20;

        //Random Variables to pick up server and file at random
        Random rnd = new Random();

        //Check if Parameters enetered for running the program are correct
        if (args.length < 1) {
            throw new InvalidParameterException("Parameters Not Sufficient For The Program. Please Enter <Client ID> (mandatory) <Number of Requests> (optional)");
        }
        
        //Class objects
        Client clnt = new Client(args[0]);
        HashFunction hashobj = new HashFunction();

        //Check if user has explicitly mentioned a certain number of operations to be performed
        if (args.length == 2) {
            totalNumberOfRequests = Integer.parseInt(args[1]);
        }

        // Load Server Details From Config File
        clnt.loadServersFromConfigFile("config.txt");

        //Perform the desired number of operation on the objects on different servers
        for (int i = 0; i < totalNumberOfRequests; i++) {

            //Select an object at random from the list
            String selectedObject = listOfFiles[rnd.nextInt(listOfFiles.length)];

            //Select whether to Read/Write to the object at random
            Integer mode = rnd.nextInt(2);

            //Add the servers to which the requests are to be sent based on the hash function
            List<Integer> selectedServersList = Arrays.asList(hashobj.HashServers(selectedObject));
            

            // Mode = 0 --> Read Request
            //Send Read Request to the calculated servers
            if (mode == 0) {                
                
                //Log Send of Read Request
                System.out.println(String.format("Sending Read Request for Object %s From Servers.",selectedObject));

                //Shuffle the list of servers
                Collections.shuffle(selectedServersList);

                //Run the operation for each server from the list until we receive a success 
                for (Integer server : selectedServersList) {
                    
                    //Pick up one server at a time from the selected servers by using the Hash Function
                    Process selectedServer = clnt.listOfServers.get(server);

                    try {
                        
                        //Create a socket for the new server
                        Socket = new Socket(selectedServer.ipAddress, selectedServer.portNumber);

                    }
                    catch (ConnectException ex) {
                        
                        //If connection to server is broken or blocked Log the failure
                        System.out.println(String.format("Failed To Connect To Server: %s For Reading File: %s", selectedServer.processId, selectedObject));
                        
                        //Try connecting to the other servers for reading the object since the current request failed
                        continue;
                    }

                    //Writer to send messages on the Channel
                    pWriter = new PrintWriter(Socket.getOutputStream(), true);
        
                    //Reader to read messages from the channel
                    bReader = new BufferedReader(new InputStreamReader(Socket.getInputStream()));

                    //Frame Read Request Message
                    String requestMessage = String.format("CLIENT:%s:READ:%s", clnt.processId, selectedObject);

                    //Log Read Request In Progress
                    System.out.println(String.format("Reading Object %s By Client %s From Server %s In Progress", selectedObject, clnt.processId, selectedServer.processId));

                    //Send the Request Message to the Selected Server
                    pWriter.println(requestMessage);

                    //Read the Response Message from the Server
                    String responseMessage = bReader.readLine();

                    //Split the components of the Response Message
                    String[] responseDetails = responseMessage.split(":", 2);

                    //ACK - Read Success
                    if (responseDetails[0].equals("ACK")) {

                        //Log Success - The Content of the File  in the selected Server onto the Console
                        System.out.println(String.format("SUCCESS! Contents Of Object: %s In Server: %s Is: %s", selectedObject, selectedServer.processId, responseDetails[1]));

                        //Close the Socket since read is complete
                        Socket.close();

                        //Read Successful, So break from the loop since we dont need to read from the other servers
                        break;
                    }
                    else {
                        //Log Failure of Read
                        System.out.println(String.format("FAILURE! Read Failed For File: %s at Server: %s for Request %s with Response - %s", selectedObject, selectedServer.processId, requestMessage, responseMessage));
                    }
        
                    //Close the socket
                    Socket.close();
                }
            }

            //Mode = 1 --> Write Request
            //Send Write Request to the calculated servers
            else {
                
                //Log Send of Write Request
                System.out.println(String.format("Sending Write Request for Object: %s To Servers", selectedObject));

                //Variable Declaration
                String replicaServersForObject = "";
                long timeStamp = Instant.now().toEpochMilli();

                //Frame the message to be written on the object
                String writeMessage = String.format("Client %s Write # %s", clnt.processId, ++countOfWrites);

                //Maintain list of all Connection Channels between the client and the server for that object
                List<Route> serverRoutes = new ArrayList<>();

                //Send Write Request to all the Servers
                for (Integer server : selectedServersList) {

                    //Pickup each server details
                    Process selectedServer = clnt.listOfServers.get(server);

                    //Route from the client to the server
                    Route rt = null;

                    try {
                        
                        //Create a new route for the client and server
                        rt = new Route(selectedServer.ipAddress, selectedServer.portNumber, selectedServer.processId);
                        
                        //Set the list of servers to which the same write request has already been sent
                        replicaServersForObject = String.format("%s,%s", selectedServer.processId, replicaServersForObject);
                    }
                    catch (ConnectException ex) {

                        //Log Failed to Connect to the server
                        System.out.println(String.format("Failed To Connect To Server: %s For Writing To File: %s Message: %s", selectedServer.processId, selectedObject, writeMessage));
                        
                    }
                    finally {

                        //Maintain all these created routes
                        serverRoutes.add(rt);
                    }
                }

                //Abort the write if not enough servers are available
                if (countValidRoutes(serverRoutes) < 2) {

                    //Frame Abort Message
                    String abortMessage = String.format("CLIENT:%s:ABORT:%s:%s:%s", clnt.processId, selectedObject, writeMessage, timeStamp);
                
                    //Send Abort Message to all the Serer Routes which are not null
                    for (Route rt : serverRoutes) {
                        //If route doesnot exist then move on to the route
                        if (rt == null)
                            continue;
    
                        //Log Abort Write
                        System.out.println(String.format("Abort Write Message: %s to Object: %s, By Client: %s To Server %s At %s", writeMessage, selectedObject, clnt.processId, rt.processId, timeStamp));
                        
                        //Send Abort Message on the non null routes
                        rt.sendMessage(abortMessage);
                    }
    
                    for (Route rt : serverRoutes) {
                        
                        //If Routes doesnot exist then move on to the next valid route
                        if (rt == null) 
                            continue;
    
                        //Receive the response from the non faulty servers
                        String responseMesssage = rt.receiveMessage();
    
                        //Split the response to derive details
                        String[] responseDetails = responseMesssage.split(":", 2);
    
                        //ACK --> Abort Success
                        //ELSE --> Abort Failure
                        if (responseDetails[0].equals("ACK")) {
                            
                            //Log Abort Success
                            System.out.println(String.format("SUCCESS! Abort Write From Server: %s For Object: %s Successful", rt.processId, selectedObject));
                        }
                        else {

                            //Log Abort Failure
                            System.out.println(String.format("FAILURE! Abort Write From Server: %s For Object: %s Message: %s Failed With Response: %s", rt.processId, selectedObject, abortMessage, responseMesssage));
                        }
            
                        //Close socket
                        rt.closeRoute();
                    }
                }
                
                //Write if enough servers are available
                else {
                    
                    //Frame the Write Message
                    String writeRequest = String.format("CLIENT:%s:WRITE:%s:%s:%s:%s", clnt.processId, selectedObject, writeMessage, timeStamp, replicaServersForObject);
                
                    //Send Write Requests to all the routes
                    for (Route rt : serverRoutes) {    
                        
                        //If not valid route move to the next route
                        if (rt == null)
                            continue;
    
                        //Log Write Progress
                        System.out.println(String.format("Write Message: %s To Object: %s By Client: %s At Server %s In Progress At %s", writeMessage, selectedObject ,clnt.processId, rt.processId, timeStamp));
                        
                        //Send the write Message on the route
                        rt.sendMessage(writeRequest);
                    }
    
                    //Receive Write Responses from all routes
                    for (Route rt : serverRoutes) {
                        
                        //If the route is invalid move on to the next route
                        if (rt == null)
                            continue;
    
                        //Receive the response from the route
                        String responseMessage = rt.receiveMessage();
    
                        //Split the response message to fetch details
                        String[] responseDetail = responseMessage.split(":", 2);
    
                        //ACK --> Success
                        if (responseDetail[0].equals("ACK")) {
                            
                            //Log Write Success
                            System.out.println(String.format("SUCCESS! Write Successful at Server: %s for Object: %s With Message: %s", rt.processId, selectedObject, writeMessage));
                        }

                        //ELSE --> Failure
                        else {
                            System.out.println(String.format("FAILURE! Write Failed At Server: %s for Object: %s With Message %s And Response - %s", rt.processId, selectedObject, writeRequest, responseMessage));
                        }
            
                        //Close socket
                        rt.closeRoute();
                    }
                }
            }
        }

        //Log the read and Write requests Performed
        System.out.println(String.format("Total Requests Performed - Write: %s, Read: %s", countOfWrites, totalNumberOfRequests - countOfWrites));
    }

    //Check if the list of routes for the client are valid and we have been successful in connecting
    public static Integer countValidRoutes(List<? extends Object> rtList) {
        //Variable Declaration
        //Keep count of routes that we have been able to build
        int count = 0;

        //Check for all routes in the list if they are valid
        for (Object route :  rtList) {
            //If valid increment the count of valid routes
            if (route != null) {
                count++;
            }        
        }
        //Return the count of such routes
        return count;
    }

    //Read all the server configurations from the Config File
    public void loadServersFromConfigFile(String file) {
        try {

            //Variable Declaration
            BufferedReader buffReader= new BufferedReader(new FileReader(file));
            String lineRead;
            String[] serverDetails;

            //Log loading of the server details from the config file
            System.out.println("Loading Servers From The Config File");

            //Read line by line from the file and load the contents of each line
            while ((lineRead = buffReader.readLine()) != null) {
                
                //Split the contents to get the detailed server information
                serverDetails = lineRead.split(" ");

                //Log the server information that has been loaded
                System.out.println(String.format("Server ID = %s, IP Address = %s, Port Number = %s", serverDetails[0], serverDetails[1], serverDetails[2]));

                //Add the server information to the server list of the 
                this.listOfServers.add(new Process(serverDetails[0], serverDetails[1], Integer.parseInt(serverDetails[2])));
            }
            //close the file reader
            buffReader.close();
        }
        catch (Exception e) {
            //Catch and Log any exception that occurs while reading from the config file
            System.out.println(String.format("Couldnot Load Servers From Config File: %s", file));
        }  
    }
}
