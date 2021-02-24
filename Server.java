import java.net.*;
import java.rmi.UnexpectedException;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.security.*;
import java.time.Instant;
import java.util.concurrent.*;
import javax.naming.NameNotFoundException;

//Server Class that inherits from  Process
public class Server extends Process {

    //Variable Declaration
    //Server Socket
    private static ServerSocket srvrSock;

    //Maps Server Ids with the Server Details
    Map<String, Process> idToProcessDetails = new HashMap<String, Process>();

    //Maps files with Jobs
    public Map<String, Object> lockObject;
    public Map<String, Job> jobToLockedObject;

     //Job Queue
     Map<String, PriorityBlockingQueue<Job>> jobQueue = new ConcurrentHashMap<String, PriorityBlockingQueue<Job>>(Process.listOfFiles.length);
    
    //Blank job
    public static Job NULL_JOB = new Job(null, null, null, null, (long)0);

    //Constructor
    public Server(String processId, String ipAddress, int portNumber) {

        //Set Process Details for the Server
        super(processId, ipAddress, portNumber);

        //Job File Map
        this.lockObject = new ConcurrentHashMap<String, Object>(100);
        this.jobToLockedObject = new ConcurrentHashMap<String, Job>(100);

        for (String file : Process.listOfFiles) {
            
            //Initialize the Queue with the files and initially have place for 30 Jobs per file 
            this.jobQueue.put(file, new PriorityBlockingQueue<Job>(30, new JobComparator()));
            
            //Put all file names in the Map
            this.lockObject.put(file, new Object());
        }
    }

    //Add other server details from the config file so as to be able to connect to the servers
    public void loadOtherServerDetailsFromConfig(String fileName) throws FileNotFoundException, IOException {
        
        //Variable Declaration
        BufferedReader inpBuff = new BufferedReader(new FileReader(fileName));
        String lineRead;
        String[] serverDetails;

        //Log Loading Server
        System.out.println("Loading Servers From Config File");

        //Read each line until we reach end of file
        while ((lineRead = inpBuff.readLine()) != null) {
            
            //Split the lines to get the server details
            serverDetails = lineRead.split(" ");

            //Check if the process on that line is the same as this process 
            if (!serverDetails[0].equals(this.processId)) { 
                
                //Log Server Details
                System.out.println(String.format("Found Server Process ID = %s, IP Address = %s, Port Number = %s", serverDetails[0], serverDetails[1], serverDetails[2]));
                
                //Load all process details other than itself
                this.idToProcessDetails.put(serverDetails[0], new Process(serverDetails[0], serverDetails[1], Integer.parseInt(serverDetails[2])));
            }
        }
        //CLose Buffer
        inpBuff.close();
    }

    
    // Program entry point for server
    // args[0] ServerID that identifies the server uniquely
    // args[1] IPAddress of the server
    // args[2] PortNumber on the server
    
    public static void main(String[] args) throws IOException {
        
        //Maximum size of thread pool
        int MAX_THREAD_POOL_SIZE = 7;

        //Check if all arguments are available
        if (args.length != 3) {
            //Throw Exception if all parameters are not available
            throw new InvalidParameterException("required parameters <servername> <ip> <port>");
        }

        //Server Instance for itself and store server configurations
        Server ownServer = new Server(args[0], args[1], Integer.parseInt(args[2]));

        //Current System Time
        Instant instant = Instant.now();

        //Log Server Start Time
        System.out.println(String.format("Server: %s Starts At Time: %s", ownServer.processId, instant.toEpochMilli()));

        //Load all other servers
        ownServer.loadOtherServerDetailsFromConfig("config.txt");

        //Spawn thread
        final ExecutorService service = Executors.newFixedThreadPool(MAX_THREAD_POOL_SIZE);

        //Create the server Socket
        Server.srvrSock = new ServerSocket(ownServer.portNumber);

        while (true) {
            //Listen and accept incoming connection requests
            Socket clntSocket = Server.srvrSock.accept();

            //Create a route between client and server
            Route clientRoute = new Route(clntSocket);

            //Log Connection Request Received
            System.out.println(String.format("Received Connection Request From IP Address = %s, Port Number = %s",clntSocket.getInetAddress(), clntSocket.getPort()));

            requestHandler callobject = new requestHandler(
                clientRoute,
                ownServer
            );

            //Spawn thread to handle client connection request
            service.submit(callobject);
        }
    }
}

class requestHandler implements Callable<Integer> {
    
    //Variable Declaration
    private Route requesterProcessRoute;
    Server ownr;
    String requesterProcessId,
        requesterProcessType;

    //Constructor
    public requestHandler(Route rt, Server owner) {
        this.requesterProcessRoute = rt;
        this.ownr = owner;
    }

    //Read File as requested by Client
    public String ReadFile(String file) throws FileNotFoundException, IOException
    {
        StringBuffer contents = new StringBuffer();
        
        //Variable Declaration
        BufferedReader inputFileBuffer = new BufferedReader(new FileReader("FileSystem/" + this.ownr.processId + "/" + file+".txt"));
        String newLine;

        //Read the File requested by the client
        while ((newLine = inputFileBuffer.readLine()) != null) 
        {
            contents.append(newLine);
        }
        //Close the file reader buffer
        inputFileBuffer.close();
        return contents.toString();
    }

    
    //Entry point for each thread
    public Integer call() throws IOException, FileNotFoundException {
        
        //Request Message
        String requestMessage = this.requesterProcessRoute.receiveMessage();

        //Details of the Request Message
        String[] requestDetails = requestMessage.split(":");

        //Split components of the request
        this.requesterProcessType = requestDetails[0];
        this.requesterProcessId = requestDetails[1];
        String mode = requestDetails[2];
        String file = requestDetails[3];

        //Check if the requestor is Client
        if (this.requesterProcessType.equals("CLIENT")) {

            //Log Client Request
            System.out.println(String.format("Request Received From Client With Message %s", requestMessage));

            String contentOfFile;
            
            //Check if Mode = READ
            if (mode.equals("READ")) {
                try {
                    
                    //Call Client Read Request Handler
                    contentOfFile = this.clientReadRequestHandler(file);
                    
                    //Log Read Successful
                    System.out.println(String.format("SUCCESS! Server %s Sends a Successful ACK To Client %s", this.ownr.processId, this.requesterProcessId));
    
                    //Send Successful Read acknowledgement to client
                    this.requesterProcessRoute.sendMessage(String.format("ACK:%s", contentOfFile));
                }
                catch (FileNotFoundException ex) {
                    
                    //Log Error Message for File Not Found
                    System.out.println(String.format("ERR: Object %s Not Found At Server %s %s", file, this.ownr.processId, ex.getMessage()));
    
                    //Send Error Message to client
                    this.requesterProcessRoute.sendMessage(String.format("ERR: Object %s Not Found At Server %s", file, this.ownr.processId));
                    
                    //Return Failed
                    return 0;
                }
                catch (IOException ex) {

                    //Log Error Message Unable to Read File
                    System.out.println(String.format("ERR: Could Not Read Object %s At Server %s %s", file, this.ownr.processId, ex.getMessage()));
    
                    //Send Error Message to the Client
                    this.requesterProcessRoute.sendMessage(String.format("ERR: Could Not Read Object %s At Server %s", file, this.ownr.processId));
    
                    return 0;
                }
                catch (Exception ex) {

                    //Log Error Message
                    System.out.println(ex.getMessage());
    
                    //Send Error Message to the Client
                    this.requesterProcessRoute.sendMessage(String.format("ERR: %s", ex.getMessage()));
     
                    //Return Failed
                    return 0;
                }
            }
            //Check if Mode = WRITE
            else if (mode.equals("WRITE")) {
                try {

                    //Call Client Write Request Handler
                    this.clientWriteRequestHandler(file, requestDetails[4], Long.parseLong(requestDetails[5]), requestDetails[6].split(","));
  
                    //Log Successful Write ACK Send to Client
                    System.out.println(String.format("SUCCESS! Sending Successful Acnowledgement FROM Server %s To client %s", this.ownr.processId, this.requesterProcessId));
    
                    //Send Successful Write ACK to client
                    this.requesterProcessRoute.sendMessage("ACK");
                }
                catch (IOException ex) {

                    //Log Error Send to Client
                    System.out.println(String.format("ERR: Failed to Write to Object %s at Server %s %s", file, this.ownr.processId, ex.getMessage()));
    
                    //Send Error Message to the Client
                    this.requesterProcessRoute.sendMessage(String.format("ERR: Write Failed for Object %s at Server %s", file, this.ownr.processId));
    
                    //Return Failed
                    return 0;
                }
                catch (Exception ex) {

                    //Log Error Send to Client
                    System.out.println(ex.getMessage());
                    
                    //Send Error Message to the Client
                    this.requesterProcessRoute.sendMessage(String.format("ERR: %s", ex.getMessage()));
                    
                    //Return Failed
                    return 0;
                }
            }

            //Check if Mode = ABORT
            else if (mode.equals("ABORT")) {
                try {

                    //Log Successful ABORT ACK Send to Client
                    System.out.println(String.format("server %s sends a successful abort ack to client %s", this.ownr.processId, this.requesterProcessId));
    
                    //Send acknowledgement to client for successful write to object
                    this.requesterProcessRoute.sendMessage("ACK");
                }
                catch (Exception ex) {
                    
                    //Log Error Send to Client
                    System.out.println(ex.getMessage());
    
                    //Send Error Message to CLient
                    this.requesterProcessRoute.sendMessage(String.format("ERR: %s", ex.getMessage()));
    
                    //Return Failed
                    return 0;
                }
            }
        }

        //Check if it is a Server Request
        else if (this.requesterProcessType.equals("SERVER")) {
            
            //Log Server Request Received
            System.out.println(String.format("Request Received From Server With Message %s", requestMessage));

            //Check if Mode = VOTE
            if (mode.equals("VOTE")) {
                try {
                    
                    //Call Server Vote Request Handler
                    this.serverVoteRequestHandler(file, requestDetails[4], Long.parseLong(requestDetails[5]));
    
                    //Log Vote Processed
                    System.out.println(String.format("Vote For Request %s Has Been Processed", requestMessage));
                }
                catch (Exception ex) {
                    //Log Error During Vote Processing
                    System.out.println(String.format("ERR: Failed While Processing Vote Request %s With Message %s", requestMessage, ex.getMessage()));

                    //Send ERR back to the server
                    this.requesterProcessRoute.sendMessage(String.format("ERR: Failed While Processing Vore %s", ex.getMessage()));
                    
                    //Return Failed
                    return 0;
                }    
            }

            String response = this.requesterProcessRoute.receiveMessage();
            //Check if Mode = RELEASE
            if (response.equals("RELEASE")) {
                try {

                    //Call Server
                    this.serverReleaseRequestHandler(file, requestDetails[4], Long.parseLong(requestDetails[5]));

                    //Log Release for request
                    System.out.println(String.format("Release For Request %s Confirmed", requestMessage));
                }
                catch (Exception ex) {
                    
                    //Log Error While Releasing
                    System.out.println(String.format("ERR: Failed while handling release request %s With %s", this.requesterProcessId, ex.getMessage()));
    
                    //Send Error While Release
                    this.requesterProcessRoute.sendMessage(String.format("ERR: %s", ex.getMessage()));
    
                    //Return Failed
                    return 0;
                } 
            }
        }
        //Return Success
        return 1;
    }

    private String clientReadRequestHandler(String fileName) throws FileNotFoundException, IOException {
        //Call Read From File
        return ReadFile(fileName);
    }

    private void clientWriteRequestHandler(String fileName, String writeMessage, long timeStamp, String[] replicaServers) throws IOException, InterruptedException {
        
        //Create New Job for the Write Request
        Job jb = new Job(this.requesterProcessId, this.ownr.processId, fileName, writeMessage, timeStamp);

        //Add Job to the Priority Blocking Queue for the file
        this.ownr.jobQueue.get(fileName).add(jb);

        boolean writeCompleted = false;

        // Keep trying until task succeeds
        while (!writeCompleted) {

            //Sleep Thread Till Job reaches Head of the Queue
            while (!this.ownr.jobQueue.get(fileName).peek().equals(jb)) {
                Thread.sleep(10);
            }

            //Lock the File Object
            synchronized(this.ownr.lockObject.get(fileName)) {
                
                List<Route> srvrRoutes = new ArrayList<>();

                //Store task in locked variable for obj
                this.ownr.jobToLockedObject.put(fileName, jb);

                //Broadcast Votes to All Replica servers
                for (String srvrId : replicaServers) {

                    //Ignore the copy of itself
                    if (srvrId.equals(this.ownr.processId))
                        continue;

                    //Get Process details of the current replica server
                    Process chosenServer = this.ownr.idToProcessDetails.get(srvrId);

                    try {
                        //Create a route to that server
                        Route rt = new Route(chosenServer.ipAddress, chosenServer.portNumber, chosenServer.processId);

                        //Send Vote Request Message to the replica server
                        rt.sendMessage(String.format("SERVER:%s:VOTE:%s:%s:%s", this.ownr.processId, fileName, this.requesterProcessId, timeStamp));

                        //Maintain all such routes
                        srvrRoutes.add(rt);
                    }
                    catch (IOException ex) {
                        //Log Failure
                        System.out.println(String.format("FAILURE! Connect to Server %s For Voting Job %s Failed", chosenServer.processId, jb));
                    }
                }

                int countOfVotes = 0;
                boolean requestRejected = false;

                //Wait for Responses From Replicas
                for (Route rt : srvrRoutes) {

                    //Receive Responses from the replicas
                    String responseRequest = rt.receiveMessage();

                    //Response Details
                    String[] responseDetails = responseRequest.split(":");

                    //If Error Response is received
                    if (responseDetails[0].equals("ERR")) {
                        //Log Error in VOting
                        System.out.println(String.format("Server %s Failed To Process Vote For Job %s", rt.processId, jb));
                    }
                    //If ACK received as Response
                    else if (responseDetails[0].equals("ACK")) {
                        //If Accepted
                        if (responseDetails[1].equals("ACCEPT")) {
                            
                            //Increase Vote Count
                            countOfVotes++;

                            //Log Acceptance of Vote
                            System.out.println(String.format("Accept Received From %s For Job %s", rt.processId, jb));
                        }
                        else if (responseDetails[1].equals("REJECT")) {
                            //Set flag to true
                            requestRejected = true;

                            //Log Rejection of Vote
                            System.out.println(String.format("Reject Received From %s For Job %s", rt.processId, jb)); 
                        }
                    }
                }
                
                //Rejection by Any Replica
                if (requestRejected) {
                    //Log Rejection
                    System.out.println(String.format("Job %s Rejected, Removing Lock", jb));

                    for (Route rt : srvrRoutes) {
                        
                        //Log Sending Reject ACK
                        System.out.println(String.format("Sending Reject ACK for Job %s", jb));

                        //Send Reject ACK
                        rt.sendMessage("ACK:REJECT");
                    }

                    // Unlock and retry. Note that retry happens by default until executed = true
                    this.ownr.jobToLockedObject.remove(fileName);
                }
                else if (countOfVotes >= 1) { // If enough replicas ACCEPT
                    System.out.println(String.format("task %s accepted, executing...", jb));

                    // Perform write
                    jb.execute();

                    // Remove task from queue
                    this.ownr.jobQueue.get(fileName).remove(jb);

                    
                    for (Route rt : srvrRoutes) {
                        
                        System.out.println(String.format("sending release for task %s", jb));

                        rt.sendMessage("RELEASE");
                    }

                    // Get Ack from all reachable replicas
                    for (Route rt : srvrRoutes) {
                        System.out.println(String.format("waiting for release ack for task %s", jb));

                        String response = rt.receiveMessage();

                        if (!response.equals("ACK:RELEASE")) {
                            System.out.println(String.format("failed ack response from server %s", rt.processId));
                        }

                        rt.closeRoute();
                    }

                    // Release lock
                    this.ownr.jobToLockedObject.remove(fileName);

                    //Set Write Complete to true and exit out of loop
                    writeCompleted = true;
                }
            }
        }
    }

    //Handle Server Vote Request
    private void serverVoteRequestHandler(String fileName, String jobOwnerProcess, long timeStamp) throws InterruptedException {

        //Create New Job for Voting Requests
        Job voteJob = new Job(jobOwnerProcess, null, null, null, timeStamp);

        //Keep doing this until the Job is ACCEPTed or REJECTed
        while (true) {

            //Fetch the Locked Job for the file
            Job lckdJob = this.ownr.jobToLockedObject.get(fileName);

            //Check if job exists if not wait
            if (lckdJob == null) {
                Thread.sleep(10);

                continue;
            }

            //Check if job exists
            if (lckdJob.equals(voteJob)) {

                //Log Voting ACK 
                System.out.println(String.format("Send ACK upon Aceepting Vote For Job %s", voteJob));

                //Send ACK for Voting Request
                this.requesterProcessRoute.sendMessage("ACK:ACCEPT");

                break;
            }

            // Check if task being voted is behind earliest task in queue, if yes send ACK:REJECT
            Job earliestJob = this.ownr.jobQueue.get(fileName).peek();

            //Check for the earliest job for the file
            if (earliestJob == null || earliestJob.logicalTimeStamp < voteJob.logicalTimeStamp || (earliestJob.logicalTimeStamp == voteJob.logicalTimeStamp && earliestJob.requestingClientID.compareTo(voteJob.requestingClientID) < 0)) {
            
                //Log vote reject
                System.out.println(String.format("Send ACK upon Rejecting Vote For Job %s", voteJob));
                
                //Send Vote Reject
                this.requesterProcessRoute.sendMessage("ACK:REJECT"); 

                break;
            }

            //Sleep and try again until accept or reject
            Thread.sleep(10);
        }
    }

    //Handle Server Release Requests
    private void serverReleaseRequestHandler(String fileName, String taskOwnerProcess, long timeStamp) {
        
        //Create New Job for Release Request
        Job releaseJob = new Job(taskOwnerProcess, null, null, null, timeStamp);

        //Log Release Request Progress
        System.out.println(String.format("Release Request in Progress for Job %s", releaseJob));

        //Release Task Must be present in the queue to proceed
        while (this.ownr.jobQueue.get(fileName).contains(releaseJob))
            continue;

        //Log Release Success
        System.out.println(String.format("SUCCESS! Sending Release ACK for Job %s", releaseJob));
        
        //Send Release ACK
        this.requesterProcessRoute.sendMessage("ACK:RELEASE");
    }
}