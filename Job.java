import java.io.*;
import java.util.*;

//Class containing Job Details
public class Job {
    
    //Variable Declaration
    
    //Requesting Client Information
    String  requestingClientID;

    //Requested Server Information
    String  requestedServerID;

    //File Name to write to
    String  file;

    //Message to Write
    String  msg;                    
    
    //Logical Time Stamp of Job
    public long logicalTimeStamp;  

    //Constructor
    public Job(String clientId, String serverId, String filename, String message, long tstamp) {
        this.requestingClientID = clientId;
        this.requestedServerID = serverId;
        this.file = filename;
        this.msg = message;
        this.logicalTimeStamp = tstamp;
    }

    //Write to the file
    public void execute() throws IOException {
        
        //Open file at the requested server
        PrintWriter writer = new PrintWriter(
            new FileWriter("FileSystem/" + this.requestedServerID + "/" + file+".txt", true)
        );

        //Write the Write Message on the file at the requested server
        writer.println(this.msg);

        //Close the socket
        writer.close();
    }

    //Override the function toString
    @Override
    public String toString() {
        return String.format("(%s:%s:%s:%s)", requestingClientID, logicalTimeStamp, file, msg);
    }

    //Override the equals function
    @Override
    public boolean equals(Object obj) {

        // Return truw if the same object is compared with itself
        if (obj == this) { 
            return true; 
        } 
  
        // If the item being checked is not an instance of Job return false
        if (!(obj instanceof Job)) { 
            return false; 
        } 
        
        Job t = (Job) obj; 
          
        // Job is same if logical timestamp and requesting server are the same  
        return t.logicalTimeStamp == this.logicalTimeStamp && t.requestingClientID.equals(this.requestingClientID);
    }
};

class JobComparator implements Comparator<Job> {
    
    //Arrage Jobs by ascending timestamp, ties broken by ascending requesting client Ids
    public int compare(Job t1, Job t2) { 
        //If logical time stamp differs t1 > t2 return 1
        if (t1.logicalTimeStamp > t2.logicalTimeStamp) {
            return 1;
        }
        //If logical time stamp differs t1 < t2 return -1
        else if (t1.logicalTimeStamp < t2.logicalTimeStamp) {
            return -1; 
        }
        //Break ties based on requesting Client Id
        else {
            //t1 client id > t2 client id return 1
            if (t1.requestingClientID.compareTo(t2.requestingClientID) > 0) {
                return 1;
            }
            //Else return -1
            else {
                return -1;
            }
        }
    }
} 