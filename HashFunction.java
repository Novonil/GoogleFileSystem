//Class containing the Hash function related details
public class HashFunction
{
    //Hash Function Design that Generates Server Numbers from File Names 
    public Integer[] HashServers(String fileName) throws Exception
    {
        //If file name is null throw exception
        if(fileName == null)
        {
            throw new Exception("File Name to be hashed cannot be null");
        }
        //Variable Declaration
        char[] alphabets = fileName.toCharArray();
        Integer[] serverNumbers = new Integer[3];
        int sum = 0;

        //Calculate the total of ASCII values of the file
        for(char c : alphabets)
        {
            sum += (int)c;
        }
        //Server Numbers = Total ASCII % 7, (Total ASCII % 7 + 1) % 7, (Total ASCII % 7 + 2) % 7
        serverNumbers[0] = sum % 7;
        serverNumbers[1] = (serverNumbers[0] + 1) % 7;
        serverNumbers[2] = (serverNumbers[0] + 2) % 7;

        //Return the calculated Servers
        return serverNumbers;
    }
}