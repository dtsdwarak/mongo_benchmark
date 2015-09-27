//javac -classpath library/*:. mongoCopy.java
//java -classpath library/*:. mongoCopy

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import org.bson.Document; //For Document type

//Java packages
import java.util.Random;

public class mongoCopy{
  public static void main(String[] args){

    //Trying create a 1-by-1 entry in authors and recrod time

    //Properties for READ Database -  author_backup
    MongoClient mongo_read = new MongoClient();
    MongoCursor<Document> read_cursor = mongo_read.getDatabase("twitter").getCollection("aaron").find().limit(100000).iterator();

    //Properties for WRITE Database - authors
    MongoClient mongo_write = new MongoClient();
    MongoDatabase write_db = mongo_write.getDatabase("backup");

    //Deleting the exisiting WRITE datatbase collection, if any!
    write_db.getCollection("aaron").drop();

    long totaltime=0,starttime,endtime;

    while(read_cursor.hasNext()){
      // System.out.println("writing");
      starttime = System.currentTimeMillis();
      write_db.getCollection("aaron").insertOne(read_cursor.next());
      endtime = System.currentTimeMillis();
      totaltime += endtime - starttime;
    }

    System.out.println("Total time to write = " + (float) totaltime/1000 + " seconds");
    read_cursor.close();


  }
}
