//javac -classpath library/*:. mongoapp.java
//java -classpath library/*:. mongoapp

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;


import org.bson.Document; //For creating a json/bson document

//For Inserting values
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

//For Regex
import java.util.regex.Pattern;

//Fetching values from db
import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;

//For Iterable size
import com.google.common.collect.Iterables;

public class mongoapp
{
    public static void main( String[] args ) throws ParseException
    {

      MongoClient mongoClient = new MongoClient();
      MongoDatabase db = mongoClient.getDatabase("authors");


      // Insert value into a collection
      // ******************************
      // DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
      // db.getCollection("restaurants").insertOne(
      //           new Document("address",
      //                   new Document()
      //                           .append("street", "2 Avenue")
      //                           .append("zipcode", "10075")
      //                           .append("building", "1480")
      //                           .append("coord", asList(-73.9557413, 40.7720266)))
      //                   .append("borough", "Manhattan")
      //                   .append("cuisine", "Italian")
      //                   .append("grades", asList(
      //                           new Document()
      //                                   .append("date", format.parse("2014-10-01T00:00:00Z"))
      //                                   .append("grade", "A")
      //                                   .append("score", 11),
      //                           new Document()
      //                                   .append("date", format.parse("2014-01-16T00:00:00Z"))
      //                                   .append("grade", "B")
      //                                   .append("score", 17)))
      //                   .append("name", "Vella")
      //                   .append("restaurant_id", "41704620"));

      // View value fetched from a collection
      // ************************************
      // FindIterable<Document> iterable = db.getCollection("prices").find(new Document("name","pen"));
      MongoCollection<Document> values = db.getCollection("authordump");
      System.out.println("Total number of entries in the collection = " + values.count());
      // FindIterable<Document> iterable = values.find(new Document("$or", asList(new Document("name","pen"), new Document("name","bottle"))  ));
      FindIterable<Document> iterable = values.find(new Document("name",java.util.regex.Pattern.compile("John",Pattern.CASE_INSENSITIVE)));
      System.out.println("Iterable size is = "+ Iterables.size(iterable));

      // DBCollection coll = db.getCollection("authordump");
      // MongoCursor<Document> cursor = values.find(new Document("name",java.util.regex.Pattern.compile("John",Pattern.CASE_INSENSITIVE))).iterator();

      // System.out.println("\nCursor size = " + cursor.Count());

      // if(iterable instanceof MongoCollection){
      //   System.out.println("Not entering this loop at all!");
      //   System.out.println("\nTotal entries found = "+ ((MongoCollection)iterable).count());
      // }

      // iterable.forEach(new Block<Document>() {
      //     // @Override
      //     public void apply(final Document document) {
      //         System.out.println(document);
      //     }
      // });


      System.out.println("Connected to Database");
    }
}
