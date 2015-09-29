//javac -classpath library/*:. mongoprog2.java
//java -classpath library/*:. mongoprog2 twitter aaron backup aaron

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import org.bson.Document; //For Document type
import static com.mongodb.client.model.Filters.*;
import org.bson.types.ObjectId; //For ObjectID store

//Java packages
import java.util.concurrent.TimeUnit; //For Time Wait
import java.util.Scanner;
import java.util.ArrayList; //For objectID store
import java.util.Iterator;

//Json Packages
import org.json.JSONObject;

public class mongoprog2{

  //Database parameters
  public static String read_database;
  public static String read_collection;
  public static String write_database;
  public static String write_collection;

  //ObjectID store
  public static ArrayList<ObjectId> object_id_store = new ArrayList<ObjectId>();

  public static void main(String[] args) throws Exception  {

    if(args.length<4){
      System.out.println("Database parameters not given. Quitting");
      return;
    }
    //Assign parameters to Database
    read_database = args[0]; read_collection = args[1]; write_database = args[2]; write_collection = args[3];

    write_to_db();
    TimeUnit.SECONDS.sleep(3);
    read_from_db();
    TimeUnit.SECONDS.sleep(3);
    update_db();
    TimeUnit.SECONDS.sleep(3);
    delete_db_records();

  }

  // WRITE TO DATABASE
  public static void write_to_db(){

    //Properties for READ Database -  author_backup
    MongoClient mongo_read = new MongoClient("172.50.88.22",27020);
    MongoCursor<Document> read_cursor = mongo_read.getDatabase(read_database).getCollection(read_collection).find().limit(1000).iterator();

    //Properties for WRITE Database - authors
    MongoClient mongo_write = new MongoClient("172.50.88.22",27020);
    MongoDatabase write_db = mongo_write.getDatabase(write_database);

    //Deleting the exisiting WRITE datatbase collection, if any!
    write_db.getCollection(write_collection).drop();

    long totaltime=0,starttime,endtime;
    Document temp_read_record; //For the record that was currently read

    System.out.println("Writing to Database. Please Wait.");

    while(read_cursor.hasNext()){
      // System.out.println("writing");
      temp_read_record = read_cursor.next();

      System.out.println(temp_read_record.getObjectId("_id"));

      //Store the "_id" field of the object written into Database for further use
      object_id_store.add(temp_read_record.getObjectId("_id"));

      starttime = System.currentTimeMillis();
      write_db.getCollection(write_collection).insertOne(temp_read_record);
      endtime = System.currentTimeMillis();
      totaltime += endtime - starttime;

    }

    System.out.println("Total time to write = " + (float) totaltime/1000 + " seconds");
    read_cursor.close();
    // Writing Queries done

  }

  // READ FROM DATABASE
  public static void read_from_db(){

    MongoClient mongo_read = new MongoClient("172.50.88.22",27020);
    MongoCollection<Document> mongo_read_collection = mongo_read.getDatabase(write_database).getCollection(write_collection);


    long totaltime=0,starttime,endtime;

    Scanner in = new Scanner(System.in);
    int choice;
    System.out.print("\n\n Reading from Database\n 1. 1-by-1\n 2. Bulk\n Enter your choice : ");
    choice = in.nextInt();

    switch(choice){
      case 1: System.out.println("Reading Database 1-BY-1");
              MongoCursor<Document> read_cursor = mongo_read_collection.find(new Document("_id",new Document("$in",object_id_store))).iterator();
              while(read_cursor.hasNext()){
                starttime = System.currentTimeMillis();
                JSONObject JSON_DATA = new JSONObject (read_cursor.next().toJson());
                System.out.println("Value = "+ JSON_DATA.getJSONObject("user").getString("name"));
                endtime = System.currentTimeMillis();
                totaltime += endtime - starttime;
              }
              read_cursor.close();
              System.out.println("Total Time to read records 1-BY-1 = " + (float) totaltime/1000 + " seconds");
              break;
      case 2: System.out.println("Reading Database in BULK");
              starttime = System.currentTimeMillis();
              FindIterable<Document> iterable = mongo_read_collection.find(new Document("_id",new Document("$in",object_id_store)));
              endtime = System.currentTimeMillis();
              iterable.forEach(new Block<Document>() {
                  @Override
                  public void apply(final Document document) {
                      JSONObject JSON_DATA = new JSONObject (document.toJson());
                      System.out.println("Value = "+ JSON_DATA.getJSONObject("user").getString("name"));                  }
              });
              System.out.println("Total Time to read records BULK = " + (float) (endtime - starttime)/1000 + " seconds");
              break;
    default:  System.out.println("\n Wrong choice");
    }

  //Reading Queries done
  }

  // UPDATE THE DATABASE
  public static void update_db(){

    MongoClient mongo_read = new MongoClient("172.50.88.22",27020);
    MongoCollection<Document> update_collection = mongo_read.getDatabase(write_database).getCollection(write_collection);

    Scanner in = new Scanner(System.in);
    int choice;
    System.out.print("\n\n Database Update\n 1. 1-by-1\n 2. Bulk\n 3. Replace\n Enter your choice : ");
    choice = in.nextInt();

    long starttime,endtime;

    //Iterator for object_id_store
    Iterator<ObjectId> object_iterator = object_id_store.iterator();


    switch(choice){
      case 1:  System.out.println("Updating Database 1-BY-1");
               starttime = System.currentTimeMillis();

               while(object_iterator.hasNext()){
                 update_collection.updateOne( new Document("_id",object_iterator.next()), new Document("$set",new Document("id","NumberLong(100101102103104105)")) );
               }

              //  while(update_collection.updateOne( new Document("_id",new Document("$in",object_id_store)), new Document("$set",new Document("id","NumberLong(100101102103104105)")) ).getModifiedCount()!=0);
               endtime = System.currentTimeMillis();
               System.out.println("Total Time to update records 1-by-1 = " + (float) (endtime-starttime)/1000 + " seconds");
               break;
      case 2:  System.out.println("Updating Database BULK");
               starttime = System.currentTimeMillis();
               update_collection.updateMany( new Document("_id",new Document("$in",object_id_store)), new Document("$set", new Document("id","NumberLong(100101102103104105)")) );
               endtime = System.currentTimeMillis();
               System.out.println("Total Time to update all records by BULK = " + (float) (endtime-starttime)/1000 + " seconds");
               break;
      case 3:  System.out.println("Updating Database by REPLACE");
               starttime = System.currentTimeMillis();

               while(object_iterator.hasNext()){
                 update_collection.replaceOne( new Document("_id",object_iterator.next()), new Document("$set",new Document("id","NumberLong(100101102103104105)")) );
               }

              //  while(update_collection.replaceOne( new Document("id", new Document("$ne","NumberLong(100101102103104105)")), new Document("id","NumberLong(100101102103104105)") ).getModifiedCount()!=0);
               endtime = System.currentTimeMillis();
               System.out.println("Total Time to update all records by REPLACE = " + (float) (endtime-starttime)/1000 + " seconds");
               break;
      default: System.out.println("Wrong choice");
    }

  }

  // DELETE DATABASE
  public static void delete_db_records(){

    MongoClient mongo_read = new MongoClient("172.50.88.22",27020);
    MongoCollection<Document> delete_collection = mongo_read.getDatabase(write_database).getCollection(write_collection);

    Scanner in = new Scanner(System.in);
    int choice;
    System.out.print("\n\n Database Delete Operations\n 1. 1-by-1\n 2. Bulk\n Enter your choice : ");
    choice = in.nextInt();

    long starttime,endtime;

    switch(choice){
      case 1: System.out.println("Delete Database 1-BY-1");
              starttime = System.currentTimeMillis();

              Iterator<ObjectId> object_iterator = object_id_store.iterator();
              while(object_iterator.hasNext()){
                delete_collection.deleteOne( new Document("_id",object_iterator.next()) );
              }

              // while(delete_collection.deleteOne(new Document()).getDeletedCount()!=0);
              endtime = System.currentTimeMillis();
              System.out.println(" Total time to delete all records 1-BY-1 = " + (float) (endtime-starttime)/1000 + " seconds");
              break;
      case 2: System.out.println("Delete Database BULK");
              starttime = System.currentTimeMillis();
              delete_collection.deleteMany(new Document("_id",new Document("$in",object_id_store)));
              endtime = System.currentTimeMillis();
              System.out.println(" Total time to delete all records BULK = " + (float) (endtime-starttime)/1000 + " seconds");
              break;
    default:  System.out.println("Wrong choice");

    }

  }


}
