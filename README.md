# mongo_benchmark

## Usage
```bash
$ javac -classpath library/*:. mongoprog2.java #Compile
# Syntax
# $ java -classpath library/*:. mongoprog2 <from_db> <from_collection> <destination_db> <destination_collection> <destination_ip> <destination_port> <no_of_records>
$ java -classpath library/*:. mongoprog2 twitter aaron cloud social 172.50.88.22 27020 100 #Example
```

## Notes
* It is assumed that you have a source mongo instance running in your host computer at port 27017. You may want to configure if you have an instance elsewhere.
* Code compatible with Mongo 3.0.6
