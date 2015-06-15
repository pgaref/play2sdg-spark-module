# play2sdg-spark-module

==== BE careful with Spark and Cassandra Connectior Version! ===
![Version Compatibility](http://i.stack.imgur.com/wNVfN.png)


## Million Song DataSet Used
http://labrosa.ee.columbia.edu/millionsong/lastfm


========TODO: Solve Mesos lib dependency =======

## Project Jar usage:
* java -cp target/play2sdg-Spark-module-0.0.1-SNAPSHOT-driver.jar main.java.uk.ac.imperial.lsds.play2sdg.PrepareData
* java -jar target/play2sdg-Spark-module-0.0.1-SNAPSHOT-driver.jar
* ./bin/spark-submit --master mesos://wombat30.doc.res.ic.ac.uk:5050 --class main.java.uk.ac.imperial.lsds.play2sdg.SparkCollaborativeFiltering ../play2sdg-Spark-module-0.0.1-SNAPSHOT-driver.jar
