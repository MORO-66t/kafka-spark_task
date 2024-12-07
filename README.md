# kafka-spark_task
**what we need to install **

Java jdk 8
Spark 3.2.4
Hadoop 2.7
PySpark 3.2.4
Python 3.9
Kafka  2.8
Mysql 
Mysql connector jdbc (java mysql integration to allow write to Mysql)


how to work all these together 👍
1- Navigate to Kafka Directory:

// ex D:\FHCAI\Bigdata\kafka_2.12-2.8.0

2 - srart zookeper 
```
 bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```
3 = sratr kafka server (or start broker )
```
bin\windows\kafka-server-start.bat config\server.properties
```
4 - start mysql and login to the user have database and tale we  want to write in 

5 - start kafka producer topic and send streaming data
* code in project.ipynb *

6 - start spark consumer script with kafka scala java connector and mysql connector\j
### please go to spark.py directory and make sure that mysql-connector-j-9.1.0 folder there 
```
 spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 --jars mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar spark.py
```
## congratulation if you still here 😅




### ** kafka useful commands**

### ** Start Zookeeper**

Run Zookeeper first (Kafka depends on it):

```
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

```

### ** Start Kafka Broker**

Start Kafka after Zookeeper is running:

```
bin\windows\kafka-server-start.bat config\server.properties

```

---

### **3. Kafka Operations in CMD**

### **Create a Topic**

Create a new topic called `test-topic`:

```
bin\windows\kafka-topics.bat --create --topic test-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

```

### **List Topics**

List all existing topics:

```
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

```

### **Describe a Topic**

Get details about a topic:

```
bin\windows\kafka-topics.bat --describe --topic test-topic --bootstrap-server localhost:9092

```

---

### **4. Send and Read Messages**

### **Send Messages (Producer)**

Use the producer tool to send messages to a topic:

```
bin\windows\kafka-console-producer.bat --topic test-topic --broker-list localhost:9092

```

Type your message in CMD and press **Enter**.

Example:

```
Hello Kafka!
{"key": "value"}

```

### **Read Messages (Consumer)**

Start a consumer to read messages from a topic:

```
bin\windows\kafka-console-consumer.bat --topic test-topic --bootstrap-server localhost:9092 --from-beginning

```

You’ll see the messages sent earlier.

---

### **5. Stop Kafka Services**

### **Stop Kafka Broker**

Close the terminal or press **Ctrl + C** in the CMD running Kafka Broker.

### **Stop Zookeeper**

Close the terminal or press **Ctrl + C** in the CMD running Zookeeper.

---

### **6. Additional Commands**

### **Delete a Topic**

If deletion is enabled in Kafka, you can delete a topic:

```
bin\windows\kafka-topics.bat --delete --topic test-topic --bootstrap-server localhost:9092

```

### **Check Broker Versions**

```
bin\windows\kafka-broker-api-versions.bat --bootstrap-server localhost:9092

```







