# BigData Real-Time Streaming: Amazon Product Rating Pipeline

## Overview

A real-time data streaming pipeline that processes Amazon product reviews using **Apache Kafka** and **Apache Spark Structured Streaming**, then stores aggregated results in **MySQL**.

The pipeline reads consumer reviews of Amazon products from a CSV dataset, streams each review through Kafka, and uses Spark to compute a running weighted average rating per product category -- all in real time.

## How It Works

1. **Data Cleaning** (`project.ipynb` -- Cell 1): Loads the raw dataset (`Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products.csv`), selects relevant columns (`primaryCategories`, `reviews.rating`, `reviews.text`, `reviews.title`, `reviews.username`), drops rows with missing values, and saves the result as `Cleaned_Reviews.csv`.

2. **Kafka Producer** (`project.py` / `project.ipynb` -- Cell 2): Reads `Cleaned_Reviews.csv` and sends each review as a JSON message (`primaryCategories`, `reviews_rating`) to the Kafka topic `product-reviews`, one record per second.

3. **Spark Streaming Consumer** (`spark.py`): Subscribes to `product-reviews` via Spark Structured Streaming, parses the JSON messages, groups by `primaryCategories`, computes a running average rating, and writes each micro-batch to the MySQL table `average_ratings` using a weighted average to preserve historical accuracy.

## Project Structure

| File / Folder | Description |
|---|---|
| `project.ipynb` | Notebook for data cleaning and interactive Kafka producer |
| `project.py` | Standalone Kafka producer script |
| `spark.py` | Spark Structured Streaming consumer + MySQL writer |
| `Cleaned_Reviews.csv` | Cleaned review data (generated from notebook) |
| `Datafiniti_Amazon_Consumer_Reviews_of_Amazon_Products.csv` | Raw Amazon reviews dataset |
| `kafka_2.12-2.8.0/` | Local Kafka installation |
| `mysql-connector-j-9.1.0/` | MySQL JDBC connector for Spark-to-MySQL writes |

## Prerequisites

| Tool | Version |
|---|---|
| Java JDK | 8 |
| Apache Spark | 3.2.4 |
| Hadoop | 2.7 |
| PySpark | 3.2.4 |
| Python | 3.9 |
| Apache Kafka | 2.8.0 |
| MySQL | Any recent version |
| MySQL Connector/J | 9.1.0 (included) |

Python packages: `kafka-python`, `pandas`, `mysql-connector-python`

## Quick Start

```bash
# 1. Start Zookeeper
bin\windows\zookeeper-server-start.bat config\zookeeper.properties

# 2. Start Kafka Broker
bin\windows\kafka-server-start.bat config\server.properties

# 3. Make sure MySQL is running and the database/table exist
#    Database: reviewsdb
#    Table: average_ratings (primaryCategories VARCHAR PK, reviews_rating DOUBLE, total_reviews INT)

# 4. Run the Kafka Producer (sends review data to the topic)
python project.py

# 5. In a separate terminal, run the Spark consumer
#    (make sure mysql-connector-j-9.1.0/ folder is in the same directory as spark.py)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 --jars mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar spark.py
```

## MySQL Table Schema

```sql
CREATE TABLE average_ratings (
    primaryCategories VARCHAR(255) PRIMARY KEY,
    reviews_rating DOUBLE,
    total_reviews INT
);
```

---
---

# Old README (Architecture / Command Reference)

# kafka-spark_task
**what we need to install **

Java jdk 8

Spark 3.2.4

Hadoop 2.7 (just file "winutils")

PySpark 3.2.4

Python 3.9

Kafka  2.8.0 (scala version 2.12)

Mysql 

Mysql connector jdbc (java mysql integration to allow write to Mysql)

<br> </br>

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







