# Kafka-telsol
A NTUA IT Infrastructure Design Class project...
JDBC Sink instructions adapted from this [video](https://www.youtube.com/watch?v=b-3qN_tlYR4).



## Linux Installation Instructions

### 1. Install Docker

1. Select your server (i.e. Debian, Fedora, Ubuntu) from [here](https://docs.docker.com/engine/install/#server).
2. Follow the instructions on sections `SET UP THE REPOSITORY` and `INSTALL DOCKER ENGINE`.


### 2. Clone this repo

```bash
git clone https://github.com/AthinaKyriakou/kafka-telsol.git
cd kafka-telsol
```


## Start Playing

Open a terminal into the *kafka-telsol* directory.

### 1. Start your containers with Docker Compose

1. Run Docker Compose on a terminal. The first time it runs for long...
```bash
docker-compose up -d
```

2. Make sure that everything is up and running
```bash
docker-compose ps
```
| Container name  | Command                        | State        | Ports                             |
|:----------------|:-------------------------------|:-------------|:----------------------------------|
| broker          | /etc/confluent/docker/run      | Up           | 0.0.0.0:9092->9092/tcp            |
| kafka-connect   | bash -c cd /usr/share/conf ... | Up (healthy) | 0.0.0.0:8083->8083/tcp, 9092/tcp  |
| kafkacat        | /bin/sh -c apk add jq;         | Up           |                                   |
|                 | wh ...                         |              |                                   |
| ksqldb          | /usr/bin/docker/run            | Up           | 0.0.0.0:8088->8088/tcp            |
| mysql           | docker-entrypoint.sh mysqld    | Up           | 0.0.0.0:3306->3306/tcp, 33060/tcp |
| schema-registry | /etc/confluent/docker/run      | Up           | 0.0.0.0:8081->8081/tcp            |
| zookeeper       | /etc/confluent/docker/run      | Up           | 2181/tcp, 2888/tcp, 3888/tcp      |


### 2. Check the installation of the MySQL JDBC driver (optional)

#### a. Find the location of the JDBC plugin
```bash
docker-compose logs kafka-connect|grep kafka-connect-jdbc|more
```
Copy path: **INFO Loading plugin from: <path>**

#### b. Get into the kafka-connect container and cd into the found path
```bash
docker exec -it kafka-connect bash
cd <path>
ls
```
Within this folder there needs to be the **mysql-connector-java-8.0.23.jar** of the JDBC driver.


### 3. Producer: Create Kafka topics and publish data

#### a. From the customized Java producerApp

Adapted from [here](https://kafka-tutorials.confluent.io/creating-first-apache-kafka-producer-application/kafka.html).

Get in the producerApp folder:
```bash 
cd producerApp
```

When you run the producer for the **first** time:
```bash 
gradle wrapper
```

Compile the producerApp: 
```bash 
./gradlew shadowJar
```

Run the producerApp: 
```bash 
java -jar build/libs/kafka-producer-application-standalone-0.0.1.jar
```

#### b. In a new terminal start ksqldb (to interface with Kafka)

In new terminal start ksqldb: 
```bash 
docker exec -it ksqldb ksql http://ksqldb:8088
```

Create the test01 topic using Apache Avro to manage the schema (alternative JSON)
```bash
CREATE STREAM TEST01 (COL1 INT, COL2 VARCHAR)
  WITH (KAFKA_TOPIC='test01', PARTITIONS=1, VALUE_FORMAT='AVRO');
```

Insert dummy data to test01 topic
```bash
INSERT INTO TEST01 (ROWKEY, COL1, COL2) VALUES ('X',1,'FOO');
INSERT INTO TEST01 (ROWKEY, COL1, COL2) VALUES ('Y',2,'BAR');
```

Show topics and print the data
```bash
SHOW TOPICS;
PRINT test01 FROM BEGINNING;
```

### 4. Consumer: Read data from Kafka topics

#### a. From the customized Java consumerApp

Get in the consumerApp folder:
```bash 
cd consumerApp
```

When you run the consumer for the **first** time:
```bash 
gradle wrapper
```

Compile the consumerApp: 
```bash 
./gradlew shadowJar
```

Run the consumerApp: 
```bash 
java -jar build/libs/kafka-consumer-application-standalone-0.0.1.jar
```

### 5. Insert data from a topic to MySQL db

#### a. In a new terminal open MySQL as root, create a demo database and grant privileges to athina user (which is the MYSQL_USER=athina in the docker-compose.yml file)
```bash
docker exec -it mysql bash -c 'mysql -u root -p$MYSQL_ROOT_PASSWORD'
```

```bash
create database demo;
grant all on demo.* to 'athina'@'%';
```

To check that privileges were granted you can start MySQL as **athina** user
```bash
docker exec -it mysql bash -c 'mysql -u$MYSQL_USER -p$MYSQL_PASSWORD'
```
and run
```bash
use demo;
show grants;
```

#### b. Create the Sink JDBC MySQL connector using the REST interface of Kafka

In a new terminal:

```bash
curl -X PUT http://localhost:8083/connectors/sink-jdbc-mysql-01/config \
     -H "Content-Type: application/json" -d '{
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:mysql://mysql:3306/demo",
    "topics": "test01",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "connection.user": "athina",
    "connection.password": "athina",
    "auto.create": true,
    "auto.evolve": true,
    "insert.mode": "insert",
    "pk.mode": "record_key",
    "pk.fields": "MESSAGE_KEY"
}'
```
Here we insert data to the demo db from the **test01** topic. Each topic will create its own table in the db.


#### c. Check that the Sink JDBC MySQL connector is working from the ksqldb terminal
```bash
show connectors;
```

### 6. Check the created topic table and the inserted data in MySQL

In a new terminal open MySQL:
```bash
docker exec -it mysql bash -c 'mysql -u root -p$MYSQL_ROOT_PASSWORD'
```

Use the created db, see the created table from the **test01** topic the inserted data.
```bash
use demo;
select * from test01;
```
Keep publishing data to the **test01** topic from the ksqldb. The data will appear in the **test01** table of the demo database.


## For logs

### Kafka Connect
```bash
docker logs -f kafka-connect
```

## Terminal Hack
To keep track of what is happening, I usually have the following terminals open:
* one open in the kafka-telsol folder
* MySQL: 
```bash 
docker exec -it mysql bash -c 'mysql -u root -p$MYSQL_ROOT_PASSWORD'
```
* ksqlDB: 
```bash
docker exec -it ksqldb ksql http://ksqldb:8088
```
* logs:
```bash 
docker logs -f kafka-connect
```