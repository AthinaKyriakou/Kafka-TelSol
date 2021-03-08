# Kafka-telsol
A NTUA IT Infrastructure Design Class project...

## Linux Installation Instructions

### 1. Install Docker

1. Select your server (i.e. Debian, Fedora, Ubuntu) from [here](https://docs.docker.com/engine/install/#server).
2. Follow the instructions on sections `SET UP THE REPOSITORY` and `INSTALL DOCKER ENGINE`.

### 2. Clone this repository

```bash
git clone https://github.com/AthinaKyriakou/kafka-telsol.git

cd kafka-telsol
```

### 3. Docker Compose

1. Bring the Docker Compose up. The first time it runs for long...
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
