FROM openjdk:8-jdk-buster

ENV DEBIAN_FRONTEND noninteractive

# Download hadoop
WORKDIR /opt
RUN apt-get update
RUN apt-get install -y bash curl maven python
RUN curl -L 'http://archive.apache.org/dist/hadoop/core/hadoop-3.2.1/hadoop-3.2.1.tar.gz' | tar -xz

# Copy the project
COPY . /opt/ldbc_snb_datagen
WORKDIR /opt/ldbc_snb_datagen

# Build jar bundle
RUN mvn -DskipTests clean assembly:assembly

ENV HADOOP_CLIENT_OPTS '-Xmx8G'
CMD /opt/ldbc_snb_datagen/docker_run.sh
