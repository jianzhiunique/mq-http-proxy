FROM openjdk:9
MAINTAINER jianzhiunique <jianzhiunique@163.com>
VOLUME /tmp
EXPOSE 8080
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/mqproxy/mqproxy.jar
ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/mqproxy/mqproxy.jar"]
