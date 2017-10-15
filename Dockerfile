FROM openjdk:8

ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/notification-service/notification-service.jar"]

# Add Maven dependencies
ADD target/lib /usr/share/notification-service/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/notification-service/notification-service.jar
