FROM openjdk:8-jre-alpine
COPY out/artifacts/centralstation_jar/centralstation.jar /usr/app/
WORKDIR /usr/app
ENTRYPOINT sleep 35 && java -jar centralstation.jar