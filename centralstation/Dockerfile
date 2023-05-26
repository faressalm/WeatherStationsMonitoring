FROM openjdk:8-jre-alpine
COPY out/artifacts/central_station_jar/central_station.jar /usr/app/
WORKDIR /usr/app
ENTRYPOINT sleep 35 && java -jar central_station.jar