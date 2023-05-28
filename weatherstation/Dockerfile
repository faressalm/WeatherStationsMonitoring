FROM openjdk:8-jre-alpine
COPY out/artifacts/weather_station_jar/weather_station.jar /usr/app/
WORKDIR /usr/app
ENTRYPOINT sleep 35 && java -jar weather_station.jar
