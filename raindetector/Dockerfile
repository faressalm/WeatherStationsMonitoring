FROM openjdk:8-jre-alpine
COPY out/artifacts/rain_detector_jar/raindetector.jar /usr/app/
WORKDIR /usr/app
ENTRYPOINT sleep 35 && java -jar raindetector.jar