package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SensorData {
    @JsonProperty("station_id")
    private Long stationId;

    @JsonProperty("s_no")
    private Long sNo;

    @JsonProperty("battery_status")
    private String batteryStatus;

    @JsonProperty("status_timestamp")
    private Long statusTimestamp;

    @JsonProperty("weather")
    private Weather weather;

    // Getters and Setters

    public Long getStationId() {
        return stationId;
    }

    public void setStationId(Long stationId) {
        this.stationId = stationId;
    }

    public Long getSNo() {
        return sNo;
    }

    public void setSNo(Long sNo) {
        this.sNo = sNo;
    }

    public String getBatteryStatus() {
        return batteryStatus;
    }

    public void setBatteryStatus(String batteryStatus) {
        this.batteryStatus = batteryStatus;
    }

    public Long getStatusTimestamp() {
        return statusTimestamp;
    }

    public void setStatusTimestamp(Long statusTimestamp) {
        this.statusTimestamp = statusTimestamp;
    }

    public Weather getWeather() {
        return weather;
    }

    public void setWeather(Weather weather) {
        this.weather = weather;
    }

    // Weather class
    public static class Weather {
        private Integer humidity;
        private Integer temperature;
        private Integer windSpeed;

        // Getters and Setters

        public Integer getHumidity() {
            return humidity;
        }

        public void setHumidity(Integer humidity) {
            this.humidity = humidity;
        }

        public Integer getTemperature() {
            return temperature;
        }

        public void setTemperature(Integer temperature) {
            this.temperature = temperature;
        }

        public Integer getWindSpeed() {
            return windSpeed;
        }

        public void setWindSpeed(Integer windSpeed) {
            this.windSpeed = windSpeed;
        }

        @Override
        public String toString() {
            return "Weather{" +
                    "humidity=" + humidity +
                    ", temperature=" + temperature +
                    ", windSpeed=" + windSpeed +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "stationId=" + stationId +
                ", sNo=" + sNo +
                ", batteryStatus='" + batteryStatus + '\'' +
                ", statusTimestamp=" + statusTimestamp +
                ", weather=" + weather +
                '}';
    }
}
