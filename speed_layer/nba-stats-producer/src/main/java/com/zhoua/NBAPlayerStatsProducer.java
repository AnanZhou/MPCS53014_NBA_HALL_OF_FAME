package com.zhoua;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;

public class NBAPlayerStatsProducer {

    // Kafka topic name
    private static final String TOPIC = "current-season-stats-zhoua";

    // Kafka brokers
    private static final String BOOTSTRAP_SERVERS = "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092,wn1-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092";

    // API Key for SportsAPI
    private static final String API_KEY = "00a179a8374cf8486761b08a87b9e5ce";

    // Map of player IDs to player names
    private static final Map<Integer, String> PLAYER_MAP = new HashMap<>();

    static {
        PLAYER_MAP.put(747, "James Lebron");
        PLAYER_MAP.put(807, "Antetokounmpo Giannis");
        PLAYER_MAP.put(661, "Curry Stephen");
        PLAYER_MAP.put(939, "Durant Kevin");
        PLAYER_MAP.put(621, "Jokic Nikola");
        PLAYER_MAP.put(612, "Doncic Luka");
        PLAYER_MAP.put(745, "Anthony Davis");
        PLAYER_MAP.put(730, "Leonard Kawhi");
        PLAYER_MAP.put(722, "Harden James");
        PLAYER_MAP.put(608, "Irving Kyrie");
    }

    public static void main(String[] args) throws Exception {
        // Set up Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Loop through each player and send their stats
        for (Map.Entry<Integer, String> entry : PLAYER_MAP.entrySet()) {
            int playerId = entry.getKey();
            String playerName = entry.getValue();

            try {
                // Fetch and process player stats
                String aggregatedStats = getAggregatedPlayerStats(playerId, "2024-2025");

                // Send aggregated stats to Kafka topic
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, playerName, aggregatedStats);
                producer.send(record);

                System.out.println("Aggregated stats for " + playerName + " sent to Kafka topic: " + TOPIC);
            } catch (Exception e) {
                System.err.println("Failed to process stats for player: " + playerName);
                e.printStackTrace();
            }
        }

        producer.close();
    }

    private static String getAggregatedPlayerStats(int playerId, String season) throws Exception {
        String uri = "https://v1.basketball.api-sports.io/games/statistics/players?season=" + season + "&player=" + playerId;

        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet request = new HttpGet(uri);
        request.addHeader("x-rapidapi-host", "v1.basketball.api-sports.io");
        request.addHeader("x-rapidapi-key", API_KEY);

        CloseableHttpResponse response = client.execute(request);

        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode != 200) {
            String responseBody = EntityUtils.toString(response.getEntity());
            client.close();
            throw new Exception("Failed to fetch player stats: " + responseBody);
        }

        String responseBody = EntityUtils.toString(response.getEntity());
        client.close();

        // Parse JSON response and aggregate stats
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(responseBody);

        int totalPoints = 0;
        int totalRebounds = 0;
        int totalAssists = 0;

        JsonNode games = rootNode.get("response");
        if (games != null && games.isArray()) {
            for (JsonNode game : games) {
                totalPoints += game.get("points").asInt();
                totalRebounds += game.get("rebounds").get("total").asInt();
                totalAssists += game.get("assists").asInt();
            }
        }

        // Construct the aggregated stats report
        String aggregatedStats = String.format(
                "Total Stats for %s in Season %s: Points: %d, Rebounds: %d, Assists: %d",
                PLAYER_MAP.get(playerId), season, totalPoints, totalRebounds, totalAssists);

        return aggregatedStats;
    }
}
