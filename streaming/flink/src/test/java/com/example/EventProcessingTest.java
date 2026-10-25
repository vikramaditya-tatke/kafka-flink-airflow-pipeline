package com.example;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class EventProcessingTest {
    static String event(String id) {
        return "{\"eventUid\":\"" + id + "\",\"eventId\":4625,\"source\":\"Security\",\"level\":\"Error\","
            + "\"timestamp\":\"2026-10-25 12:00:01.000\",\"computer\":\"HOST1\",\"user\":\"ADMIN\",\"message\":\"failed\"}";
    }
    @Test void validatesClassifiesAndPreservesIdentity() throws Exception {
        var value = DataConsumerFlink.JSON.readTree(DataConsumerFlink.normalize(event("one")));
        assertEquals("one", value.get("eventUid").asText());
        assertEquals("failed_login", value.get("category").asText());
        assertEquals("HIGH", value.get("severity").asText());
        assertEquals("2026-10-25T12:00:01Z", value.get("timestamp").asText());
    }
    @Test void rejectsMalformedAndInvalidSchema() {
        for (String raw : new String[] {"null", "[]", "{", "{}", event("one").replace("4625", "true"), event("one").replace("2026-10-25", "invalid")}) {
            assertThrows(Exception.class, () -> DataConsumerFlink.normalize(raw));
        }
    }
    @Test void duplicateIdentityDoesNotInflateSecurityCount() throws Exception {
        var aggregate = new DataConsumerFlink.DistinctFailures();
        var state = aggregate.createAccumulator();
        aggregate.add(DataConsumerFlink.normalize(event("one")), state);
        aggregate.add(DataConsumerFlink.normalize(event("one")), state);
        assertEquals(1, aggregate.getResult(state));
        aggregate.add(DataConsumerFlink.normalize(event("two")), state);
        aggregate.add(DataConsumerFlink.normalize(event("three")), state);
        assertEquals(3, aggregate.getResult(state));
    }
    @Test void alertThresholdAndIdentityAreDeterministic() throws Exception {
        assertNull(DataConsumerFlink.securityAlert("HOST1", 0, 30000, 2));
        String alert = DataConsumerFlink.securityAlert("HOST1", 0, 30000, 3);
        assertEquals(alert, DataConsumerFlink.securityAlert("HOST1", 0, 30000, 3));
        assertNotEquals(alert, DataConsumerFlink.securityAlert("HOST2", 0, 30000, 3));
        assertEquals(3, DataConsumerFlink.JSON.readTree(alert).get("failedLogins").asInt());
    }
    @Test void quarantinePreservesRawInputAndStableIdentity() throws Exception {
        String output = DataConsumerFlink.quarantine("bad JSON", "invalid_event");
        assertEquals(output, DataConsumerFlink.quarantine("bad JSON", "invalid_event"));
        assertEquals("bad JSON", DataConsumerFlink.JSON.readTree(output).get("raw").asText());
    }
}
