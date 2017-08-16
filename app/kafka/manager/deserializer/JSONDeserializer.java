package kafka.manager.deserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.*;
import java.util.Map;

public class JSONDeserializer implements Deserializer<byte[]> {
  public static final Logger LOG = LoggerFactory.getLogger(JSONDeserializer.class);

  @Override public void close() {
  }

  @Override public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public byte[] deserialize(String topic, byte[] bytes) {
    if (bytes != null) {
      String deserialized = new String(bytes);
      try {
        ObjectMapper mapper = new ObjectMapper();
        deserialized = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readValue(deserialized, Object.class));
      } catch (Exception e) {
        LOG.info("Couldn't pretty print JSON");
      }
      return deserialized.getBytes();
    } else {
      return null;
    }
  }
}
