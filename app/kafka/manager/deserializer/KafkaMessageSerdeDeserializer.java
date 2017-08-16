package kafka.manager.deserializer;

import java.lang.*;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Map;
import com.groupon.kafka.serde.LoggernautEvent;
import com.groupon.kafka.serde.decoder.JanusParser;
import com.groupon.kafka.serde.decoder.ParserFactory;
import com.groupon.kafka.serde.decoder.Parser;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by rtang on 8/15/17.
 */
public class KafkaMessageSerdeDeserializer implements Deserializer<byte[]> {
  public static final Logger LOG = LoggerFactory.getLogger(KafkaMessageSerdeDeserializer.class);
  public static final String CONFIG_VALUE_LOGGERNAUT_META = "value.deserializer.loggernaut.meta";

  private boolean loggernautMetaBool = true;
  private Parser rawParser;
  private Parser loggernautParser;
  private Parser prodJanusParser;
  private Parser stagingJanusParser;
  private Parser uatJanusParser;

  @Override public void close() {
  }

  @Override public void configure(Map<String, ?> configs, boolean isKey) {
    if (!isKey) {
      loggernautMetaBool = Boolean.valueOf(String.valueOf(configs.get(CONFIG_VALUE_LOGGERNAUT_META)));
    }
    rawParser = ParserFactory.getRawParser();
    loggernautParser = ParserFactory.getLoggernautParser(loggernautMetaBool);
    prodJanusParser = ParserFactory.getJanusParser(JanusParser.PRODUCTION_URL);
    stagingJanusParser = ParserFactory.getJanusParser(JanusParser.STAGING_URL);
    uatJanusParser = ParserFactory.getJanusParser(JanusParser.UAT_URL);
  }

  @Override
  public byte[] deserialize(String topic, byte[] bytes) {
    if (bytes != null) {
      boolean rawBool = true;
      String deserialized = "";
      List<Object> msgs;
      try {
        msgs = loggernautParser.getMessageList(bytes);
        LOG.info("Try to decode with loggernaut parser");
        if (loggernautMetaBool) rawBool = false;
      } catch (Exception e1) {
        try {
          msgs = prodJanusParser.getMessageList(bytes);
          LOG.info("Try to decode with production Janus parser");
        } catch (Exception e2) {
          try {
            msgs = stagingJanusParser.getMessageList(bytes);
            LOG.info("Try to decode with staging Janus parser");
          } catch (Exception e3) {
            try {
              msgs = uatJanusParser.getMessageList(bytes);
              LOG.info("Try to decode with UAT staging parser");
            } catch (Exception e4) {
              try {
                msgs = rawParser.getMessageList(bytes);
                LOG.info("Try to decode with raw parser");
              } catch (Exception e5) {
                try {
                  deserialized = new String(bytes);
                  LOG.info("Try to string-ify raw bytes");
                  return deserialized.getBytes();
                } catch (Exception e6) {
                  return deserialized.getBytes();
                }
              }
            }
          }
        }
      }
      for (Object msg : msgs) {
        if (rawBool) {
          deserialized += msg.toString();
        } else {
          try {
            deserialized += ((LoggernautEvent) msg).toJsonString(!loggernautMetaBool, false);
          } catch (Exception e) {
            deserialized += msg.toString();
          }
        }
      }
      if (!rawBool) {
        try {
          ObjectMapper mapper = new ObjectMapper();
          deserialized = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readValue(deserialized, Object.class));
        } catch (Exception e) {
          LOG.info("Couldn't pretty print JSON");
        }
      }
      return deserialized.getBytes();
    } else {
      return null;
    }
  }
}

