package ps.hassany.kafka.tutorial.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.data.Json;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.util.Map;

public class JsonToAvroWithKeyMessageParser<K, V> implements MessageLineParser<K, V> {
  private final Schema schema;
  private final String keyName;
  private final ObjectMapper objectMapper;

  public JsonToAvroWithKeyMessageParser(Schema schema, String keyName) {
    this.schema = schema;
    this.keyName = keyName;
    this.objectMapper = new ObjectMapper();
  }

  public Message<K, V> parseMessage(final String message) throws IOException {
    K key = (K) ((Map<String, Object>) Json.parseJson(message)).get(keyName);
    DatumReader<V> reader = new SpecificDatumReader<>(schema);
    Decoder decoder = DecoderFactory.get().jsonDecoder(schema, message);
    V value = reader.read(null, decoder);
    return new Message<>(key, value);
  }
}
