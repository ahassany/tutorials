package ps.hassany.tutorial.dockerCompose;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;

@Node("Person")
@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode
public class PersonEntity {
  @Id private Long id;
  private String name;
}
