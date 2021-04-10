package ps.hassany.tutorial.dockerCompose;

import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PeopleRepository extends Neo4jRepository<PersonEntity, Long> {}
