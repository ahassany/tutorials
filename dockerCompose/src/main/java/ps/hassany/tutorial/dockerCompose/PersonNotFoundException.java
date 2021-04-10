package ps.hassany.tutorial.dockerCompose;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.NOT_FOUND)
public class PersonNotFoundException extends RuntimeException {
  public PersonNotFoundException(Long id) {
    super(String.format("{\"message\": \"Person with id: '%s' is not found\"}", id));
  }
}
