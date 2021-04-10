package ps.hassany.tutorial.dockerCompose;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/people")
@SuppressWarnings("unused")
public class Controller {
  private final PeopleRepository peopleRepository;

  public Controller(PeopleRepository peopleRepository) {
    this.peopleRepository = peopleRepository;
  }

  @GetMapping("")
  public List<PersonEntity> all() {
    return peopleRepository.findAll();
  }

  @GetMapping("/{id}")
  public PersonEntity getPersonById(@PathVariable Long id) {
    return peopleRepository
        .findById(id)
        .orElseThrow(
            () -> {
              throw new PersonNotFoundException(id);
            });
  }

  @PutMapping("/{id}")
  public PersonEntity updatePersonById(
      @PathVariable Long id, @RequestBody PersonEntity personEntity) {
    return peopleRepository.save(personEntity);
  }

  @PostMapping(path = "", produces = MediaType.APPLICATION_JSON_VALUE)
  ResponseEntity<PersonEntity> newPerson(@RequestBody PersonEntity newPerson) {
    var person = peopleRepository.save(newPerson);
    return new ResponseEntity<>(person, HttpStatus.CREATED);
  }
}
