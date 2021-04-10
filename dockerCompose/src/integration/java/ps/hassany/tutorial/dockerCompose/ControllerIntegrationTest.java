package ps.hassany.tutorial.dockerCompose;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class ControllerIntegrationTest {
  @Autowired MockMvc mockMvc;
  @Autowired ObjectMapper objectMapper;
  @Autowired PeopleRepository peopleRepository;
  private PersonEntity testPerson1;
  private PersonEntity testPerson2;
  private List<PersonEntity> peopleList;

  @BeforeEach
  public void init() {
    testPerson1 = new PersonEntity(1L, "Person1");
    testPerson2 = new PersonEntity(2L, "Person2");
    peopleList = List.of(testPerson1, testPerson2);
  }

  @AfterEach
  public void cleanup() {
    peopleRepository.deleteAll();
  }

  @Test
  public void getAll_shouldReturn() throws Exception {
    peopleRepository.saveAll(peopleList);
    mockMvc
        .perform(get("/people"))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(peopleList), true))
        .andReturn();
  }

  @Test
  public void getPersonById_shouldReturnOnePerson() throws Exception {
    peopleRepository.saveAll(peopleList);
    mockMvc
        .perform(get(String.format("/people/%s", testPerson1.getId())))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(testPerson1), true))
        .andReturn();
  }

  @Test
  public void getPersonById_shouldReturnNotFound() throws Exception {
    mockMvc
        .perform(get(String.format("/people/%s", testPerson1.getId())))
        .andExpect(status().isNotFound())
        .andReturn();
  }

  @Test
  public void updatePersonById_shouldReturnOnePerson() throws Exception {
    peopleRepository.saveAll(peopleList);
    var updatedPerson1 = new PersonEntity(testPerson1.getId(), "Updated Person 1");
    mockMvc
        .perform(
            put(String.format("/people/%s", updatedPerson1.getId()))
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(updatedPerson1)))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(updatedPerson1), true))
        .andReturn();
  }

  @Test
  public void newPerson_shouldReturnOnePerson() throws Exception {
    mockMvc
        .perform(
            post("/people")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testPerson1)))
        .andExpect(status().isCreated())
        .andExpect(content().json(objectMapper.writeValueAsString(testPerson1), true))
        .andReturn();
  }
}
