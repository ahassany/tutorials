package ps.hassany.tutorial.dockerCompose;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class ControllerTest {
  @Autowired MockMvc mockMvc;
  @Autowired ObjectMapper objectMapper;
  @MockBean PeopleRepository peopleRepository;
  private PersonEntity testPerson1;
  private PersonEntity testPerson2;

  public ControllerTest() {
    testPerson1 = new PersonEntity(1L, "Person1");
    testPerson2 = new PersonEntity(2L, "Person2");
  }

  @Test
  public void getAll_shouldReturn() throws Exception {
    var peopleList = List.of(testPerson1, testPerson2);
    when(peopleRepository.findAll()).thenReturn(peopleList);
    mockMvc
        .perform(get("/people"))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(peopleList), true))
        .andReturn();
  }

  @Test
  public void getPersonById_shouldReturnOnePerson() throws Exception {
    when(peopleRepository.findById(testPerson1.getId())).thenReturn(Optional.of(testPerson1));
    mockMvc
        .perform(get(String.format("/people/%s", testPerson1.getId())))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(testPerson1), true))
        .andReturn();
  }

  @Test
  public void getPersonById_shouldReturnNotFound() throws Exception {
    when(peopleRepository.findById(testPerson1.getId())).thenReturn(Optional.empty());
    mockMvc
        .perform(get(String.format("/people/%s", testPerson1.getId())))
        .andExpect(status().isNotFound())
        .andReturn();
  }

  @Test
  public void updatePersonById_shouldReturnOnePerson() throws Exception {
    when(peopleRepository.save(testPerson1)).thenReturn(testPerson1);
    mockMvc
        .perform(
            put(String.format("/people/%s", testPerson1.getId()))
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(testPerson1)))
        .andExpect(status().isOk())
        .andExpect(content().json(objectMapper.writeValueAsString(testPerson1), true))
        .andReturn();
  }

  @Test
  public void newPerson_shouldReturnOnePerson() throws Exception {
    when(peopleRepository.save(testPerson1)).thenReturn(testPerson1);
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
