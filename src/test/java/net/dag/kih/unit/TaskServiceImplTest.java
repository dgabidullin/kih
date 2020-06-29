package net.dag.kih.unit;

import com.google.common.collect.ImmutableMap;
import net.dag.kih.service.psa.PsaService;
import net.dag.kih.service.task.impl.TaskServiceImpl;
import net.dag.kih.model.Task;
import net.dag.kih.model.TaskParams;
import net.dag.kih.model.TaskResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static net.dag.kih.model.TaskResult.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskServiceImplTest {

  private static final String DEST_PATH = "/tmp/version=123";
  private static final String LATEST_PATH = "/tmp/latest";

  private TaskServiceImpl taskService;
  @Mock
  private PsaService<Row, Task> psaService;
  @Mock
  private Dataset<Row> dataset;

  @Before
  public void setUp() {
    taskService = new TaskServiceImpl(psaService);
  }

  @Test
  public void testProcessWithoutKeys() {
    Task task = new Task();
    TaskParams params = new TaskParams();
    params.setPath(DEST_PATH);
    task.setParams(params);
    when(psaService.processDataset(task)).thenReturn(dataset);

    TaskResult result = taskService.execute(task);

    assertEquals(OK, result.getStatus());
    verify(psaService, times(1)).processDataset(task);
    verify(psaService, times(1)).save(dataset, DEST_PATH, 1, SaveMode.Overwrite);
    verify(psaService, times(1)).save(dataset, LATEST_PATH, 1, SaveMode.Overwrite);
    verify(psaService, times(2)).save(any(), any(), anyInt(), any());
  }

  @Test
  public void testProcessWithKeys() {
    Task task = new Task();
    TaskParams params = new TaskParams();
    params.setPath(DEST_PATH);
    task.setParams(params);
    task.setKeys(ImmutableMap.of("batch", "1"));
    when(psaService.processDataset(task)).thenReturn(dataset);

    TaskResult result = taskService.execute(task);

    assertEquals(OK, result.getStatus());
    verify(psaService, times(1)).processDataset(task);
    verify(psaService, times(1)).save(dataset, DEST_PATH, 1, SaveMode.Overwrite);
    verify(psaService, times(1)).save(any(), any(), anyInt(), any());
  }

  @Test
  public void testProcessWithEmptyPath() {
    Task task = new Task();
    TaskParams params = new TaskParams();
    params.setPath("");
    task.setParams(params);
    task.setKeys(ImmutableMap.of("batch", "1"));
    when(psaService.processDataset(task)).thenReturn(dataset);

    TaskResult result = taskService.execute(task);

    assertEquals(OK, result.getStatus());
    verify(psaService, times(1)).processDataset(task);
    verify(psaService, times(1)).save(dataset, "", 1, SaveMode.Overwrite);
    verify(psaService, times(1)).save(any(), any(), anyInt(), any());
  }
}
