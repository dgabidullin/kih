package net.dag.kih.model;

import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskParams implements Serializable {

  private static final long serialVersionUID = 1L;

  @NotNull
  private String dataEntity;

  @NotNull
  private String path;
}
