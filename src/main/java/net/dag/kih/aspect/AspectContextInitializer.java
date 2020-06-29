package net.dag.kih.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import java.util.UUID;

@Order(1)
@Service
@Aspect
public class AspectContextInitializer {

  private static final String OPERATION_ID = "operation_id";
  private static final String HOST_NAME = "host_name";
  private final String hostname;

  @Autowired
  public AspectContextInitializer(String hostname) {
    this.hostname = hostname;
  }

  @Around("methodsAnnotatedWithMethodWithMdcContext()")
  public void aroundAnnotatedMethods(ProceedingJoinPoint joinPoint) throws Throwable {
    String operationId = UUID.randomUUID().toString().toUpperCase().replace("-", "");
    MDC.put(OPERATION_ID, "operation_id=" + operationId);
    MDC.put(HOST_NAME, "host_name=" + hostname);
    joinPoint.proceed();
  }

  @Pointcut(value = "@annotation(net.dag.kih.aspect.DiagnosticOperation)")
  public void methodsAnnotatedWithMethodWithMdcContext() {
    // defines pointcut for methods annotated with MethodWithMdcContext
  }
}
