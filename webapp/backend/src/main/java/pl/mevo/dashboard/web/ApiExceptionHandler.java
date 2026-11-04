package pl.mevo.dashboard.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import pl.mevo.dashboard.service.WarehouseQueryException;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(WarehouseQueryException.class)
    ProblemDetail handleWarehouseQuery(WarehouseQueryException ex) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.SERVICE_UNAVAILABLE);
        problem.setTitle("ClickHouse unavailable");
        Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
        problem.setDetail(cause.getMessage());
        return problem;
    }
}
