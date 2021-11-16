package study.querydsl;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    @GetMapping("/hello")
    public String hello() {
        System.out.println("HelloController.hello");

        return "hello";
    }
}
