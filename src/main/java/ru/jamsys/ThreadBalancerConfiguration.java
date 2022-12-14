package ru.jamsys;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.jamsys.component.ThreadBalancerFactory;

@Configuration
public class ThreadBalancerConfiguration {

    @Bean
    public ThreadBalancerFactory getThreadBalancerFactory() {
        return new ThreadBalancerFactory();
    }

}
