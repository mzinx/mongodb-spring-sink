package com.mzinx.mongodb.sink.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

/**
 * Configuration for the sink module (change-driven collection sinks:
 * materialized views and event-driven mirrors).
 *
 * @see SinkAutoConfig
 */
@Data
@ConfigurationProperties("sink")
@Component
public class SinkProperties {
    /** Master on/off switch for the module (auto-config is gated on this). */
    private boolean enabled = true;
}
