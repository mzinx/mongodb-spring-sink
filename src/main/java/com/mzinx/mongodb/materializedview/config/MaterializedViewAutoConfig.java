package com.mzinx.mongodb.materializedview.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

/**
 * Auto-configuration for the materialized-view module: registers the generic
 * {@code materializedViewListener} (and supporting beans) that maintains a
 * change-stream-driven materialized view by re-running a configured aggregation
 * pipeline on every source change.
 * <p>
 * Active unless {@code materialized-view.enabled=false}. Requires the
 * change-stream and aggregation infrastructure (pulled in transitively via
 * {@code mongodb-spring-change-stream}).
 */
@AutoConfiguration
@EnableConfigurationProperties(MaterializedViewProperties.class)
@ConditionalOnProperty(prefix = "materialized-view", name = "enabled", havingValue = "true", matchIfMissing = true)
@ComponentScan("com.mzinx.mongodb.materializedview")
@Import(AutoConfigurationPackageRegistrar.class)
public class MaterializedViewAutoConfig {
}
