package com.mzinx.mongodb.sink.config;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

/**
 * Auto-configuration for the sink module. Registers the generic change-driven
 * sink listeners (and supporting beans):
 * <ul>
 *   <li>{@code materializedViewListener} — maintains a materialized view by
 *       re-running a configured aggregation over the source collection on every
 *       change (right for {@code $group}/join rollups).</li>
 *   <li>{@code changeMirrorListener} — event-driven mirror that writes only the
 *       changed document into a destination collection (O(1) per event), filtered
 *       by the change stream's own watch pipeline.</li>
 * </ul>
 * <p>
 * Active unless {@code sink.enabled=false}. Requires the change-stream and
 * aggregation infrastructure (pulled in transitively via
 * {@code mongodb-spring-change-stream}).
 */
@AutoConfiguration
@EnableConfigurationProperties(SinkProperties.class)
@ConditionalOnProperty(prefix = "sink", name = "enabled", havingValue = "true", matchIfMissing = true)
@ComponentScan("com.mzinx.mongodb.sink")
@Import(AutoConfigurationPackageRegistrar.class)
public class SinkAutoConfig {
}
