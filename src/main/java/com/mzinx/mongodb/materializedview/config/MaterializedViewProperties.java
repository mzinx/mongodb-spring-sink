package com.mzinx.mongodb.materializedview.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

/**
 * Configuration for the materialized-view module.
 *
 * @see MaterializedViewAutoConfig
 */
@Data
@ConfigurationProperties("materialized-view")
@Component
public class MaterializedViewProperties {
    /** Master on/off switch for the module (auto-config is gated on this). */
    private boolean enabled = true;
}
