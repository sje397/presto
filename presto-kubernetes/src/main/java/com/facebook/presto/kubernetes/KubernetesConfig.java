package com.facebook.presto.kubernetes;

import com.facebook.airlift.configuration.Config;

import java.net.URI;

import javax.validation.constraints.NotNull;

public class KubernetesConfig {
    private String kubernetesConfigFilename;

    @NotNull
    public String getKubernetesConfigFilename()
    {
        return kubernetesConfigFilename;
    }

    @Config("kubernetes-config-filename")
    public KubernetesConfig setKubernetesConfigFilename(String kubernetesConfigFilename)
    {
        this.kubernetesConfigFilename = kubernetesConfigFilename;
        return this;
    }
}
