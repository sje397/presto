package com.facebook.presto.procfs;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.validation.constraints.NotNull;

public class ProcFsConfig {
    private String username;
    private String password;
    private String hosts;

    @NotNull
    public String getUsername()
    {
        return username;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @NotNull
    public List<HostAddress> getHosts()
    {
        Splitter HOST_SPLITTER = Splitter.on(",").trimResults();
        Splitter PORT_SPLITTER = Splitter.on(":").trimResults();

        ArrayList<HostAddress> ret = new ArrayList<>();
        HOST_SPLITTER.split(hosts).forEach(host -> {
            int port = 22;
            if (host.indexOf(':') != -1) {
                Iterator<String> portSplit = PORT_SPLITTER.split(host).iterator();
                host = portSplit.next();
                port = Integer.parseInt(portSplit.next());
            }
            ret.add(HostAddress.fromParts(host, port));
        });

        return ret;
    }

    @Config("username")
    public ProcFsConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    @Config("password")
    public ProcFsConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @Config("hosts")
    public ProcFsConfig setHosts(String hosts)
    {
        this.hosts = hosts;
        return this;
    }
}
