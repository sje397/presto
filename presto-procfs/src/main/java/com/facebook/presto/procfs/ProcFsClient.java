package com.facebook.presto.procfs;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcFsClient {
    private static final Logger log = Logger.get(ProcFsClient.class);

    private final Map<String, Map<String, ProcFsTable>> tables;

    private Map<HostAddress, Session> sessionCache = new HashMap<>();

    private final String username;
    private final String password;

    @Inject
    public ProcFsClient(ProcFsConfig config) {
        this.username = config.getUsername();
        this.password = config.getPassword();

        Map<String, ProcFsTable> objectsTables = new HashMap<>();
        objectsTables.put("cpuinfo",
                new ProcFsTable("cpuinfo", ImmutableList.of(
//        processor	: 1
//        vendor_id	: GenuineIntel
//        cpu family	: 6
//        model		: 79
//        model name	: Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz
//        stepping	: 0
//        microcode	: 0xb000036
//        cpu MHz		: 2099.998
//        cache size	: 20480 KB
//        physical id	: 2
//        siblings	: 1
//        core id		: 0
//        cpu cores	: 1
//        apicid		: 2
//        initial apicid	: 2
//        fpu		: yes
//        fpu_exception	: yes
//        cpuid level	: 13
//        wp		: yes
//        flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc arch_perfmon nopl xtopology tsc_reliable nonstop_tsc eagerfpu pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt aes xsave avx f16c rdrand hypervisor lahf_lm 3dnowprefetch ssbd ibrs ibpb stibp fsgsbase smep arat md_clear spec_ctrl intel_stibp flush_l1d arch_capabilities
//        bogomips	: 4199.99
//        clflush size	: 64
//        cache_alignment	: 64
//        address sizes	: 40 bits physical, 48 bits virtual
//        power management:
                        new ProcFsColumn("host", VARCHAR),
                        new ProcFsColumn("processor_id", BIGINT),
                        new ProcFsColumn("vendor_id", VARCHAR),
                        new ProcFsColumn("family", BIGINT),
                        new ProcFsColumn("model", BIGINT),
                        new ProcFsColumn("model_name", VARCHAR),
                        new ProcFsColumn("stepping", BIGINT),
                        new ProcFsColumn("microcode", VARCHAR),
                        new ProcFsColumn("mhz", DOUBLE),
                        new ProcFsColumn("cache_size", VARCHAR),
                        new ProcFsColumn("physical_id", BIGINT),
                        new ProcFsColumn("siblings", BIGINT),
                        new ProcFsColumn("core_id", BIGINT),
                        new ProcFsColumn("cores", BIGINT),
                        new ProcFsColumn("apicid", BIGINT),
                        new ProcFsColumn("initial_apicid", BIGINT),
                        new ProcFsColumn("fpu", BOOLEAN),
                        new ProcFsColumn("fpu_exception", BOOLEAN),
                        new ProcFsColumn("cpuid_level", BIGINT),
                        new ProcFsColumn("wp", BOOLEAN),
                        new ProcFsColumn("flags", VARCHAR),
                        new ProcFsColumn("bogomips", DOUBLE),
                        new ProcFsColumn("clflush_size", BIGINT),
                        new ProcFsColumn("cache_alignment", BIGINT),
                        new ProcFsColumn("address_sizes", VARCHAR),
                        new ProcFsColumn("power_management", VARCHAR)
                ))
        );
        objectsTables.put("meminfo",
                new ProcFsTable(",meminfo", ImmutableList.of(
//        MemTotal:        1882244 kB
//        MemFree:          967392 kB
//        MemAvailable:    1387940 kB
//        Buffers:            4180 kB
//        Cached:           528900 kB
//        SwapCached:        38584 kB
//        Active:           428744 kB
//        Inactive:         284664 kB
//        Active(anon):      50224 kB
//        Inactive(anon):   161504 kB
//        Active(file):     378520 kB
//        Inactive(file):   123160 kB
//        Unevictable:           0 kB
//        Mlocked:               0 kB
//        SwapTotal:       4194300 kB
//        SwapFree:        3959292 kB
//        Dirty:                16 kB
//        Writeback:             0 kB
//        AnonPages:        158060 kB
//        Mapped:            32164 kB
//        Shmem:             31400 kB
//        Slab:             126604 kB
//        SReclaimable:     100084 kB
//        SUnreclaim:        26520 kB
//        KernelStack:        2672 kB
//        PageTables:         8036 kB
//        NFS_Unstable:          0 kB
//        Bounce:                0 kB
//        WritebackTmp:          0 kB
//        CommitLimit:     5135420 kB
//        Committed_AS:     796276 kB
//        VmallocTotal:   34359738367 kB
//        VmallocUsed:      153832 kB
//        VmallocChunk:   34359341052 kB
//        HardwareCorrupted:     0 kB
//        AnonHugePages:     12288 kB
//        CmaTotal:              0 kB
//        CmaFree:               0 kB
//        HugePages_Total:       0
//        HugePages_Free:        0
//        HugePages_Rsvd:        0
//        HugePages_Surp:        0
//        Hugepagesize:       2048 kB
//        DirectMap4k:       75648 kB
//        DirectMap2M:     2021376 kB
//        DirectMap1G:           0 kB
                        new ProcFsColumn("host", VARCHAR),
                        new ProcFsColumn("total", BIGINT),
                        new ProcFsColumn("free", BIGINT),
                        new ProcFsColumn("available", BIGINT),
                        new ProcFsColumn("buffers", BIGINT),
                        new ProcFsColumn("cached", BIGINT),
                        new ProcFsColumn("swap_cached", BIGINT),
                        new ProcFsColumn("active", BIGINT),
                        new ProcFsColumn("inactive", BIGINT),
                        new ProcFsColumn("swap_total", BIGINT),
                        new ProcFsColumn("swap_free", BIGINT)
                ))
        );
        objectsTables.put("fsinfo",
                new ProcFsTable(",fsinfo", ImmutableList.of(
                        new ProcFsColumn("host", VARCHAR),
                        new ProcFsColumn("fs", VARCHAR),
                        new ProcFsColumn("blocks_1k", BIGINT),
                        new ProcFsColumn("used", BIGINT),
                        new ProcFsColumn("available", BIGINT),
                        new ProcFsColumn("used_pct", BIGINT),
                        new ProcFsColumn("mount_point", VARCHAR)
                ))
        );
        objectsTables.put("netinfo",
                new ProcFsTable(",netinfo", ImmutableList.of(
                        new ProcFsColumn("host", VARCHAR),
                        new ProcFsColumn("dev", VARCHAR),
                        new ProcFsColumn("bytes_rx", BIGINT),
                        new ProcFsColumn("packets_rx", BIGINT),
                        new ProcFsColumn("errs_rx", BIGINT),
                        new ProcFsColumn("drop_rx", BIGINT),
                        new ProcFsColumn("bytes_tx", BIGINT),
                        new ProcFsColumn("packets_tx", BIGINT),
                        new ProcFsColumn("errs_tx", BIGINT),
                        new ProcFsColumn("drop_tx", BIGINT)
                ))
        );
        tables = ImmutableMap.of("proc",  objectsTables);
    }

    public Set<String> getSchemaNames() {
        return tables.keySet();
    }

    public Set<String> getTableNames(String schemaName) {
        return tables.get(schemaName).keySet();
    }

    public ProcFsTable getTable(String schemaName, String tableName) {
        return tables.get(schemaName).get(tableName);
    }

    public List<String> getRows(String schemaName, String tablename, HostAddress host)
            throws Exception
    {
        if(!schemaName.equals("proc")) {
            throw new UnsupportedOperationException();
        }

        switch(tablename) {
            case "cpuinfo": return getCpuInfo(host);
            case "meminfo": return getMemInfo(host);
            case "fsinfo": return getFsInfo(host);
            case "netinfo": return getNetInfo(host);
        }

        return ImmutableList.of();
    }

    private List<String> getCpuInfo(HostAddress host) throws Exception {
        Iterator<String> fileData = runRemoteCommand(host, "cat /proc/cpuinfo");

        List<String> rows = new ArrayList<>();
        String row = host.getHostText();
        while(fileData.hasNext()) {
            String line = fileData.next();
            log.debug("Processing line: " + line);

            if(line.trim().isEmpty()) {
                log.debug("Blank line - adding row: " + row);
                rows.add(row);
                row = host.getHostText();
                continue;
            }

            Iterator<String> parts = Splitter.on(":").split(line).iterator();
            // skip param name
            parts.next();
            String value = "";
            if(parts.hasNext()) value = parts.next();
            row += "," + value;
        }

        return rows;
    }

    private List<String> getMemInfo(HostAddress host) throws Exception {
        Iterator<String> fileData = runRemoteCommand(host, "cat /proc/meminfo");

        List<String> rows = new ArrayList<>();
        Map<String, String> table = new HashMap<>();
        while(fileData.hasNext()) {
            String line = fileData.next();
            log.debug("Processing line: " + line);

            if(line.trim().isEmpty()) {
                continue;
            }

            Iterator<String> parts = Splitter.on(":").split(line).iterator();
            // skip param name
            String param = parts.next().trim();
            String value = "";
            if(parts.hasNext()) value = parts.next().trim().replace(" kB", "");
            table.put(param, value);
        }

        String row = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                host.getHostText(),
                table.get("MemTotal"), table.get("MemFree"), table.get("MemAvailable"),
                table.get("Buffers"), table.get("Cached"), table.get("SwapCached"),
                table.get("Active"), table.get("Inactive"), table.get("SwapTotal"),
                table.get("SwapFree")
        );

        return ImmutableList.of(row);
    }

    private List<String> getFsInfo(HostAddress host) throws Exception {
        Iterator<String> fileData = runRemoteCommand(host, "df");

        List<String> rows = new ArrayList<>();
        while(fileData.hasNext()) {
            String line = fileData.next();
            log.debug("Processing line: " + line);

            if(line.trim().isEmpty() || line.startsWith("Filesystem")) {
                continue;
            }

            Iterator<String> parts = Splitter.onPattern("\\s+").split(line).iterator();
            String row = host.getHostText();
            while(parts.hasNext()) {
                row += "," + parts.next().replace("%", "");
            }

            log.debug("Adding row: " + row);
            rows.add(row);
        }

        return rows;
    }

    private List<String> getNetInfo(HostAddress host) throws Exception {
        Iterator<String> fileData = runRemoteCommand(host, "cat /proc/net/dev");

        List<String> rows = new ArrayList<>();
        while(fileData.hasNext()) {
            String line = fileData.next().trim();

            if(line.trim().isEmpty() || line.contains("|")) {
                continue;
            }

            Iterator<String> parts = Splitter.onPattern("\\s+").split(line).iterator();
            String row = host.getHostText();
            // device, and first four values (rx)
            for(int i = 0; i < 5; i++) {
                row += "," + parts.next().replace(":", "");
            }
            // skip four
            for(int i = 0; i < 4; i++) {
                parts.next();
            }
            // tx values
            for(int i = 0; i < 4; i++) {
                row += "," + parts.next().replace(":", "");
            }

            log.debug("Adding row: " + row);
            rows.add(row);
        }

        return rows;
    }

    private Iterator<String> runRemoteCommand(HostAddress host, String command)
            throws JSchException, IOException, InterruptedException
    {
        log.debug("Connecting to %s...", host);

        Session session = null;
        ChannelExec channel = null;

        try {
            if(sessionCache.containsKey(host)) {
                session = sessionCache.get(host);
            } else {
                session = new JSch().getSession(username, host.getHostText(), host.getPort());
                session.setPassword(password);
                session.setConfig("StrictHostKeyChecking", "no");
                session.connect();

                sessionCache.put(host, session);
            }

            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            ByteArrayOutputStream responseStream = new ByteArrayOutputStream();
            channel.setOutputStream(responseStream);
            channel.connect();

            while (channel.isConnected()) {
                Thread.sleep(100);
            }

            ByteSource in = ByteSource.wrap(responseStream.toByteArray());
            log.debug("Returning data from command \"%s\"", command);
            return in.asCharSource(UTF_8).readLines().stream().iterator();
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
        }
    }

    public void shutdown() {
        sessionCache.entrySet().stream().forEach(e -> {
            Session session = e.getValue();
            session.disconnect();
        });
    }
}
