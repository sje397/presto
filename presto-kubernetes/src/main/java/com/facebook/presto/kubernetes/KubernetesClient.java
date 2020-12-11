package com.facebook.presto.kubernetes;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1AttachedVolume;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1ContainerImage;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1LoadBalancerIngress;
import io.kubernetes.client.openapi.models.V1LoadBalancerStatus;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1NamespaceStatus;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1NodeSystemInfo;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.openapi.models.V1ServiceStatus;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KubernetesClient {
    private final Map<String, Map<String, KubernetesTable>> tables;
    private final CoreV1Api api;

    @Inject
    public KubernetesClient(KubernetesConfig config) throws IOException {
        ApiClient client;
        if(config.getKubernetesConfigFilename().equals("default")) {
            client = Config.defaultClient();
        } else {
            client = Config.fromConfig(config.getKubernetesConfigFilename());
        }

        Configuration.setDefaultApiClient(client);
        this.api = new CoreV1Api(client);

        Map<String, KubernetesTable> objectsTables = new HashMap<>();
        objectsTables.put("pods",
                new KubernetesTable("pods", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("kind", VARCHAR),
                        new KubernetesColumn("status", VARCHAR),
                        new KubernetesColumn("namespace", VARCHAR),
                        new KubernetesColumn("host_ip", VARCHAR),
                        new KubernetesColumn("pod_ip", VARCHAR),
                        new KubernetesColumn("reason", VARCHAR)
                ))
        );
        objectsTables.put("namespaces",
                new KubernetesTable("namespaces", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("kind", VARCHAR),
                        new KubernetesColumn("status", VARCHAR)
                ))
        );
        objectsTables.put("containers",
                new KubernetesTable("containers", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("pod_uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("image", VARCHAR),
                        new KubernetesColumn("image_id", VARCHAR),
                        new KubernetesColumn("last_state", VARCHAR),
                        new KubernetesColumn("state", VARCHAR),
                        new KubernetesColumn("ready", VARCHAR),
                        new KubernetesColumn("restart_count", VARCHAR),
                        new KubernetesColumn("started", VARCHAR),
                        new KubernetesColumn("namespace", VARCHAR)
                ))
        );
        objectsTables.put("services",
                new KubernetesTable("services", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("cluster_name", VARCHAR),
                        new KubernetesColumn("namespace", VARCHAR)
                ))
        );
        objectsTables.put("service_ingress",
                new KubernetesTable("service_ingress", ImmutableList.of(
                        new KubernetesColumn("service_uid", VARCHAR),
                        new KubernetesColumn("hostname", VARCHAR),
                        new KubernetesColumn("ip", VARCHAR)
                ))
        );
        objectsTables.put("config_maps",
                new KubernetesTable("config_maps", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("namespace", VARCHAR)
                ))
        );
        objectsTables.put("config_map_values",
                new KubernetesTable("config_map_values", ImmutableList.of(
                        new KubernetesColumn("map_uid", VARCHAR),
                        new KubernetesColumn("parameter", VARCHAR),
                        new KubernetesColumn("value", VARCHAR)
                ))
        );
        objectsTables.put("nodes",
                new KubernetesTable("nodes", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("status", VARCHAR),
                        new KubernetesColumn("architecture", VARCHAR),
                        new KubernetesColumn("boot_id", VARCHAR),
                        new KubernetesColumn("container_runtime_version", VARCHAR),
                        new KubernetesColumn("kernel_version", VARCHAR),
                        new KubernetesColumn("kubelet_version", VARCHAR),
                        new KubernetesColumn("kube_proxy_version", VARCHAR),
                        new KubernetesColumn("machine_id", VARCHAR),
                        new KubernetesColumn("os", VARCHAR),
                        new KubernetesColumn("os_image", VARCHAR),
                        new KubernetesColumn("system_uuid", VARCHAR)
                ))
        );
        objectsTables.put("node_addresses",
                new KubernetesTable("node_addresses", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("address", VARCHAR),
                        new KubernetesColumn("type", VARCHAR)
                ))
        );
        objectsTables.put("node_capacities",
                new KubernetesTable("node_capacities", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("cap", VARCHAR)
                ))
        );
        objectsTables.put("node_allocatable",
                new KubernetesTable("node_allocatable", ImmutableList.of(
                        new KubernetesColumn("uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("cap", VARCHAR)
                ))
        );
        objectsTables.put("node_images",
                new KubernetesTable("node_images", ImmutableList.of(
                        new KubernetesColumn("node_uid", VARCHAR),
                        new KubernetesColumn("names", VARCHAR),
                        new KubernetesColumn("size", VARCHAR)
                ))
        );
        objectsTables.put("node_volumes_attached",
                new KubernetesTable("node_volumes_attached", ImmutableList.of(
                        new KubernetesColumn("node_uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR),
                        new KubernetesColumn("device_path", VARCHAR)
                ))
        );
        objectsTables.put("node_volumes_in_use",
                new KubernetesTable("node_volumes_in_use", ImmutableList.of(
                        new KubernetesColumn("node_uid", VARCHAR),
                        new KubernetesColumn("name", VARCHAR)
                ))
        );

        tables = ImmutableMap.of("objects",  objectsTables);
    }

    public Set<String> getSchemaNames() {
        return tables.keySet();
    }

    public Set<String> getTableNames(String schemaName) {
        return tables.get(schemaName).keySet();
    }

    public KubernetesTable getTable(String schemaName, String tableName) {
        return tables.get(schemaName).get(tableName);
    }

    public List<String> getRows(String schemaName, String tablename) throws ApiException {
        if (!schemaName.equals("objects")) {
            throw new UnsupportedOperationException();
        }

        switch(tablename) {
            case "namespaces": return getNamespaces();
            case "pods": return getPods();
            case "containers": return getPodContainers();
            case "services": return getServices();
            case "service_ingress": return getServiceIngress();
            case "config_maps": return getConfigMaps();
            case "config_map_values": return getConfigMapValues();
            case "nodes": return getNodes();
            case "node_addresses": return getNodeAddresses();
            case "node_capacities": return getNodeCapacities();
            case "node_allocatable": return getNodeAllocatable();
            case "node_images": return getNodeImages();
            case "node_volumes_attached": return getNodeAttachedVolumes();
            case "node_volumes_in_use": return getNodeVolumesInUse();
        }

        return ImmutableList.of();
    }

    private List<String> getNamespaces() throws ApiException {

        V1NamespaceList nl = api.listNamespace(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for (V1Namespace ns : nl.getItems()) {
            V1ObjectMeta meta = ns.getMetadata();
            V1NamespaceStatus status = ns.getStatus();
            String kind = ns.getKind();

            String row = String.format("%s,%s,%s,%s,%s",
                    meta.getUid(), meta.getName(), kind, status.getPhase(),
                    meta.getNamespace());

            ret.add(row);
        }

        return ret;
    }

    private List<String> getPods() throws ApiException {
        V1PodList pl = api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for (V1Pod p : pl.getItems()) {
            V1ObjectMeta meta = p.getMetadata();
            V1PodStatus status = p.getStatus();
            String kind = p.getKind();

            String row = String.format("%s,%s,%s,%s,%s,%s,%s,%s",
                    meta.getUid(), meta.getName(), kind, status.getPhase(),
                    meta.getNamespace(), status.getHostIP(), status.getPodIP(),
                    status.getReason());

            ret.add(row);
        }

        return ret;
    }

    private List<String> getPodContainers() throws ApiException {
        V1PodList pl = api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for (V1Pod p : pl.getItems()) {
            V1ObjectMeta meta = p.getMetadata();
            V1PodStatus status = p.getStatus();
            List<V1ContainerStatus> containerStatuses = status.getContainerStatuses();
            if (containerStatuses == null) {
                continue;
            }

            for (V1ContainerStatus containerStatus : containerStatuses) {

                String row = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                        containerStatus.getContainerID(),
                        meta.getUid(),
                        containerStatus.getName(),
                        containerStatus.getImage(),
                        containerStatus.getImageID(),
                        containerStatus.getLastState().toString(),
                        containerStatus.getState().toString(),
                        containerStatus.getReady().toString(),
                        containerStatus.getRestartCount(),
                        containerStatus.getStarted().toString(),
                        meta.getNamespace());


                ret.add(row);
            }
        }

        return ret;
    }

    private List<String> getServices() throws ApiException {
        V1ServiceList sl = api.listServiceForAllNamespaces(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for (V1Service s : sl.getItems()) {
            V1ObjectMeta meta = s.getMetadata();

            String row = String.format("%s,%s,%s,%s",
                    meta.getUid(), meta.getName(), meta.getClusterName(), meta.getNamespace());
            ret.add(row);
        }
        return ret;
    }

    private List<String> getServiceIngress() throws ApiException {
        V1ServiceList sl = api.listServiceForAllNamespaces(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for (V1Service s : sl.getItems()) {
            V1ObjectMeta meta = s.getMetadata();
            V1ServiceStatus status = s.getStatus();
            V1LoadBalancerStatus lb_status = status.getLoadBalancer();
            List<V1LoadBalancerIngress> ingress = lb_status.getIngress();
            if(ingress == null) {
                continue;
            }
            ingress.forEach(i -> {
                ret.add(String.format("%s,%s,%s",
                        meta.getUid(), i.getHostname(), i.getIp()));
            });
        }
        return ret;
    }

    private List<String> getConfigMaps() throws ApiException {
        V1ConfigMapList cml = api.listConfigMapForAllNamespaces(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for (V1ConfigMap cm : cml.getItems()) {
            V1ObjectMeta meta = cm.getMetadata();

            String row = String.format("%s,%s,%s",
                    meta.getUid(), meta.getName(), meta.getNamespace());
            ret.add(row);
        }
        return ret;
    }

    private List<String> getConfigMapValues() throws ApiException {
        V1ConfigMapList cml = api.listConfigMapForAllNamespaces(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for (V1ConfigMap cm : cml.getItems()) {
            V1ObjectMeta meta = cm.getMetadata();

            Map<String, String> data = cm.getData();
            for(String param: data.keySet()) {
                String value = data.get(param);
                String row = String.format("%s,%s,%s",
                        meta.getUid(), param, value);
                ret.add(row);
            }
        }
        return ret;
    }

    private List<String> getNodes() throws ApiException {
        V1NodeList nl = api.listNode(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for(V1Node n: nl.getItems()) {
            V1ObjectMeta meta = n.getMetadata();
            V1NodeStatus status = n.getStatus();
            V1NodeSystemInfo info = n.getStatus().getNodeInfo();

            String row = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                    meta.getUid(), meta.getName(), status.getPhase(),
                    info.getArchitecture(), info.getBootID(), info.getContainerRuntimeVersion(),
                    info.getKernelVersion(), info.getKubeletVersion(), info.getKubeProxyVersion(),
                    info.getMachineID(), info.getOperatingSystem(), info.getOsImage(),
                    info.getSystemUUID());
            ret.add(row);
        }
        return ret;
    }

    private List<String> getNodeAddresses() throws ApiException {
        V1NodeList nl = api.listNode(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for(V1Node n: nl.getItems()) {
            V1ObjectMeta meta = n.getMetadata();
            V1NodeStatus status = n.getStatus();

            List<V1NodeAddress> addresses = status.getAddresses();
            if(addresses == null) {
                continue;
            }
            for(V1NodeAddress addr: addresses) {
                String row = String.format("%s,%s,%s",
                        meta.getUid(), addr.getAddress(), addr.getType());
                ret.add(row);
            }
        }
        return ret;
    }

    private List<String> getNodeCapacities() throws ApiException {
        V1NodeList nl = api.listNode(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for(V1Node n: nl.getItems()) {
            V1ObjectMeta meta = n.getMetadata();
            V1NodeStatus status = n.getStatus();

            Map<String, Quantity> capMap = status.getCapacity();
            if(capMap == null) {
                continue;
            }
            for(String name: capMap.keySet()) {
                String row = String.format("%s,%s,%s",
                        meta.getUid(), name, capMap.get(name));
                ret.add(row);
            }
        }
        return ret;
    }

    private List<String> getNodeAllocatable() throws ApiException {
        V1NodeList nl = api.listNode(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for(V1Node n: nl.getItems()) {
            V1ObjectMeta meta = n.getMetadata();
            V1NodeStatus status = n.getStatus();

            Map<String, Quantity> capMap = status.getAllocatable();
            if(capMap == null) {
                continue;
            }
            for(String name: capMap.keySet()) {
                String row = String.format("%s,%s,%s",
                        meta.getUid(), name, capMap.get(name));
                ret.add(row);
            }
        }
        return ret;
    }

    private List<String> getNodeImages() throws ApiException {
        V1NodeList nl = api.listNode(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for(V1Node n: nl.getItems()) {
            V1ObjectMeta meta = n.getMetadata();
            V1NodeStatus status = n.getStatus();

            List<V1ContainerImage> images = status.getImages();
            if(images == null) {
                continue;
            }
            for(V1ContainerImage image: images) {
                String row = String.format("%s,%s,%s",
                        meta.getUid(), String.join("|", image.getNames()), image.getSizeBytes());
                ret.add(row);
            }
        }
        return ret;
    }

    private List<String> getNodeAttachedVolumes() throws ApiException {
        V1NodeList nl = api.listNode(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for(V1Node n: nl.getItems()) {
            V1ObjectMeta meta = n.getMetadata();
            V1NodeStatus status = n.getStatus();

            List<V1AttachedVolume> volumes = status.getVolumesAttached();
            if(volumes == null) {
                continue;
            }
            for(V1AttachedVolume volume: volumes) {
                String row = String.format("%s,%s,%s",
                        meta.getUid(), volume.getName(), volume.getDevicePath());
                ret.add(row);
            }
        }
        return ret;
    }

    private List<String> getNodeVolumesInUse() throws ApiException {
        V1NodeList nl = api.listNode(null, null, null, null, null, null, null, null, null);
        List<String> ret = new ArrayList<>();
        for(V1Node n: nl.getItems()) {
            V1ObjectMeta meta = n.getMetadata();
            V1NodeStatus status = n.getStatus();

            List<String> volumes = status.getVolumesInUse();
            if(volumes == null) {
                continue;
            }
            for(String volume: volumes) {
                String row = String.format("%s,%s",
                        meta.getUid(), volume);
                ret.add(row);
            }
        }
        return ret;
    }
}
