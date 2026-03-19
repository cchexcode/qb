use {
    anyhow::{
        Context,
        Result,
    },
    jiff::Timestamp,
    k8s_openapi::{
        api::{
            apps::v1::{
                DaemonSet,
                Deployment,
                ReplicaSet,
                StatefulSet,
            },
            autoscaling::v1::HorizontalPodAutoscaler,
            batch::v1::{
                CronJob,
                Job,
            },
            core::v1::{
                ConfigMap,
                Endpoints,
                Namespace,
                Node,
                PersistentVolume,
                PersistentVolumeClaim,
                Pod,
                Secret,
                Service,
                ServiceAccount,
            },
            events::v1::Event,
            networking::v1::{
                Ingress,
                NetworkPolicy,
            },
            rbac::v1::{
                ClusterRole,
                ClusterRoleBinding,
                Role,
                RoleBinding,
            },
            storage::v1::StorageClass,
        },
        apimachinery::pkg::apis::meta::v1::Time,
    },
    kube::{
        api::{
            Api,
            ListParams,
        },
        config::{
            KubeConfigOptions,
            Kubeconfig,
        },
        Client,
    },
    serde_json::Value,
};

// ---------------------------------------------------------------------------
// Resource type definitions
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResourceType {
    // Workloads
    Deployment,
    StatefulSet,
    DaemonSet,
    ReplicaSet,
    Pod,
    CronJob,
    Job,
    HorizontalPodAutoscaler,
    // Config
    ConfigMap,
    Secret,
    // Network
    Service,
    Ingress,
    Endpoints,
    NetworkPolicy,
    // Storage
    PersistentVolumeClaim,
    PersistentVolume,
    StorageClass,
    // RBAC
    ServiceAccount,
    Role,
    RoleBinding,
    ClusterRole,
    ClusterRoleBinding,
    // Cluster
    Node,
    Namespace,
    Event,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Category {
    Workloads,
    Config,
    Network,
    Storage,
    Rbac,
    Cluster,
}

impl ResourceType {
    pub fn display_name(&self) -> &'static str {
        match self {
            | Self::Deployment => "Deployments",
            | Self::StatefulSet => "StatefulSets",
            | Self::DaemonSet => "DaemonSets",
            | Self::ReplicaSet => "ReplicaSets",
            | Self::Pod => "Pods",
            | Self::CronJob => "CronJobs",
            | Self::Job => "Jobs",
            | Self::HorizontalPodAutoscaler => "HPAs",
            | Self::ConfigMap => "ConfigMaps",
            | Self::Secret => "Secrets",
            | Self::Service => "Services",
            | Self::Ingress => "Ingresses",
            | Self::Endpoints => "Endpoints",
            | Self::NetworkPolicy => "NetworkPolicies",
            | Self::PersistentVolumeClaim => "PVCs",
            | Self::PersistentVolume => "PVs",
            | Self::StorageClass => "StorageClasses",
            | Self::ServiceAccount => "ServiceAccounts",
            | Self::Role => "Roles",
            | Self::RoleBinding => "RoleBindings",
            | Self::ClusterRole => "ClusterRoles",
            | Self::ClusterRoleBinding => "ClusterRoleBindings",
            | Self::Node => "Nodes",
            | Self::Namespace => "Namespaces",
            | Self::Event => "Events",
        }
    }

    pub fn singular_name(&self) -> &'static str {
        match self {
            | Self::Deployment => "deployment",
            | Self::StatefulSet => "statefulset",
            | Self::DaemonSet => "daemonset",
            | Self::ReplicaSet => "replicaset",
            | Self::Pod => "pod",
            | Self::CronJob => "cronjob",
            | Self::Job => "job",
            | Self::HorizontalPodAutoscaler => "hpa",
            | Self::ConfigMap => "configmap",
            | Self::Secret => "secret",
            | Self::Service => "service",
            | Self::Ingress => "ingress",
            | Self::Endpoints => "endpoints",
            | Self::NetworkPolicy => "networkpolicy",
            | Self::PersistentVolumeClaim => "pvc",
            | Self::PersistentVolume => "pv",
            | Self::StorageClass => "storageclass",
            | Self::ServiceAccount => "serviceaccount",
            | Self::Role => "role",
            | Self::RoleBinding => "rolebinding",
            | Self::ClusterRole => "clusterrole",
            | Self::ClusterRoleBinding => "clusterrolebinding",
            | Self::Node => "node",
            | Self::Namespace => "namespace",
            | Self::Event => "event",
        }
    }

    pub fn column_headers(&self) -> Vec<&'static str> {
        match self {
            | Self::Deployment => vec!["NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE"],
            | Self::StatefulSet => vec!["NAME", "READY", "AGE"],
            | Self::DaemonSet => vec!["NAME", "DESIRED", "CURRENT", "READY", "UP-TO-DATE", "AGE"],
            | Self::ReplicaSet => vec!["NAME", "DESIRED", "CURRENT", "READY", "AGE"],
            | Self::Pod => vec!["NAME", "READY", "STATUS", "RESTARTS", "AGE"],
            | Self::CronJob => vec!["NAME", "SCHEDULE", "SUSPEND", "ACTIVE", "AGE"],
            | Self::Job => vec!["NAME", "COMPLETIONS", "DURATION", "AGE"],
            | Self::HorizontalPodAutoscaler => vec!["NAME", "REFERENCE", "MIN", "MAX", "REPLICAS", "AGE"],
            | Self::ConfigMap => vec!["NAME", "DATA", "AGE"],
            | Self::Secret => vec!["NAME", "TYPE", "DATA", "AGE"],
            | Self::Service => vec!["NAME", "TYPE", "CLUSTER-IP", "PORT(S)", "AGE"],
            | Self::Ingress => vec!["NAME", "CLASS", "HOSTS", "ADDRESS", "AGE"],
            | Self::Endpoints => vec!["NAME", "ENDPOINTS", "AGE"],
            | Self::NetworkPolicy => vec!["NAME", "POD-SELECTOR", "AGE"],
            | Self::PersistentVolumeClaim => vec!["NAME", "STATUS", "VOLUME", "CAPACITY", "AGE"],
            | Self::PersistentVolume => vec!["NAME", "CAPACITY", "RECLAIM POLICY", "STATUS", "CLAIM", "AGE"],
            | Self::StorageClass => vec!["NAME", "PROVISIONER", "RECLAIM POLICY", "AGE"],
            | Self::ServiceAccount => vec!["NAME", "SECRETS", "AGE"],
            | Self::Role => vec!["NAME", "AGE"],
            | Self::RoleBinding => vec!["NAME", "ROLE", "AGE"],
            | Self::ClusterRole => vec!["NAME", "AGE"],
            | Self::ClusterRoleBinding => vec!["NAME", "ROLE", "AGE"],
            | Self::Node => vec!["NAME", "STATUS", "ROLES", "VERSION", "AGE"],
            | Self::Namespace => vec!["NAME", "STATUS", "AGE"],
            | Self::Event => vec!["NAME", "TYPE", "REASON", "OBJECT", "AGE"],
        }
    }

    pub fn supports_logs(&self) -> bool {
        matches!(
            self,
            Self::Deployment
                | Self::StatefulSet
                | Self::DaemonSet
                | Self::ReplicaSet
                | Self::Pod
                | Self::CronJob
                | Self::Job
        )
    }

    pub fn supports_scale(&self) -> bool {
        matches!(self, Self::Deployment | Self::StatefulSet | Self::ReplicaSet)
    }

    pub fn supports_exec(&self) -> bool {
        matches!(self, Self::Pod)
    }

    pub fn is_cluster_scoped(&self) -> bool {
        matches!(
            self,
            Self::Node
                | Self::Namespace
                | Self::PersistentVolume
                | Self::StorageClass
                | Self::ClusterRole
                | Self::ClusterRoleBinding
        )
    }

    /// Returns the kube `ApiResource` descriptor for dynamic API operations.
    pub fn api_resource(&self) -> kube::api::ApiResource {
        use kube::api::ApiResource;
        match self {
            | Self::Deployment => ApiResource::erase::<Deployment>(&()),
            | Self::StatefulSet => ApiResource::erase::<StatefulSet>(&()),
            | Self::DaemonSet => ApiResource::erase::<DaemonSet>(&()),
            | Self::ReplicaSet => ApiResource::erase::<ReplicaSet>(&()),
            | Self::Pod => ApiResource::erase::<Pod>(&()),
            | Self::CronJob => ApiResource::erase::<CronJob>(&()),
            | Self::Job => ApiResource::erase::<Job>(&()),
            | Self::HorizontalPodAutoscaler => ApiResource::erase::<HorizontalPodAutoscaler>(&()),
            | Self::ConfigMap => ApiResource::erase::<ConfigMap>(&()),
            | Self::Secret => ApiResource::erase::<Secret>(&()),
            | Self::Service => ApiResource::erase::<Service>(&()),
            | Self::Ingress => ApiResource::erase::<Ingress>(&()),
            | Self::Endpoints => ApiResource::erase::<Endpoints>(&()),
            | Self::NetworkPolicy => ApiResource::erase::<NetworkPolicy>(&()),
            | Self::PersistentVolumeClaim => ApiResource::erase::<PersistentVolumeClaim>(&()),
            | Self::PersistentVolume => ApiResource::erase::<PersistentVolume>(&()),
            | Self::StorageClass => ApiResource::erase::<StorageClass>(&()),
            | Self::ServiceAccount => ApiResource::erase::<ServiceAccount>(&()),
            | Self::Role => ApiResource::erase::<Role>(&()),
            | Self::RoleBinding => ApiResource::erase::<RoleBinding>(&()),
            | Self::ClusterRole => ApiResource::erase::<ClusterRole>(&()),
            | Self::ClusterRoleBinding => ApiResource::erase::<ClusterRoleBinding>(&()),
            | Self::Node => ApiResource::erase::<Node>(&()),
            | Self::Namespace => ApiResource::erase::<Namespace>(&()),
            | Self::Event => ApiResource::erase::<Event>(&()),
        }
    }

    pub fn all_by_category() -> Vec<(Category, Vec<ResourceType>)> {
        vec![
            (Category::Workloads, vec![
                Self::Deployment,
                Self::StatefulSet,
                Self::DaemonSet,
                Self::ReplicaSet,
                Self::Pod,
                Self::CronJob,
                Self::Job,
                Self::HorizontalPodAutoscaler,
            ]),
            (Category::Network, vec![
                Self::Service,
                Self::Ingress,
                Self::Endpoints,
                Self::NetworkPolicy,
            ]),
            (Category::Config, vec![Self::ConfigMap, Self::Secret]),
            (Category::Storage, vec![
                Self::PersistentVolumeClaim,
                Self::PersistentVolume,
                Self::StorageClass,
            ]),
            (Category::Rbac, vec![
                Self::ServiceAccount,
                Self::Role,
                Self::RoleBinding,
                Self::ClusterRole,
                Self::ClusterRoleBinding,
            ]),
            (Category::Cluster, vec![Self::Node, Self::Namespace, Self::Event]),
        ]
    }
}

impl Category {
    pub fn display_name(&self) -> &'static str {
        match self {
            | Self::Workloads => "Workloads",
            | Self::Config => "Config",
            | Self::Network => "Network",
            | Self::Storage => "Storage",
            | Self::Rbac => "RBAC",
            | Self::Cluster => "Cluster",
        }
    }
}

// ---------------------------------------------------------------------------
// Resource entry (one row in the table)
// ---------------------------------------------------------------------------

pub struct ResourceEntry {
    pub name: String,
    pub namespace: String,
    pub columns: Vec<String>,
    /// Optional sort key (e.g. ISO 8601 timestamp for Events). Not displayed.
    pub sort_key: Option<String>,
}

// ---------------------------------------------------------------------------
// Pod info (for log view pod/container selection)
// ---------------------------------------------------------------------------

pub struct PodInfo {
    pub name: String,
    pub containers: Vec<String>,
}

pub struct RelatedResource {
    pub resource_type: ResourceType,
    pub name: String,
    pub namespace: String,
    pub info: String,
    pub category: &'static str,
    pub sort_key: String,
}

pub struct RelatedEvent {
    pub type_: String,
    pub reason: String,
    pub message: String,
    pub count: i32,
    pub last_seen: String,
}

// ---------------------------------------------------------------------------
// Cluster stats
// ---------------------------------------------------------------------------

pub struct NodeStats {
    pub name: String,
    pub status: String,
    pub unschedulable: bool,
    pub roles: String,
    pub version: String,
    pub os_arch: String,
    pub cpu_capacity: String,
    pub cpu_allocatable: String,
    pub mem_capacity: String,
    pub mem_allocatable: String,
    pub pods_capacity: String,
    pub pods_allocatable: String,
    pub age: String,
}

pub struct ClusterStatsData {
    pub server_version: String,
    pub node_count: usize,
    pub nodes_ready: usize,
    pub nodes_not_ready: usize,
    pub nodes_cordoned: usize,
    pub namespace_count: usize,
    pub pod_count: usize,
    pub pods_running: usize,
    pub pods_pending: usize,
    pub pods_failed: usize,
    pub deployment_count: usize,
    pub service_count: usize,
    pub pods_crash_loop: usize,
    pub pods_error: usize,
    pub recent_warnings: usize,
    pub nodes_with_pressure: usize,
    pub nodes: Vec<NodeStats>,
}

// ---------------------------------------------------------------------------
// Kubernetes client
// ---------------------------------------------------------------------------

pub struct KubeClient {
    client: Client,
    kubeconfig: Kubeconfig,
    kubeconfig_path: Option<String>,
    current_context: String,
    current_namespace: Option<String>,
}

impl KubeClient {
    pub async fn new(
        kubeconfig_path: Option<String>,
        context: Option<String>,
        namespace: Option<String>,
    ) -> Result<Self> {
        let kubeconfig = match &kubeconfig_path {
            | Some(path) => Kubeconfig::read_from(path).context("Failed to read kubeconfig")?,
            | None => Kubeconfig::read().context("Failed to read default kubeconfig")?,
        };

        let current_context = context
            .or_else(|| kubeconfig.current_context.clone())
            .unwrap_or_default();

        let options = KubeConfigOptions {
            context: Some(current_context.clone()),
            ..Default::default()
        };
        let config = kube::Config::from_kubeconfig(&options)
            .await
            .context("Failed to build kube config from kubeconfig")?;

        let client = Client::try_from(config)?;
        // Explicit --namespace flag sets a specific namespace; otherwise default to all
        let current_namespace = namespace;

        Ok(Self {
            client,
            kubeconfig,
            kubeconfig_path,
            current_context,
            current_namespace,
        })
    }

    pub fn kubeconfig_path(&self) -> Option<&str> {
        self.kubeconfig_path.as_deref()
    }

    pub fn contexts(&self) -> Vec<String> {
        self.kubeconfig.contexts.iter().map(|c| c.name.clone()).collect()
    }

    pub fn current_context(&self) -> &str {
        &self.current_context
    }

    /// Returns the current namespace filter, or None if all namespaces.
    pub fn current_namespace(&self) -> Option<&str> {
        self.current_namespace.as_deref()
    }

    /// Returns a display string for the namespace: the name or "All
    /// Namespaces".
    pub fn namespace_display(&self) -> &str {
        self.current_namespace.as_deref().unwrap_or("All Namespaces")
    }

    pub fn is_all_namespaces(&self) -> bool {
        self.current_namespace.is_none()
    }

    pub fn set_namespace(&mut self, ns: Option<String>) {
        self.current_namespace = ns;
    }

    pub async fn switch_context(&mut self, ctx: &str) -> Result<()> {
        let options = KubeConfigOptions {
            context: Some(ctx.to_string()),
            ..Default::default()
        };
        let config = kube::Config::from_kubeconfig(&options).await?;
        self.current_namespace = None;
        self.client = Client::try_from(config)?;
        self.current_context = ctx.to_string();
        Ok(())
    }

    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        let api: Api<Namespace> = Api::all(self.client.clone());
        let list = api.list(&ListParams::default()).await?;
        let mut names: Vec<String> = list.items.iter().filter_map(|ns| ns.metadata.name.clone()).collect();
        names.sort();
        Ok(names)
    }

    pub async fn fetch_cluster_stats(&self) -> Result<ClusterStatsData> {
        // Server version
        let server_version = match self.client.apiserver_version().await {
            | Ok(info) => format!("{}.{}", info.major, info.minor),
            | Err(_) => "unknown".into(),
        };

        // Nodes
        let node_api: Api<Node> = Api::all(self.client.clone());
        let node_list = node_api.list(&ListParams::default()).await?;
        let mut nodes_ready = 0usize;
        let mut nodes_not_ready = 0usize;
        let mut node_stats = Vec::new();

        for n in &node_list.items {
            let status = n.status.as_ref();
            let conditions = status.and_then(|s| s.conditions.as_ref());
            let is_ready = conditions
                .and_then(|c| c.iter().find(|c| c.type_ == "Ready"))
                .map(|c| c.status == "True")
                .unwrap_or(false);
            if is_ready {
                nodes_ready += 1;
            } else {
                nodes_not_ready += 1;
            }

            let roles = n
                .metadata
                .labels
                .as_ref()
                .map(|labels| {
                    labels
                        .keys()
                        .filter_map(|k| {
                            k.strip_prefix("node-role.kubernetes.io/")
                                .map(|r| if r.is_empty() { "worker" } else { r })
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .unwrap_or_default();

            let info = status.and_then(|s| s.node_info.as_ref());
            let capacity = status.and_then(|s| s.capacity.as_ref());
            let allocatable = status.and_then(|s| s.allocatable.as_ref());

            let get_res = |map: Option<
                &std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
            >,
                           key: &str|
             -> String {
                map.and_then(|m| m.get(key))
                    .map(|q| q.0.clone())
                    .unwrap_or_else(|| "-".into())
            };

            let unschedulable = n.spec.as_ref().and_then(|s| s.unschedulable).unwrap_or(false);
            let node_status = match (is_ready, unschedulable) {
                | (true, false) => "Ready".to_string(),
                | (true, true) => "Ready,SchedulingDisabled".to_string(),
                | (false, false) => "NotReady".to_string(),
                | (false, true) => "NotReady,SchedulingDisabled".to_string(),
            };

            node_stats.push(NodeStats {
                name: meta_name(&n.metadata),
                status: node_status,
                unschedulable,
                roles: if roles.is_empty() { "<none>".into() } else { roles },
                version: info.map(|i| i.kubelet_version.clone()).unwrap_or_default(),
                os_arch: info
                    .map(|i| format!("{}/{}", i.operating_system, i.architecture))
                    .unwrap_or_default(),
                cpu_capacity: get_res(capacity, "cpu"),
                cpu_allocatable: get_res(allocatable, "cpu"),
                mem_capacity: format_memory_gb(&get_res(capacity, "memory")),
                mem_allocatable: format_memory_gb(&get_res(allocatable, "memory")),
                pods_capacity: get_res(capacity, "pods"),
                pods_allocatable: get_res(allocatable, "pods"),
                age: format_age(n.metadata.creation_timestamp.as_ref()),
            });
        }

        // Namespaces
        let ns_api: Api<Namespace> = Api::all(self.client.clone());
        let ns_count = ns_api.list(&ListParams::default()).await?.items.len();

        // Pods
        let pod_api: Api<Pod> = Api::all(self.client.clone());
        let pod_list = pod_api.list(&ListParams::default()).await?;
        let pod_count = pod_list.items.len();
        let mut pods_running = 0usize;
        let mut pods_pending = 0usize;
        let mut pods_failed = 0usize;
        let mut pods_crash_loop = 0usize;
        let mut pods_error = 0usize;
        for p in &pod_list.items {
            match p.status.as_ref().and_then(|s| s.phase.as_deref()) {
                | Some("Running") => pods_running += 1,
                | Some("Pending") => pods_pending += 1,
                | Some("Failed") => pods_failed += 1,
                | _ => {},
            }
            let container_statuses = p.status.as_ref().and_then(|s| s.container_statuses.as_ref());
            if let Some(statuses) = container_statuses {
                for cs in statuses {
                    if let Some(waiting) = cs.state.as_ref().and_then(|s| s.waiting.as_ref()) {
                        match waiting.reason.as_deref() {
                            | Some("CrashLoopBackOff") => pods_crash_loop += 1,
                            | Some("ImagePullBackOff") | Some("ErrImagePull") | Some("CreateContainerConfigError") => {
                                pods_error += 1
                            },
                            | _ => {},
                        }
                    }
                }
            }
        }

        // Deployments & Services
        let dep_api: Api<Deployment> = Api::all(self.client.clone());
        let deployment_count = dep_api.list(&ListParams::default()).await?.items.len();
        let svc_api: Api<Service> = Api::all(self.client.clone());
        let service_count = svc_api.list(&ListParams::default()).await?.items.len();

        // Nodes with resource pressure
        let mut nodes_with_pressure = 0usize;
        for n in &node_list.items {
            if let Some(conditions) = n.status.as_ref().and_then(|s| s.conditions.as_ref()) {
                for c in conditions {
                    if matches!(c.type_.as_str(), "DiskPressure" | "MemoryPressure" | "PIDPressure")
                        && c.status == "True"
                    {
                        nodes_with_pressure += 1;
                        break;
                    }
                }
            }
        }

        // Recent warning events (last 1 hour)
        let event_api: Api<Event> = Api::all(self.client.clone());
        let recent_warnings = if let Ok(events) = event_api.list(&ListParams::default()).await {
            let cutoff = Timestamp::now() - jiff::SignedDuration::from_hours(1);
            events
                .items
                .iter()
                .filter(|e| {
                    e.type_.as_deref() == Some("Warning")
                        && e.event_time
                            .as_ref()
                            .map(|t| t.0)
                            .or(e.deprecated_last_timestamp.as_ref().map(|t| t.0))
                            .map(|t| t >= cutoff)
                            .unwrap_or(false)
                })
                .count()
        } else {
            0
        };

        let nodes_cordoned = node_stats.iter().filter(|n| n.unschedulable).count();

        Ok(ClusterStatsData {
            server_version,
            node_count: node_list.items.len(),
            nodes_ready,
            nodes_not_ready,
            nodes_cordoned,
            namespace_count: ns_count,
            pod_count,
            pods_running,
            pods_pending,
            pods_failed,
            deployment_count,
            service_count,
            pods_crash_loop,
            pods_error,
            recent_warnings,
            nodes_with_pressure,
            nodes: node_stats,
        })
    }

    pub async fn list_resources(&self, rt: ResourceType) -> Result<Vec<ResourceEntry>> {
        match rt {
            | ResourceType::Deployment => self.list_typed::<Deployment>(Self::map_deployment).await,
            | ResourceType::StatefulSet => self.list_typed::<StatefulSet>(Self::map_statefulset).await,
            | ResourceType::DaemonSet => self.list_typed::<DaemonSet>(Self::map_daemonset).await,
            | ResourceType::ReplicaSet => self.list_typed::<ReplicaSet>(Self::map_replicaset).await,
            | ResourceType::Pod => self.list_typed::<Pod>(Self::map_pod).await,
            | ResourceType::CronJob => self.list_typed::<CronJob>(Self::map_cronjob).await,
            | ResourceType::Job => self.list_typed::<Job>(Self::map_job).await,
            | ResourceType::HorizontalPodAutoscaler => self.list_typed::<HorizontalPodAutoscaler>(Self::map_hpa).await,
            | ResourceType::ConfigMap => self.list_typed::<ConfigMap>(Self::map_configmap).await,
            | ResourceType::Secret => self.list_typed::<Secret>(Self::map_secret).await,
            | ResourceType::Service => self.list_typed::<Service>(Self::map_service).await,
            | ResourceType::Ingress => self.list_typed::<Ingress>(Self::map_ingress).await,
            | ResourceType::Endpoints => self.list_typed::<Endpoints>(Self::map_endpoints).await,
            | ResourceType::NetworkPolicy => self.list_typed::<NetworkPolicy>(Self::map_network_policy).await,
            | ResourceType::PersistentVolumeClaim => self.list_typed::<PersistentVolumeClaim>(Self::map_pvc).await,
            | ResourceType::PersistentVolume => self.list_cluster::<PersistentVolume>(Self::map_pv).await,
            | ResourceType::StorageClass => self.list_cluster::<StorageClass>(Self::map_storage_class).await,
            | ResourceType::ServiceAccount => self.list_typed::<ServiceAccount>(Self::map_service_account).await,
            | ResourceType::Role => self.list_typed::<Role>(Self::map_role).await,
            | ResourceType::RoleBinding => self.list_typed::<RoleBinding>(Self::map_role_binding).await,
            | ResourceType::ClusterRole => self.list_cluster::<ClusterRole>(Self::map_cluster_role).await,
            | ResourceType::ClusterRoleBinding => {
                self.list_cluster::<ClusterRoleBinding>(Self::map_cluster_role_binding)
                    .await
            },
            | ResourceType::Node => self.list_cluster::<Node>(Self::map_node).await,
            | ResourceType::Namespace => self.list_cluster::<Namespace>(Self::map_namespace).await,
            | ResourceType::Event => self.list_typed::<Event>(Self::map_event).await,
        }
    }

    pub async fn get_resource(&self, rt: ResourceType, ns: &str, name: &str) -> Result<Value> {
        match rt {
            // Cluster-scoped
            | ResourceType::PersistentVolume => self.get_value_cluster::<PersistentVolume>(name).await,
            | ResourceType::StorageClass => self.get_value_cluster::<StorageClass>(name).await,
            | ResourceType::ClusterRole => self.get_value_cluster::<ClusterRole>(name).await,
            | ResourceType::ClusterRoleBinding => self.get_value_cluster::<ClusterRoleBinding>(name).await,
            | ResourceType::Node => self.get_value_cluster::<Node>(name).await,
            | ResourceType::Namespace => self.get_value_cluster::<Namespace>(name).await,
            // Namespaced
            | ResourceType::Deployment => self.get_value::<Deployment>(ns, name).await,
            | ResourceType::StatefulSet => self.get_value::<StatefulSet>(ns, name).await,
            | ResourceType::DaemonSet => self.get_value::<DaemonSet>(ns, name).await,
            | ResourceType::ReplicaSet => self.get_value::<ReplicaSet>(ns, name).await,
            | ResourceType::Pod => self.get_value::<Pod>(ns, name).await,
            | ResourceType::CronJob => self.get_value::<CronJob>(ns, name).await,
            | ResourceType::Job => self.get_value::<Job>(ns, name).await,
            | ResourceType::HorizontalPodAutoscaler => self.get_value::<HorizontalPodAutoscaler>(ns, name).await,
            | ResourceType::ConfigMap => self.get_value::<ConfigMap>(ns, name).await,
            | ResourceType::Secret => self.get_value::<Secret>(ns, name).await,
            | ResourceType::Service => self.get_value::<Service>(ns, name).await,
            | ResourceType::Ingress => self.get_value::<Ingress>(ns, name).await,
            | ResourceType::Endpoints => self.get_value::<Endpoints>(ns, name).await,
            | ResourceType::NetworkPolicy => self.get_value::<NetworkPolicy>(ns, name).await,
            | ResourceType::PersistentVolumeClaim => self.get_value::<PersistentVolumeClaim>(ns, name).await,
            | ResourceType::ServiceAccount => self.get_value::<ServiceAccount>(ns, name).await,
            | ResourceType::Role => self.get_value::<Role>(ns, name).await,
            | ResourceType::RoleBinding => self.get_value::<RoleBinding>(ns, name).await,
            | ResourceType::Event => self.get_value::<Event>(ns, name).await,
        }
    }

    // -- Edit / replace resource -----------------------------------------------

    /// Replace a resource with edited YAML. Uses the kube dynamic API so it
    /// works for any resource type without per-type match arms.
    pub async fn replace_resource_yaml(&self, rt: ResourceType, ns: &str, name: &str, yaml: &str) -> Result<Value> {
        let obj: kube::api::DynamicObject = serde_yaml::from_str(yaml).context("Invalid YAML")?;
        let ar = rt.api_resource();
        let api: Api<kube::api::DynamicObject> = if rt.is_cluster_scoped() {
            Api::all_with(self.client.clone(), &ar)
        } else {
            Api::namespaced_with(self.client.clone(), ns, &ar)
        };
        let pp = kube::api::PostParams::default();
        let result = api.replace(name, &pp, &obj).await.context("Failed to apply resource")?;
        let val = serde_json::to_value(&result)?;
        Ok(val)
    }

    pub async fn patch_metadata(
        &self,
        rt: ResourceType,
        ns: &str,
        name: &str,
        labels: Option<&serde_json::Map<String, Value>>,
        annotations: Option<&serde_json::Map<String, Value>>,
    ) -> Result<Value> {
        let ar = rt.api_resource();
        let api: Api<kube::api::DynamicObject> = if rt.is_cluster_scoped() {
            Api::all_with(self.client.clone(), &ar)
        } else {
            Api::namespaced_with(self.client.clone(), ns, &ar)
        };
        let mut metadata = serde_json::Map::new();
        if let Some(l) = labels {
            metadata.insert("labels".into(), Value::Object(l.clone()));
        }
        if let Some(a) = annotations {
            metadata.insert("annotations".into(), Value::Object(a.clone()));
        }
        let patch = serde_json::json!({ "metadata": metadata });
        let pp = kube::api::PatchParams::default();
        let result = api
            .patch(name, &pp, &kube::api::Patch::Merge(&patch))
            .await
            .context("Failed to patch metadata")?;
        let val = serde_json::to_value(&result)?;
        Ok(val)
    }

    pub async fn restart_workload(&self, rt: ResourceType, ns: &str, name: &str) -> Result<()> {
        let ar = rt.api_resource();
        let api: Api<kube::api::DynamicObject> = Api::namespaced_with(self.client.clone(), ns, &ar);
        let now = Timestamp::now().to_string();
        let patch = serde_json::json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": now
                        }
                    }
                }
            }
        });
        let pp = kube::api::PatchParams::default();
        api.patch(name, &pp, &kube::api::Patch::Merge(&patch))
            .await
            .context("Failed to restart workload")?;
        Ok(())
    }

    pub async fn create_resource_yaml(&self, yaml: &str) -> Result<Value> {
        let value: Value = serde_yaml::from_str(yaml).context("Invalid YAML")?;

        // Extract apiVersion, kind, metadata from the YAML to determine the API
        // resource
        let api_version = value.get("apiVersion").and_then(|v| v.as_str()).unwrap_or("");
        let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let namespace = value
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
            .unwrap_or("");

        if kind.is_empty() || api_version.is_empty() {
            anyhow::bail!("YAML must contain apiVersion and kind");
        }

        // Parse the group and version from apiVersion (e.g., "apps/v1" -> group="apps",
        // version="v1")
        let (group, version) = if api_version.contains('/') {
            let parts: Vec<&str> = api_version.splitn(2, '/').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            (String::new(), api_version.to_string())
        };

        // Build plural from kind (simple lowercase + s, covers most cases)
        let plural = format!("{}s", kind.to_lowercase());

        let ar = kube::api::ApiResource {
            group,
            version,
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            plural,
        };

        let obj: kube::api::DynamicObject = serde_yaml::from_str(yaml).context("Invalid YAML for DynamicObject")?;
        // If no namespace in YAML, use current namespace (or "default")
        let effective_ns = if namespace.is_empty() {
            self.current_namespace.as_deref().unwrap_or("default")
        } else {
            namespace
        };
        let api: Api<kube::api::DynamicObject> = Api::namespaced_with(self.client.clone(), effective_ns, &ar);

        let pp = kube::api::PostParams::default();
        let result = api.create(&pp, &obj).await.context("Failed to create resource")?;
        let val = serde_json::to_value(&result)?;
        Ok(val)
    }

    pub async fn delete_resource(&self, rt: ResourceType, ns: &str, name: &str) -> Result<()> {
        let ar = rt.api_resource();
        let api: Api<kube::api::DynamicObject> = if rt.is_cluster_scoped() {
            Api::all_with(self.client.clone(), &ar)
        } else {
            Api::namespaced_with(self.client.clone(), ns, &ar)
        };
        let dp = kube::api::DeleteParams::default();
        api.delete(name, &dp).await.context("Failed to delete resource")?;
        Ok(())
    }

    pub async fn scale_resource(&self, rt: ResourceType, ns: &str, name: &str, replicas: u32) -> Result<()> {
        let ar = rt.api_resource();
        let api: Api<kube::api::DynamicObject> = Api::namespaced_with(self.client.clone(), ns, &ar);
        let patch = serde_json::json!({ "spec": { "replicas": replicas } });
        let pp = kube::api::PatchParams::default();
        api.patch(name, &pp, &kube::api::Patch::Merge(&patch))
            .await
            .context("Failed to scale resource")?;
        Ok(())
    }

    /// Generate a default job name for a manually triggered CronJob.
    pub fn default_trigger_job_name(cronjob_name: &str) -> String {
        let ts = Timestamp::now().strftime("%Y%m%d%H%M%S").to_string();
        format!("{}-manual-{}", cronjob_name, ts)
    }

    /// Create a Job from a CronJob's jobTemplate (manual trigger).
    pub async fn trigger_cronjob(&self, ns: &str, name: &str, job_name: &str) -> Result<String> {
        let cj_api: Api<CronJob> = Api::namespaced(self.client.clone(), ns);
        let cj = cj_api.get(name).await.context("Failed to get CronJob")?;

        let job_template = cj
            .spec
            .as_ref()
            .map(|s| &s.job_template)
            .ok_or_else(|| anyhow::anyhow!("CronJob has no spec"))?;

        let mut job_meta = job_template.metadata.clone().unwrap_or_default();
        job_meta.name = Some(job_name.to_string());
        job_meta.namespace = Some(ns.to_string());
        // Add owner reference so the job shows up as related
        job_meta.owner_references = Some(vec![k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
            api_version: "batch/v1".to_string(),
            kind: "CronJob".to_string(),
            name: name.to_string(),
            uid: cj.metadata.uid.clone().unwrap_or_default(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }]);
        // Annotate as manually triggered
        let annotations = job_meta.annotations.get_or_insert_with(Default::default);
        annotations.insert("cronjob.kubernetes.io/instantiate".to_string(), "manual".to_string());

        let job = Job {
            metadata: job_meta,
            spec: job_template.spec.clone(),
            ..Default::default()
        };

        let job_api: Api<Job> = Api::namespaced(self.client.clone(), ns);
        let pp = kube::api::PostParams::default();
        job_api
            .create(&pp, &job)
            .await
            .context("Failed to create Job from CronJob")?;

        Ok(job_name.to_string())
    }

    /// Cordon a node (mark as unschedulable).
    pub async fn cordon_node(&self, name: &str) -> Result<()> {
        let api: Api<Node> = Api::all(self.client.clone());
        let patch = serde_json::json!({ "spec": { "unschedulable": true } });
        let pp = kube::api::PatchParams::default();
        api.patch(name, &pp, &kube::api::Patch::Merge(&patch))
            .await
            .context("Failed to cordon node")?;
        Ok(())
    }

    /// Uncordon a node (mark as schedulable).
    pub async fn uncordon_node(&self, name: &str) -> Result<()> {
        let api: Api<Node> = Api::all(self.client.clone());
        let patch = serde_json::json!({ "spec": { "unschedulable": false } });
        let pp = kube::api::PatchParams::default();
        api.patch(name, &pp, &kube::api::Patch::Merge(&patch))
            .await
            .context("Failed to uncordon node")?;
        Ok(())
    }

    /// Drain a node: cordon it, then evict all non-DaemonSet, non-mirror pods.
    pub async fn drain_node(&self, name: &str) -> Result<usize> {
        // Step 1: cordon
        self.cordon_node(name).await?;

        // Step 2: list pods on this node
        let pod_api: Api<Pod> = Api::all(self.client.clone());
        let lp = ListParams::default().fields(&format!("spec.nodeName={}", name));
        let pods = pod_api.list(&lp).await.context("Failed to list pods on node")?;

        let mut evicted = 0;
        for pod in &pods.items {
            let meta = &pod.metadata;
            let pod_name = meta.name.as_deref().unwrap_or("");
            let ns = meta.namespace.as_deref().unwrap_or("default");

            // Skip DaemonSet-owned pods
            if let Some(owners) = &meta.owner_references {
                if owners.iter().any(|o| o.kind == "DaemonSet") {
                    continue;
                }
            }

            // Skip mirror pods (created by kubelet)
            if let Some(annotations) = &meta.annotations {
                if annotations.contains_key("kubernetes.io/config.mirror") {
                    continue;
                }
            }

            // Evict the pod
            let ns_api: Api<Pod> = Api::namespaced(self.client.clone(), ns);
            let ep = kube::api::EvictParams::default();
            match ns_api.evict(pod_name, &ep).await {
                | Ok(_) => evicted += 1,
                | Err(_) => {
                    // Continue — some pods may have PDBs blocking eviction
                },
            }
        }

        Ok(evicted)
    }

    pub async fn fetch_related_events(&self, ns: &str, resource_name: &str) -> Result<Vec<RelatedEvent>> {
        let api: Api<Event> = if ns.is_empty() {
            Api::all(self.client.clone())
        } else {
            Api::namespaced(self.client.clone(), ns)
        };
        let lp = ListParams::default().fields(&format!("regarding.name={}", resource_name));
        let list = api.list(&lp).await.context("Failed to fetch events")?;
        let mut events: Vec<RelatedEvent> = list
            .items
            .iter()
            .map(|e| {
                let last_seen = e
                    .event_time
                    .as_ref()
                    .map(|t| {
                        let dur = jiff::Timestamp::now().duration_since(t.0);
                        format_duration(dur)
                    })
                    .or_else(|| e.deprecated_last_timestamp.as_ref().map(|t| format_age(Some(t))))
                    .unwrap_or_else(|| "-".into());
                let count = e.series.as_ref().map(|s| s.count).or(e.deprecated_count).unwrap_or(1);
                RelatedEvent {
                    type_: e.type_.clone().unwrap_or_default(),
                    reason: e.reason.clone().unwrap_or_default(),
                    message: e.note.clone().unwrap_or_default(),
                    count,
                    last_seen,
                }
            })
            .collect();
        events.reverse();
        Ok(events)
    }

    pub async fn fetch_related_resources(
        &self,
        rt: ResourceType,
        ns: &str,
        name: &str,
        value: &Value,
    ) -> Vec<RelatedResource> {
        let mut related = Vec::new();

        // Owner references (universal — all resource types)
        Self::add_owner_refs(&mut related, value, ns);

        match rt {
            | ResourceType::Deployment => {
                self.add_replicasets_for_deployment(&mut related, ns, value).await;
                self.add_pods_by_selector(&mut related, ns, value).await;
                self.add_hpa_for_workload(&mut related, ns, name).await;
                Self::add_service_account(&mut related, value, ns, true);
            },
            | ResourceType::StatefulSet => {
                self.add_pods_by_selector(&mut related, ns, value).await;
                self.add_hpa_for_workload(&mut related, ns, name).await;
                Self::add_service_account(&mut related, value, ns, true);
                if let Some(templates) = value
                    .get("spec")
                    .and_then(|s| s.get("volumeClaimTemplates"))
                    .and_then(|v| v.as_array())
                {
                    for tpl in templates {
                        if let Some(pvc_name) = tpl.get("metadata").and_then(|m| m.get("name")).and_then(|n| n.as_str())
                        {
                            Self::push_unique(
                                &mut related,
                                ResourceType::PersistentVolumeClaim,
                                pvc_name,
                                ns,
                                "template",
                                "Storage",
                            );
                        }
                    }
                }
            },
            | ResourceType::DaemonSet => {
                self.add_pods_by_selector(&mut related, ns, value).await;
                Self::add_service_account(&mut related, value, ns, true);
            },
            | ResourceType::ReplicaSet => {
                self.add_pods_by_selector(&mut related, ns, value).await;
                Self::add_service_account(&mut related, value, ns, true);
            },
            | ResourceType::Pod => {
                Self::add_service_account(&mut related, value, ns, false);
                if let Some(node) = value
                    .get("spec")
                    .and_then(|s| s.get("nodeName"))
                    .and_then(|n| n.as_str())
                {
                    Self::push_unique(&mut related, ResourceType::Node, node, "", "scheduled", "Cluster");
                }
            },
            | ResourceType::Service => {
                Self::push_unique(&mut related, ResourceType::Endpoints, name, ns, "endpoints", "Network");
                self.add_pods_by_service_selector(&mut related, ns, value).await;
            },
            | ResourceType::Ingress => {
                if let Some(rules) = value
                    .get("spec")
                    .and_then(|s| s.get("rules"))
                    .and_then(|r| r.as_array())
                {
                    for rule in rules {
                        if let Some(paths) = rule.get("http").and_then(|h| h.get("paths")).and_then(|p| p.as_array()) {
                            for path in paths {
                                if let Some(svc) = path
                                    .get("backend")
                                    .and_then(|b| b.get("service"))
                                    .and_then(|s| s.get("name"))
                                    .and_then(|n| n.as_str())
                                {
                                    Self::push_unique(
                                        &mut related,
                                        ResourceType::Service,
                                        svc,
                                        ns,
                                        "backend",
                                        "Network",
                                    );
                                }
                            }
                        }
                    }
                }
                if let Some(tls) = value.get("spec").and_then(|s| s.get("tls")).and_then(|t| t.as_array()) {
                    for entry in tls {
                        if let Some(secret) = entry.get("secretName").and_then(|s| s.as_str()) {
                            Self::push_unique(&mut related, ResourceType::Secret, secret, ns, "tls", "Config");
                        }
                    }
                }
            },
            | ResourceType::Job => {
                self.add_pods_by_label(&mut related, ns, &format!("job-name={}", name))
                    .await;
                Self::add_service_account(&mut related, value, ns, true);
            },
            | ResourceType::CronJob => {
                let job_api: Api<Job> = Api::namespaced(self.client.clone(), ns);
                if let Ok(jobs) = job_api.list(&ListParams::default()).await {
                    for job in &jobs.items {
                        let is_owned = job
                            .metadata
                            .owner_references
                            .as_ref()
                            .map(|refs| refs.iter().any(|r| r.kind == "CronJob" && r.name == name))
                            .unwrap_or(false);
                        if is_owned {
                            let ts = job
                                .metadata
                                .creation_timestamp
                                .as_ref()
                                .map(|t| t.0.to_string())
                                .unwrap_or_default();
                            let status = if job.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0) > 0 {
                                "Succeeded"
                            } else if job.status.as_ref().and_then(|s| s.active).unwrap_or(0) > 0 {
                                "Active"
                            } else {
                                "Unknown"
                            };
                            related.push(RelatedResource {
                                resource_type: ResourceType::Job,
                                name: meta_name(&job.metadata),
                                namespace: ns.to_string(),
                                info: status.to_string(),
                                category: ResourceType::Job.display_name(),
                                sort_key: ts,
                            });
                        }
                    }
                }
                // SA from jobTemplate
                if let Some(sa) = value
                    .get("spec")
                    .and_then(|s| s.get("jobTemplate"))
                    .and_then(|j| j.get("spec"))
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
                    .and_then(|s| s.get("serviceAccountName"))
                    .and_then(|n| n.as_str())
                {
                    Self::push_unique(
                        &mut related,
                        ResourceType::ServiceAccount,
                        sa,
                        ns,
                        "serviceAccount",
                        "RBAC",
                    );
                }
            },
            | ResourceType::HorizontalPodAutoscaler => {
                if let Some(target_ref) = value.get("spec").and_then(|s| s.get("scaleTargetRef")) {
                    let kind = target_ref.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                    let tname = target_ref.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let trt = match kind {
                        | "Deployment" => Some(ResourceType::Deployment),
                        | "StatefulSet" => Some(ResourceType::StatefulSet),
                        | "ReplicaSet" => Some(ResourceType::ReplicaSet),
                        | _ => None,
                    };
                    if let Some(trt) = trt {
                        Self::push_unique(&mut related, trt, tname, ns, "scale target", "Workloads");
                    }
                }
            },
            | ResourceType::PersistentVolumeClaim => {
                if let Some(vol) = value
                    .get("spec")
                    .and_then(|s| s.get("volumeName"))
                    .and_then(|v| v.as_str())
                {
                    Self::push_unique(
                        &mut related,
                        ResourceType::PersistentVolume,
                        vol,
                        "",
                        "bound",
                        "Storage",
                    );
                }
                if let Some(sc) = value
                    .get("spec")
                    .and_then(|s| s.get("storageClassName"))
                    .and_then(|v| v.as_str())
                {
                    Self::push_unique(
                        &mut related,
                        ResourceType::StorageClass,
                        sc,
                        "",
                        "storageClass",
                        "Storage",
                    );
                }
            },
            | ResourceType::PersistentVolume => {
                if let Some(claim) = value.get("spec").and_then(|s| s.get("claimRef")) {
                    let cn = claim.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let cns = claim.get("namespace").and_then(|n| n.as_str()).unwrap_or("");
                    if !cn.is_empty() {
                        Self::push_unique(
                            &mut related,
                            ResourceType::PersistentVolumeClaim,
                            cn,
                            cns,
                            "claim",
                            "Storage",
                        );
                    }
                }
                if let Some(sc) = value
                    .get("spec")
                    .and_then(|s| s.get("storageClassName"))
                    .and_then(|v| v.as_str())
                {
                    Self::push_unique(
                        &mut related,
                        ResourceType::StorageClass,
                        sc,
                        "",
                        "storageClass",
                        "Storage",
                    );
                }
            },
            | ResourceType::ServiceAccount => {
                // Token secrets
                if let Some(secrets) = value.get("secrets").and_then(|s| s.as_array()) {
                    for secret in secrets {
                        if let Some(sname) = secret.get("name").and_then(|n| n.as_str()) {
                            Self::push_unique(&mut related, ResourceType::Secret, sname, ns, "token", "x");
                        }
                    }
                }
                // RoleBindings referencing this SA
                let rb_api: Api<RoleBinding> = Api::namespaced(self.client.clone(), ns);
                if let Ok(list) = rb_api.list(&ListParams::default()).await {
                    for rb in &list.items {
                        let refs_sa = rb
                            .subjects
                            .as_ref()
                            .map(|subjects| subjects.iter().any(|s| s.kind == "ServiceAccount" && s.name == name))
                            .unwrap_or(false);
                        if refs_sa {
                            Self::push_unique(
                                &mut related,
                                ResourceType::RoleBinding,
                                &meta_name(&rb.metadata),
                                ns,
                                "binds SA",
                                "x",
                            );
                        }
                    }
                }
                // ClusterRoleBindings referencing this SA
                let crb_api: Api<ClusterRoleBinding> = Api::all(self.client.clone());
                if let Ok(list) = crb_api.list(&ListParams::default()).await {
                    for crb in &list.items {
                        let refs_sa = crb
                            .subjects
                            .as_ref()
                            .map(|subjects| {
                                subjects.iter().any(|s| {
                                    s.kind == "ServiceAccount" && s.name == name && s.namespace.as_deref() == Some(ns)
                                })
                            })
                            .unwrap_or(false);
                        if refs_sa {
                            Self::push_unique(
                                &mut related,
                                ResourceType::ClusterRoleBinding,
                                &meta_name(&crb.metadata),
                                "",
                                "binds SA",
                                "x",
                            );
                        }
                    }
                }
                // Workloads using this SA
                self.add_workloads_using_sa(&mut related, ns, name).await;
            },
            | ResourceType::RoleBinding | ResourceType::ClusterRoleBinding => {
                if let Some(role_ref) = value.get("roleRef") {
                    let kind = role_ref.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                    let rname = role_ref.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let rrt = match kind {
                        | "Role" => Some(ResourceType::Role),
                        | "ClusterRole" => Some(ResourceType::ClusterRole),
                        | _ => None,
                    };
                    if let Some(rrt) = rrt {
                        Self::push_unique(&mut related, rrt, rname, ns, "bound role", "RBAC");
                    }
                }
                if let Some(subjects) = value.get("subjects").and_then(|s| s.as_array()) {
                    for subj in subjects {
                        if subj.get("kind").and_then(|k| k.as_str()) == Some("ServiceAccount") {
                            let sa_name = subj.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            let sa_ns = subj.get("namespace").and_then(|n| n.as_str()).unwrap_or(ns);
                            Self::push_unique(
                                &mut related,
                                ResourceType::ServiceAccount,
                                sa_name,
                                sa_ns,
                                "subject",
                                "RBAC",
                            );
                        }
                    }
                }
            },
            | ResourceType::NetworkPolicy => {
                self.add_pods_by_network_policy_selector(&mut related, ns, value).await;
            },
            | ResourceType::Secret | ResourceType::ConfigMap => {
                self.add_workloads_referencing(&mut related, rt, ns, name).await;
            },
            | _ => {},
        }

        // Extract mounted volumes + env refs from pod templates
        let template_spec = match rt {
            | ResourceType::CronJob => {
                value
                    .get("spec")
                    .and_then(|s| s.get("jobTemplate"))
                    .and_then(|j| j.get("spec"))
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
            },
            | ResourceType::Deployment
            | ResourceType::StatefulSet
            | ResourceType::DaemonSet
            | ResourceType::Job
            | ResourceType::ReplicaSet => {
                value
                    .get("spec")
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
            },
            | ResourceType::Pod => value.get("spec"),
            | _ => None,
        };
        if let Some(spec) = template_spec {
            if let Some(volumes) = spec.get("volumes").and_then(|v| v.as_array()) {
                Self::extract_volume_refs(&mut related, volumes, ns);
            }
            if let Some(containers) = spec.get("containers").and_then(|c| c.as_array()) {
                Self::extract_env_refs(&mut related, containers, ns);
            }
            if let Some(init_containers) = spec.get("initContainers").and_then(|c| c.as_array()) {
                Self::extract_env_refs(&mut related, init_containers, ns);
            }
        }

        // Sort: by resource type category order, then by sort_key (timestamp)
        let type_order = |c: &str| -> u8 {
            match c {
                // Workloads
                | "Deployments" => 0,
                | "ReplicaSets" => 1,
                | "StatefulSets" => 2,
                | "DaemonSets" => 3,
                | "Pods" => 4,
                | "Jobs" => 5,
                | "CronJobs" => 6,
                | "HPAs" => 7,
                // Network
                | "Services" => 10,
                | "Ingresses" => 11,
                | "Endpoints" => 12,
                | "NetworkPolicies" => 13,
                // Config
                | "ConfigMaps" => 20,
                | "Secrets" => 21,
                // Storage
                | "PVCs" => 30,
                | "PVs" => 31,
                | "StorageClasses" => 32,
                // RBAC
                | "ServiceAccounts" => 40,
                | "Roles" => 41,
                | "RoleBindings" => 42,
                | "ClusterRoles" => 43,
                | "ClusterRoleBindings" => 44,
                // Cluster
                | "Nodes" => 50,
                | "Namespaces" => 51,
                | _ => 99,
            }
        };
        related.sort_by(|a, b| {
            type_order(a.category).cmp(&type_order(b.category)).then_with(|| {
                if a.sort_key.is_empty() && b.sort_key.is_empty() {
                    a.name.cmp(&b.name)
                } else {
                    b.sort_key.cmp(&a.sort_key)
                }
            })
        });

        related
    }

    fn add_owner_refs(related: &mut Vec<RelatedResource>, value: &Value, ns: &str) {
        if let Some(owners) = value
            .get("metadata")
            .and_then(|m| m.get("ownerReferences"))
            .and_then(|o| o.as_array())
        {
            for owner in owners {
                let kind = owner.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                let oname = owner.get("name").and_then(|n| n.as_str()).unwrap_or("");
                let owner_rt = match kind {
                    | "Deployment" => Some(ResourceType::Deployment),
                    | "ReplicaSet" => Some(ResourceType::ReplicaSet),
                    | "StatefulSet" => Some(ResourceType::StatefulSet),
                    | "DaemonSet" => Some(ResourceType::DaemonSet),
                    | "Job" => Some(ResourceType::Job),
                    | "CronJob" => Some(ResourceType::CronJob),
                    | "Node" => Some(ResourceType::Node),
                    | _ => None,
                };
                if let Some(ort) = owner_rt {
                    related.push(RelatedResource {
                        resource_type: ort,
                        name: oname.to_string(),
                        namespace: ns.to_string(),
                        info: "owner".to_string(),
                        category: ort.display_name(),
                        sort_key: String::new(),
                    });
                }
            }
        }
    }

    async fn add_replicasets_for_deployment(&self, related: &mut Vec<RelatedResource>, ns: &str, value: &Value) {
        let selector = value
            .get("spec")
            .and_then(|s| s.get("selector"))
            .and_then(|s| s.get("matchLabels"))
            .and_then(|m| m.as_object());
        if let Some(labels) = selector {
            let sel = labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",");
            let api: Api<ReplicaSet> = Api::namespaced(self.client.clone(), ns);
            if let Ok(list) = api.list(&ListParams::default().labels(&sel)).await {
                for rs in &list.items {
                    let rs_name = meta_name(&rs.metadata);
                    let replicas = rs.status.as_ref().map(|s| s.replicas).unwrap_or(0);
                    let ready = rs.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);
                    let revision = rs
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get("deployment.kubernetes.io/revision"))
                        .map(|s| s.as_str())
                        .unwrap_or("0");
                    let ts = rs
                        .metadata
                        .creation_timestamp
                        .as_ref()
                        .map(|t| t.0.to_string())
                        .unwrap_or_default();
                    related.push(RelatedResource {
                        resource_type: ResourceType::ReplicaSet,
                        name: rs_name,
                        namespace: ns.to_string(),
                        info: format!("rev:{} ready:{}/{}", revision, ready, replicas),
                        category: ResourceType::ReplicaSet.display_name(),
                        sort_key: ts,
                    });
                }
            }
        }
    }

    async fn add_pods_by_selector(&self, related: &mut Vec<RelatedResource>, ns: &str, value: &Value) {
        let selector = value
            .get("spec")
            .and_then(|s| s.get("selector"))
            .and_then(|s| s.get("matchLabels"))
            .and_then(|m| m.as_object());
        if let Some(labels) = selector {
            let sel = labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",");
            self.add_pods_by_label(related, ns, &sel).await;
        }
    }

    async fn add_pods_by_service_selector(&self, related: &mut Vec<RelatedResource>, ns: &str, value: &Value) {
        if let Some(selector) = value
            .get("spec")
            .and_then(|s| s.get("selector"))
            .and_then(|s| s.as_object())
        {
            let sel = selector
                .iter()
                .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",");
            self.add_pods_by_label(related, ns, &sel).await;
        }
    }

    async fn add_pods_by_network_policy_selector(&self, related: &mut Vec<RelatedResource>, ns: &str, value: &Value) {
        if let Some(selector) = value
            .get("spec")
            .and_then(|s| s.get("podSelector"))
            .and_then(|s| s.get("matchLabels"))
            .and_then(|m| m.as_object())
        {
            let sel = selector
                .iter()
                .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",");
            self.add_pods_by_label(related, ns, &sel).await;
        }
    }

    async fn add_pods_by_label(&self, related: &mut Vec<RelatedResource>, ns: &str, label_sel: &str) {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = api.list(&ListParams::default().labels(label_sel)).await {
            for pod in &list.items {
                let phase = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("Unknown");
                let ts = pod
                    .metadata
                    .creation_timestamp
                    .as_ref()
                    .map(|t| t.0.to_string())
                    .unwrap_or_default();
                related.push(RelatedResource {
                    resource_type: ResourceType::Pod,
                    name: meta_name(&pod.metadata),
                    namespace: ns.to_string(),
                    info: phase.to_string(),
                    category: ResourceType::Pod.display_name(),
                    sort_key: ts,
                });
            }
        }
    }

    async fn add_hpa_for_workload(&self, related: &mut Vec<RelatedResource>, ns: &str, name: &str) {
        let api: Api<HorizontalPodAutoscaler> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = api.list(&ListParams::default()).await {
            for hpa in &list.items {
                let target = hpa
                    .spec
                    .as_ref()
                    .map(|s| s.scale_target_ref.name.as_str())
                    .unwrap_or("");
                if target == name {
                    let min = hpa.spec.as_ref().and_then(|s| s.min_replicas).unwrap_or(0);
                    let max = hpa.spec.as_ref().map(|s| s.max_replicas).unwrap_or(0);
                    related.push(RelatedResource {
                        resource_type: ResourceType::HorizontalPodAutoscaler,
                        name: meta_name(&hpa.metadata),
                        namespace: ns.to_string(),
                        info: format!("min:{} max:{}", min, max),
                        category: ResourceType::HorizontalPodAutoscaler.display_name(),
                        sort_key: String::new(),
                    });
                }
            }
        }
    }

    fn add_service_account(related: &mut Vec<RelatedResource>, value: &Value, ns: &str, from_template: bool) {
        let spec = if from_template {
            value
                .get("spec")
                .and_then(|s| s.get("template"))
                .and_then(|t| t.get("spec"))
        } else {
            value.get("spec")
        };
        if let Some(sa) = spec.and_then(|s| s.get("serviceAccountName")).and_then(|n| n.as_str()) {
            Self::push_unique(related, ResourceType::ServiceAccount, sa, ns, "serviceAccount", "RBAC");
        }
        // Also check serviceAccount field (deprecated but still used)
        if let Some(sa) = spec.and_then(|s| s.get("serviceAccount")).and_then(|n| n.as_str()) {
            Self::push_unique(related, ResourceType::ServiceAccount, sa, ns, "serviceAccount", "RBAC");
        }
    }

    fn push_unique(
        related: &mut Vec<RelatedResource>,
        rt: ResourceType,
        name: &str,
        ns: &str,
        info: &str,
        _category: &'static str,
    ) {
        if !related.iter().any(|r| r.resource_type == rt && r.name == name) {
            related.push(RelatedResource {
                resource_type: rt,
                name: name.to_string(),
                namespace: ns.to_string(),
                info: info.to_string(),
                category: rt.display_name(),
                sort_key: String::new(),
            });
        }
    }

    /// Find workloads (Deployments, StatefulSets, DaemonSets, Jobs, CronJobs)
    /// that reference a given Secret or ConfigMap by name.
    async fn add_workloads_referencing(
        &self,
        related: &mut Vec<RelatedResource>,
        ref_type: ResourceType,
        ns: &str,
        ref_name: &str,
    ) {
        let is_secret = ref_type == ResourceType::Secret;

        // Helper: check if a pod spec references the target name
        let spec_references = |spec: &Value| -> bool {
            // Check volumes
            if let Some(volumes) = spec.get("volumes").and_then(|v| v.as_array()) {
                for vol in volumes {
                    if is_secret {
                        if vol
                            .get("secret")
                            .and_then(|s| s.get("secretName"))
                            .and_then(|n| n.as_str())
                            == Some(ref_name)
                        {
                            return true;
                        }
                        if let Some(sources) = vol
                            .get("projected")
                            .and_then(|p| p.get("sources"))
                            .and_then(|s| s.as_array())
                        {
                            for src in sources {
                                if src.get("secret").and_then(|s| s.get("name")).and_then(|n| n.as_str())
                                    == Some(ref_name)
                                {
                                    return true;
                                }
                            }
                        }
                    } else {
                        if vol
                            .get("configMap")
                            .and_then(|c| c.get("name"))
                            .and_then(|n| n.as_str())
                            == Some(ref_name)
                        {
                            return true;
                        }
                        if let Some(sources) = vol
                            .get("projected")
                            .and_then(|p| p.get("sources"))
                            .and_then(|s| s.as_array())
                        {
                            for src in sources {
                                if src
                                    .get("configMap")
                                    .and_then(|c| c.get("name"))
                                    .and_then(|n| n.as_str())
                                    == Some(ref_name)
                                {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            // Check containers env refs
            for container_key in &["containers", "initContainers"] {
                if let Some(containers) = spec.get(container_key).and_then(|c| c.as_array()) {
                    for container in containers {
                        if let Some(env_from) = container.get("envFrom").and_then(|e| e.as_array()) {
                            for src in env_from {
                                let ref_field = if is_secret { "secretRef" } else { "configMapRef" };
                                if src.get(ref_field).and_then(|r| r.get("name")).and_then(|n| n.as_str())
                                    == Some(ref_name)
                                {
                                    return true;
                                }
                            }
                        }
                        if let Some(env) = container.get("env").and_then(|e| e.as_array()) {
                            for var in env {
                                if let Some(vf) = var.get("valueFrom") {
                                    let ref_field = if is_secret { "secretKeyRef" } else { "configMapKeyRef" };
                                    if vf.get(ref_field).and_then(|r| r.get("name")).and_then(|n| n.as_str())
                                        == Some(ref_name)
                                    {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            false
        };

        // Search Deployments
        let dep_api: Api<Deployment> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = dep_api.list(&ListParams::default()).await {
            for dep in &list.items {
                let val = serde_json::to_value(dep).unwrap_or_default();
                if let Some(spec) = val
                    .get("spec")
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
                {
                    if spec_references(spec) {
                        Self::push_unique(
                            related,
                            ResourceType::Deployment,
                            &meta_name(&dep.metadata),
                            ns,
                            "references",
                            "Workloads",
                        );
                    }
                }
            }
        }
        // Search StatefulSets
        let ss_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = ss_api.list(&ListParams::default()).await {
            for ss in &list.items {
                let val = serde_json::to_value(ss).unwrap_or_default();
                if let Some(spec) = val
                    .get("spec")
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
                {
                    if spec_references(spec) {
                        Self::push_unique(
                            related,
                            ResourceType::StatefulSet,
                            &meta_name(&ss.metadata),
                            ns,
                            "references",
                            "Workloads",
                        );
                    }
                }
            }
        }
        // Search DaemonSets
        let ds_api: Api<DaemonSet> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = ds_api.list(&ListParams::default()).await {
            for ds in &list.items {
                let val = serde_json::to_value(ds).unwrap_or_default();
                if let Some(spec) = val
                    .get("spec")
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
                {
                    if spec_references(spec) {
                        Self::push_unique(
                            related,
                            ResourceType::DaemonSet,
                            &meta_name(&ds.metadata),
                            ns,
                            "references",
                            "Workloads",
                        );
                    }
                }
            }
        }
        // Search Jobs
        let job_api: Api<Job> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = job_api.list(&ListParams::default()).await {
            for job in &list.items {
                let val = serde_json::to_value(job).unwrap_or_default();
                if let Some(spec) = val
                    .get("spec")
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
                {
                    if spec_references(spec) {
                        Self::push_unique(
                            related,
                            ResourceType::Job,
                            &meta_name(&job.metadata),
                            ns,
                            "references",
                            "Workloads",
                        );
                    }
                }
            }
        }
        // Search CronJobs
        let cj_api: Api<CronJob> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = cj_api.list(&ListParams::default()).await {
            for cj in &list.items {
                let val = serde_json::to_value(cj).unwrap_or_default();
                if let Some(spec) = val
                    .get("spec")
                    .and_then(|s| s.get("jobTemplate"))
                    .and_then(|j| j.get("spec"))
                    .and_then(|s| s.get("template"))
                    .and_then(|t| t.get("spec"))
                {
                    if spec_references(spec) {
                        Self::push_unique(
                            related,
                            ResourceType::CronJob,
                            &meta_name(&cj.metadata),
                            ns,
                            "references",
                            "Workloads",
                        );
                    }
                }
            }
        }
    }

    async fn add_workloads_using_sa(&self, related: &mut Vec<RelatedResource>, ns: &str, sa_name: &str) {
        let check_sa = |val: &Value| -> bool {
            val.get("spec")
                .and_then(|s| s.get("template"))
                .and_then(|t| t.get("spec"))
                .and_then(|s| s.get("serviceAccountName").or_else(|| s.get("serviceAccount")))
                .and_then(|n| n.as_str())
                == Some(sa_name)
        };
        let dep_api: Api<Deployment> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = dep_api.list(&ListParams::default()).await {
            for dep in &list.items {
                let val = serde_json::to_value(dep).unwrap_or_default();
                if check_sa(&val) {
                    Self::push_unique(
                        related,
                        ResourceType::Deployment,
                        &meta_name(&dep.metadata),
                        ns,
                        "uses SA",
                        "x",
                    );
                }
            }
        }
        let ss_api: Api<StatefulSet> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = ss_api.list(&ListParams::default()).await {
            for ss in &list.items {
                let val = serde_json::to_value(ss).unwrap_or_default();
                if check_sa(&val) {
                    Self::push_unique(
                        related,
                        ResourceType::StatefulSet,
                        &meta_name(&ss.metadata),
                        ns,
                        "uses SA",
                        "x",
                    );
                }
            }
        }
        let ds_api: Api<DaemonSet> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = ds_api.list(&ListParams::default()).await {
            for ds in &list.items {
                let val = serde_json::to_value(ds).unwrap_or_default();
                if check_sa(&val) {
                    Self::push_unique(
                        related,
                        ResourceType::DaemonSet,
                        &meta_name(&ds.metadata),
                        ns,
                        "uses SA",
                        "x",
                    );
                }
            }
        }
        let job_api: Api<Job> = Api::namespaced(self.client.clone(), ns);
        if let Ok(list) = job_api.list(&ListParams::default()).await {
            for job in &list.items {
                let val = serde_json::to_value(job).unwrap_or_default();
                if check_sa(&val) {
                    Self::push_unique(
                        related,
                        ResourceType::Job,
                        &meta_name(&job.metadata),
                        ns,
                        "uses SA",
                        "x",
                    );
                }
            }
        }
    }

    fn extract_volume_refs(related: &mut Vec<RelatedResource>, volumes: &[Value], ns: &str) {
        for vol in volumes {
            if let Some(name) = vol
                .get("secret")
                .and_then(|s| s.get("secretName"))
                .and_then(|n| n.as_str())
            {
                Self::push_unique(related, ResourceType::Secret, name, ns, "mounted", "Config");
            }
            if let Some(name) = vol
                .get("configMap")
                .and_then(|c| c.get("name"))
                .and_then(|n| n.as_str())
            {
                Self::push_unique(related, ResourceType::ConfigMap, name, ns, "mounted", "Config");
            }
            if let Some(name) = vol
                .get("persistentVolumeClaim")
                .and_then(|p| p.get("claimName"))
                .and_then(|n| n.as_str())
            {
                Self::push_unique(
                    related,
                    ResourceType::PersistentVolumeClaim,
                    name,
                    ns,
                    "mounted",
                    "Storage",
                );
            }
            if let Some(name) = vol
                .get("projected")
                .and_then(|p| p.get("sources"))
                .and_then(|s| s.as_array())
            {
                for src in name {
                    if let Some(sname) = src.get("secret").and_then(|s| s.get("name")).and_then(|n| n.as_str()) {
                        Self::push_unique(related, ResourceType::Secret, sname, ns, "projected", "Config");
                    }
                    if let Some(cname) = src
                        .get("configMap")
                        .and_then(|c| c.get("name"))
                        .and_then(|n| n.as_str())
                    {
                        Self::push_unique(related, ResourceType::ConfigMap, cname, ns, "projected", "Config");
                    }
                }
            }
        }
    }

    fn extract_env_refs(related: &mut Vec<RelatedResource>, containers: &[Value], ns: &str) {
        for container in containers {
            if let Some(env_from) = container.get("envFrom").and_then(|e| e.as_array()) {
                for src in env_from {
                    if let Some(name) = src
                        .get("secretRef")
                        .and_then(|s| s.get("name"))
                        .and_then(|n| n.as_str())
                    {
                        Self::push_unique(related, ResourceType::Secret, name, ns, "envFrom", "Config");
                    }
                    if let Some(name) = src
                        .get("configMapRef")
                        .and_then(|c| c.get("name"))
                        .and_then(|n| n.as_str())
                    {
                        Self::push_unique(related, ResourceType::ConfigMap, name, ns, "envFrom", "Config");
                    }
                }
            }
            if let Some(env) = container.get("env").and_then(|e| e.as_array()) {
                for var in env {
                    if let Some(vf) = var.get("valueFrom") {
                        if let Some(name) = vf
                            .get("secretKeyRef")
                            .and_then(|s| s.get("name"))
                            .and_then(|n| n.as_str())
                        {
                            Self::push_unique(related, ResourceType::Secret, name, ns, "env", "Config");
                        }
                        if let Some(name) = vf
                            .get("configMapKeyRef")
                            .and_then(|c| c.get("name"))
                            .and_then(|n| n.as_str())
                        {
                            Self::push_unique(related, ResourceType::ConfigMap, name, ns, "env", "Config");
                        }
                    }
                }
            }
        }
    }

    // -- Log support -----------------------------------------------------------

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub async fn find_pods(&self, rt: ResourceType, ns: &str, name: &str) -> Result<Vec<PodInfo>> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), ns);
        match rt {
            | ResourceType::Pod => {
                let pod = api.get(name).await?;
                let containers = pod
                    .spec
                    .as_ref()
                    .map(|s| s.containers.iter().map(|c| c.name.clone()).collect())
                    .unwrap_or_default();
                Ok(vec![PodInfo {
                    name: name.to_string(),
                    containers,
                }])
            },
            | ResourceType::Job => {
                let lp = ListParams::default().labels(&format!("job-name={}", name));
                self.pods_from_list(&api, &lp).await
            },
            | ResourceType::CronJob => {
                // Find latest Job owned by this CronJob, then find its pods
                let job_api: Api<Job> = Api::namespaced(self.client.clone(), ns);
                let jobs = job_api.list(&ListParams::default()).await?;
                let latest = jobs
                    .items
                    .iter()
                    .filter(|j| {
                        j.metadata
                            .owner_references
                            .as_ref()
                            .map(|refs| refs.iter().any(|r| r.kind == "CronJob" && r.name == name))
                            .unwrap_or(false)
                    })
                    .max_by_key(|j| j.metadata.creation_timestamp.as_ref().map(|t| t.0));
                match latest {
                    | Some(j) => {
                        let job_name = j.metadata.name.as_deref().unwrap_or("");
                        let lp = ListParams::default().labels(&format!("job-name={}", job_name));
                        self.pods_from_list(&api, &lp).await
                    },
                    | None => Ok(vec![]),
                }
            },
            | ResourceType::Deployment
            | ResourceType::StatefulSet
            | ResourceType::DaemonSet
            | ResourceType::ReplicaSet => {
                // Use the resource's spec.selector.matchLabels
                let val = self.get_resource(rt, ns, name).await?;
                let labels = val
                    .get("spec")
                    .and_then(|s| s.get("selector"))
                    .and_then(|s| s.get("matchLabels"))
                    .and_then(|m| m.as_object());
                match labels {
                    | Some(map) => {
                        let selector = map
                            .iter()
                            .map(|(k, v)| format!("{}={}", k, v.as_str().unwrap_or("")))
                            .collect::<Vec<_>>()
                            .join(",");
                        let lp = ListParams::default().labels(&selector);
                        self.pods_from_list(&api, &lp).await
                    },
                    | None => Ok(vec![]),
                }
            },
            | _ => Ok(vec![]),
        }
    }

    async fn pods_from_list(&self, api: &Api<Pod>, lp: &ListParams) -> Result<Vec<PodInfo>> {
        let list = api.list(lp).await?;
        Ok(list
            .items
            .iter()
            .map(|p| {
                PodInfo {
                    name: meta_name(&p.metadata),
                    containers: p
                        .spec
                        .as_ref()
                        .map(|s| s.containers.iter().map(|c| c.name.clone()).collect())
                        .unwrap_or_default(),
                }
            })
            .collect())
    }

    /// Fetch logs for a single pod/container.
    pub async fn fetch_logs(
        &self,
        ns: &str,
        pod: &str,
        container: &str,
        tail: i64,
        since_seconds: Option<i64>,
    ) -> Result<String> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), ns);
        let lp = kube::api::LogParams {
            container: Some(container.to_string()),
            tail_lines: Some(tail),
            timestamps: true,
            since_seconds,
            ..Default::default()
        };
        Ok(api.logs(pod, &lp).await?)
    }

    /// Fetch logs for multiple pod/container pairs and merge them with
    /// prefixes.
    pub async fn fetch_logs_multi(
        &self,
        ns: &str,
        pairs: &[(String, String)],
        tail: i64,
        since_seconds: Option<i64>,
    ) -> Result<Vec<String>> {
        let mut all_lines = Vec::new();
        for (pod, container) in pairs {
            let prefix = format!("[{}/{}] ", pod, container);
            match self.fetch_logs(ns, pod, container, tail, since_seconds).await {
                | Ok(logs) => {
                    for line in logs.lines() {
                        all_lines.push(format!("{}{}", prefix, line));
                    }
                },
                | Err(_) => {
                    all_lines.push(format!("{}(failed to fetch logs)", prefix));
                },
            }
        }
        // Sort by timestamp (the timestamp comes after the prefix)
        all_lines.sort();
        Ok(all_lines)
    }

    // -- Generic list helper -------------------------------------------------

    async fn list_typed<K>(&self, mapper: fn(&K) -> ResourceEntry) -> Result<Vec<ResourceEntry>>
    where
        K: kube::Resource<DynamicType=()>+serde::de::DeserializeOwned+Clone+std::fmt::Debug,
        K: kube::Resource<Scope=k8s_openapi::NamespaceResourceScope>,
    {
        let lp = ListParams::default();
        let list = match &self.current_namespace {
            | Some(ns) => Api::<K>::namespaced(self.client.clone(), ns).list(&lp).await?,
            | None => Api::<K>::all(self.client.clone()).list(&lp).await?,
        };
        Ok(list.items.iter().map(mapper).collect())
    }

    // -- Generic value fetch for any namespaced resource --------------------

    async fn get_value<K>(&self, ns: &str, name: &str) -> Result<Value>
    where
        K: kube::Resource<DynamicType=()>+serde::de::DeserializeOwned+serde::Serialize+Clone+std::fmt::Debug,
        K: kube::Resource<Scope=k8s_openapi::NamespaceResourceScope>,
    {
        let api: Api<K> = Api::namespaced(self.client.clone(), ns);
        let obj = api.get(name).await?;
        strip_managed_fields(serde_json::to_value(&obj)?)
    }

    // -- Cluster-scoped list helper ------------------------------------------

    async fn list_cluster<K>(&self, mapper: fn(&K) -> ResourceEntry) -> Result<Vec<ResourceEntry>>
    where
        K: kube::Resource<DynamicType=()>+serde::de::DeserializeOwned+Clone+std::fmt::Debug,
        K: kube::Resource<Scope=k8s_openapi::ClusterResourceScope>,
    {
        let api: Api<K> = Api::all(self.client.clone());
        let list = api.list(&ListParams::default()).await?;
        Ok(list.items.iter().map(mapper).collect())
    }

    // -- Cluster-scoped value fetch ------------------------------------------

    async fn get_value_cluster<K>(&self, name: &str) -> Result<Value>
    where
        K: kube::Resource<DynamicType=()>+serde::de::DeserializeOwned+serde::Serialize+Clone+std::fmt::Debug,
        K: kube::Resource<Scope=k8s_openapi::ClusterResourceScope>,
    {
        let api: Api<K> = Api::all(self.client.clone());
        let obj = api.get(name).await?;
        strip_managed_fields(serde_json::to_value(&obj)?)
    }

    // -- Per-resource mappers ------------------------------------------------

    fn map_deployment(d: &Deployment) -> ResourceEntry {
        let status = d.status.as_ref();
        let replicas = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
        let ready = status.and_then(|s| s.ready_replicas).unwrap_or(0);
        let updated = status.and_then(|s| s.updated_replicas).unwrap_or(0);
        let available = status.and_then(|s| s.available_replicas).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&d.metadata),
            namespace: meta_ns(&d.metadata),
            columns: vec![
                format!("{}/{}", ready, replicas),
                updated.to_string(),
                available.to_string(),
                format_age(d.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_statefulset(s: &StatefulSet) -> ResourceEntry {
        let status = s.status.as_ref();
        let replicas = s.spec.as_ref().and_then(|sp| sp.replicas).unwrap_or(0);
        let ready = status.and_then(|st| st.ready_replicas).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&s.metadata),
            namespace: meta_ns(&s.metadata),
            columns: vec![
                format!("{}/{}", ready, replicas),
                format_age(s.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_replicaset(r: &ReplicaSet) -> ResourceEntry {
        let status = r.status.as_ref();
        let desired = r.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
        let current = status.map(|s| s.replicas).unwrap_or(0);
        let ready = status.and_then(|s| s.ready_replicas).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&r.metadata),
            namespace: meta_ns(&r.metadata),
            columns: vec![
                desired.to_string(),
                current.to_string(),
                ready.to_string(),
                format_age(r.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_pod(p: &Pod) -> ResourceEntry {
        let status = p.status.as_ref();
        let phase = status.and_then(|s| s.phase.clone()).unwrap_or_else(|| "Unknown".into());
        let containers = status
            .and_then(|s| s.container_statuses.as_ref())
            .cloned()
            .unwrap_or_default();
        let total = containers.len();
        let ready_count = containers.iter().filter(|c| c.ready).count();
        let restarts: i32 = containers.iter().map(|c| c.restart_count).sum();
        ResourceEntry {
            name: meta_name(&p.metadata),
            namespace: meta_ns(&p.metadata),
            columns: vec![
                format!("{}/{}", ready_count, total),
                phase,
                restarts.to_string(),
                format_age(p.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_cronjob(c: &CronJob) -> ResourceEntry {
        let spec = c.spec.as_ref();
        let schedule = spec.map(|s| s.schedule.clone()).unwrap_or_default();
        let suspend = spec.and_then(|s| s.suspend).unwrap_or(false);
        let active = c
            .status
            .as_ref()
            .and_then(|s| s.active.as_ref())
            .map(|a| a.len())
            .unwrap_or(0);
        ResourceEntry {
            name: meta_name(&c.metadata),
            namespace: meta_ns(&c.metadata),
            columns: vec![
                schedule,
                suspend.to_string(),
                active.to_string(),
                format_age(c.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_job(j: &Job) -> ResourceEntry {
        let status = j.status.as_ref();
        let succeeded = status.and_then(|s| s.succeeded).unwrap_or(0);
        let completions = j.spec.as_ref().and_then(|s| s.completions).unwrap_or(1);
        let duration = status
            .and_then(|s| {
                let start = s.start_time.as_ref()?;
                let end = s.completion_time.as_ref().map(|t| t.0).unwrap_or_else(Timestamp::now);
                let dur = end.duration_since(start.0);
                Some(format_duration(dur))
            })
            .unwrap_or_default();
        ResourceEntry {
            name: meta_name(&j.metadata),
            namespace: meta_ns(&j.metadata),
            columns: vec![
                format!("{}/{}", succeeded, completions),
                duration,
                format_age(j.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_configmap(c: &ConfigMap) -> ResourceEntry {
        let data_count =
            c.data.as_ref().map(|d| d.len()).unwrap_or(0) + c.binary_data.as_ref().map(|d| d.len()).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&c.metadata),
            namespace: meta_ns(&c.metadata),
            columns: vec![
                data_count.to_string(),
                format_age(c.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_secret(s: &Secret) -> ResourceEntry {
        let secret_type = s.type_.clone().unwrap_or_default();
        let data_count = s.data.as_ref().map(|d| d.len()).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&s.metadata),
            namespace: meta_ns(&s.metadata),
            columns: vec![
                secret_type,
                data_count.to_string(),
                format_age(s.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_service(s: &Service) -> ResourceEntry {
        let spec = s.spec.as_ref();
        let svc_type = spec.and_then(|s| s.type_.clone()).unwrap_or_default();
        let cluster_ip = spec
            .and_then(|s| s.cluster_ip.clone())
            .unwrap_or_else(|| "<none>".into());
        let ports = spec
            .and_then(|s| s.ports.as_ref())
            .map(|ports| {
                ports
                    .iter()
                    .map(|p| {
                        let proto = p.protocol.as_deref().unwrap_or("TCP");
                        match p.node_port {
                            | Some(np) => format!("{}:{}/{}", p.port, np, proto),
                            | None => format!("{}/{}", p.port, proto),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        ResourceEntry {
            name: meta_name(&s.metadata),
            namespace: meta_ns(&s.metadata),
            columns: vec![
                svc_type,
                cluster_ip,
                ports,
                format_age(s.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_daemonset(d: &DaemonSet) -> ResourceEntry {
        let status = d.status.as_ref();
        let desired = status.map(|s| s.desired_number_scheduled).unwrap_or(0);
        let current = status.map(|s| s.current_number_scheduled).unwrap_or(0);
        let ready = status.map(|s| s.number_ready).unwrap_or(0);
        let updated = status.and_then(|s| s.updated_number_scheduled).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&d.metadata),
            namespace: meta_ns(&d.metadata),
            columns: vec![
                desired.to_string(),
                current.to_string(),
                ready.to_string(),
                updated.to_string(),
                format_age(d.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_hpa(h: &HorizontalPodAutoscaler) -> ResourceEntry {
        let spec = h.spec.as_ref();
        let reference = spec
            .map(|s| {
                let kind = s.scale_target_ref.kind.as_str();
                let name = s.scale_target_ref.name.as_str();
                format!("{}/{}", kind, name)
            })
            .unwrap_or_default();
        let min_replicas = spec.and_then(|s| s.min_replicas).unwrap_or(0);
        let max_replicas = spec.map(|s| s.max_replicas).unwrap_or(0);
        let current = h.status.as_ref().map(|s| s.current_replicas).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&h.metadata),
            namespace: meta_ns(&h.metadata),
            columns: vec![
                reference,
                min_replicas.to_string(),
                max_replicas.to_string(),
                current.to_string(),
                format_age(h.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_ingress(i: &Ingress) -> ResourceEntry {
        let spec = i.spec.as_ref();
        let class = spec
            .and_then(|s| s.ingress_class_name.clone())
            .unwrap_or_else(|| "<none>".into());
        let hosts = spec
            .and_then(|s| s.rules.as_ref())
            .map(|rules| {
                rules
                    .iter()
                    .filter_map(|r| r.host.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let address = i
            .status
            .as_ref()
            .and_then(|s| s.load_balancer.as_ref())
            .and_then(|lb| lb.ingress.as_ref())
            .map(|ingress| {
                ingress
                    .iter()
                    .filter_map(|i| i.ip.as_ref().or(i.hostname.as_ref()).cloned())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        ResourceEntry {
            name: meta_name(&i.metadata),
            namespace: meta_ns(&i.metadata),
            columns: vec![
                class,
                hosts,
                address,
                format_age(i.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_endpoints(e: &Endpoints) -> ResourceEntry {
        let endpoints_str = e
            .subsets
            .as_ref()
            .map(|subsets| {
                let mut addrs = Vec::new();
                for subset in subsets {
                    let ports: Vec<i32> = subset
                        .ports
                        .as_ref()
                        .map(|p| p.iter().map(|p| p.port).collect())
                        .unwrap_or_default();
                    if let Some(addresses) = &subset.addresses {
                        for addr in addresses {
                            for port in &ports {
                                addrs.push(format!("{}:{}", addr.ip, port));
                            }
                        }
                    }
                }
                if addrs.len() > 3 {
                    let total = addrs.len();
                    addrs.truncate(3);
                    format!("{} + {} more", addrs.join(","), total - 3)
                } else {
                    addrs.join(",")
                }
            })
            .unwrap_or_else(|| "<none>".into());
        ResourceEntry {
            name: meta_name(&e.metadata),
            namespace: meta_ns(&e.metadata),
            columns: vec![endpoints_str, format_age(e.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_network_policy(n: &NetworkPolicy) -> ResourceEntry {
        let selector = n
            .spec
            .as_ref()
            .and_then(|s| s.pod_selector.as_ref())
            .and_then(|ps| ps.match_labels.as_ref())
            .map(|m| {
                m.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_else(|| "<none>".into());
        ResourceEntry {
            name: meta_name(&n.metadata),
            namespace: meta_ns(&n.metadata),
            columns: vec![selector, format_age(n.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_pvc(p: &PersistentVolumeClaim) -> ResourceEntry {
        let status = p.status.as_ref();
        let phase = status.and_then(|s| s.phase.clone()).unwrap_or_else(|| "Pending".into());
        let volume = p.spec.as_ref().and_then(|s| s.volume_name.clone()).unwrap_or_default();
        let capacity = status
            .and_then(|s| s.capacity.as_ref())
            .and_then(|c| c.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_default();
        ResourceEntry {
            name: meta_name(&p.metadata),
            namespace: meta_ns(&p.metadata),
            columns: vec![
                phase,
                volume,
                capacity,
                format_age(p.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_pv(p: &PersistentVolume) -> ResourceEntry {
        let spec = p.spec.as_ref();
        let capacity = spec
            .and_then(|s| s.capacity.as_ref())
            .and_then(|c| c.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_default();
        let reclaim = spec
            .and_then(|s| s.persistent_volume_reclaim_policy.clone())
            .unwrap_or_default();
        let phase = p.status.as_ref().and_then(|s| s.phase.clone()).unwrap_or_default();
        let claim = spec
            .and_then(|s| s.claim_ref.as_ref())
            .map(|c| {
                format!(
                    "{}/{}",
                    c.namespace.as_deref().unwrap_or(""),
                    c.name.as_deref().unwrap_or("")
                )
            })
            .unwrap_or_default();
        ResourceEntry {
            name: meta_name(&p.metadata),
            namespace: String::new(),
            columns: vec![
                capacity,
                reclaim,
                phase,
                claim,
                format_age(p.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_storage_class(s: &StorageClass) -> ResourceEntry {
        let provisioner = s.provisioner.clone();
        let reclaim = s.reclaim_policy.clone().unwrap_or_default();
        ResourceEntry {
            name: meta_name(&s.metadata),
            namespace: String::new(),
            columns: vec![provisioner, reclaim, format_age(s.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_service_account(s: &ServiceAccount) -> ResourceEntry {
        let secrets_count = s.secrets.as_ref().map(|s| s.len()).unwrap_or(0);
        ResourceEntry {
            name: meta_name(&s.metadata),
            namespace: meta_ns(&s.metadata),
            columns: vec![
                secrets_count.to_string(),
                format_age(s.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_role(r: &Role) -> ResourceEntry {
        ResourceEntry {
            name: meta_name(&r.metadata),
            namespace: meta_ns(&r.metadata),
            columns: vec![format_age(r.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_role_binding(r: &RoleBinding) -> ResourceEntry {
        let role = format!("{}/{}", r.role_ref.kind, r.role_ref.name);
        ResourceEntry {
            name: meta_name(&r.metadata),
            namespace: meta_ns(&r.metadata),
            columns: vec![role, format_age(r.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_cluster_role(r: &ClusterRole) -> ResourceEntry {
        ResourceEntry {
            name: meta_name(&r.metadata),
            namespace: String::new(),
            columns: vec![format_age(r.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_cluster_role_binding(r: &ClusterRoleBinding) -> ResourceEntry {
        let role = format!("{}/{}", r.role_ref.kind, r.role_ref.name);
        ResourceEntry {
            name: meta_name(&r.metadata),
            namespace: String::new(),
            columns: vec![role, format_age(r.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_node(n: &Node) -> ResourceEntry {
        let status = n.status.as_ref();
        let conditions = status.and_then(|s| s.conditions.as_ref());
        let ready = conditions
            .and_then(|conds| conds.iter().find(|c| c.type_ == "Ready").map(|c| c.status == "True"))
            .unwrap_or(false);
        let unschedulable = n.spec.as_ref().and_then(|s| s.unschedulable).unwrap_or(false);

        let status_str = match (ready, unschedulable) {
            | (true, false) => "Ready".to_string(),
            | (true, true) => "Ready,SchedulingDisabled".to_string(),
            | (false, false) => "NotReady".to_string(),
            | (false, true) => "NotReady,SchedulingDisabled".to_string(),
        };

        let roles = n
            .metadata
            .labels
            .as_ref()
            .map(|labels| {
                labels
                    .keys()
                    .filter_map(|k| {
                        k.strip_prefix("node-role.kubernetes.io/")
                            .map(|r| if r.is_empty() { "worker" } else { r })
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let version = status
            .and_then(|s| s.node_info.as_ref())
            .map(|i| i.kubelet_version.clone())
            .unwrap_or_default();
        ResourceEntry {
            name: meta_name(&n.metadata),
            namespace: String::new(),
            columns: vec![
                status_str,
                if roles.is_empty() { "<none>".into() } else { roles },
                version,
                format_age(n.metadata.creation_timestamp.as_ref()),
            ],
            sort_key: None,
        }
    }

    fn map_namespace(n: &Namespace) -> ResourceEntry {
        let phase = n
            .status
            .as_ref()
            .and_then(|s| s.phase.clone())
            .unwrap_or_else(|| "Active".into());
        ResourceEntry {
            name: meta_name(&n.metadata),
            namespace: String::new(),
            columns: vec![phase, format_age(n.metadata.creation_timestamp.as_ref())],
            sort_key: None,
        }
    }

    fn map_event(e: &Event) -> ResourceEntry {
        let event_type = e.type_.clone().unwrap_or_default();
        let reason = e.reason.clone().unwrap_or_default();
        let object = e
            .regarding
            .as_ref()
            .and_then(|r| {
                r.name
                    .as_ref()
                    .map(|n| format!("{}/{}", r.kind.as_deref().unwrap_or(""), n))
            })
            .unwrap_or_default();
        let message = e.note.clone().unwrap_or_default();
        let count = e.series.as_ref().map(|s| s.count).or(e.deprecated_count).unwrap_or(1);
        // Prefer eventTime, fall back to deprecated lastTimestamp, then
        // creationTimestamp
        let best_time: Option<Timestamp> = e
            .event_time
            .as_ref()
            .map(|t| t.0)
            .or(e.deprecated_last_timestamp.as_ref().map(|t| t.0))
            .or(e.metadata.creation_timestamp.as_ref().map(|t| t.0));
        let sort_ts = best_time.map(|t| t.to_string()).unwrap_or_default();
        let age = best_time
            .map(|t| {
                let dur = Timestamp::now().duration_since(t);
                format_duration(dur)
            })
            .unwrap_or_else(|| "N/A".into());
        ResourceEntry {
            name: meta_name(&e.metadata),
            namespace: meta_ns(&e.metadata),
            // [0]=TYPE  [1]=REASON  [2]=OBJECT  [3]=AGE  [4]=MESSAGE  [5]=COUNT
            columns: vec![event_type, reason, object, age, message, count.to_string()],
            sort_key: Some(sort_ts),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn meta_name(meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> String {
    meta.name.clone().unwrap_or_default()
}

fn meta_ns(meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> String {
    meta.namespace.clone().unwrap_or_default()
}

fn format_age(timestamp: Option<&Time>) -> String {
    match timestamp {
        | Some(time) => {
            let dur = Timestamp::now().duration_since(time.0);
            format_duration(dur)
        },
        | None => "N/A".into(),
    }
}

fn format_duration(dur: jiff::SignedDuration) -> String {
    let secs = dur.as_secs().max(0);
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    if days > 0 {
        format!("{}d", days)
    } else if hours > 0 {
        format!("{}h", hours)
    } else if mins > 0 {
        format!("{}m", mins)
    } else {
        format!("{}s", secs)
    }
}

/// Parse a Kubernetes memory quantity string (e.g. "16384Ki", "8Gi",
/// "16777216000") and format it as gigabytes with one decimal place.
fn format_memory_gb(raw: &str) -> String {
    if raw == "-" || raw.is_empty() {
        return raw.to_string();
    }

    let bytes: f64 = if let Some(val) = raw.strip_suffix("Ki") {
        val.parse::<f64>().unwrap_or(0.0) * 1024.0
    } else if let Some(val) = raw.strip_suffix("Mi") {
        val.parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0
    } else if let Some(val) = raw.strip_suffix("Gi") {
        val.parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0 * 1024.0
    } else if let Some(val) = raw.strip_suffix("Ti") {
        val.parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0 * 1024.0 * 1024.0
    } else if let Some(val) = raw.strip_suffix('K') {
        val.parse::<f64>().unwrap_or(0.0) * 1000.0
    } else if let Some(val) = raw.strip_suffix('M') {
        val.parse::<f64>().unwrap_or(0.0) * 1_000_000.0
    } else if let Some(val) = raw.strip_suffix('G') {
        val.parse::<f64>().unwrap_or(0.0) * 1_000_000_000.0
    } else if let Some(val) = raw.strip_suffix('T') {
        val.parse::<f64>().unwrap_or(0.0) * 1_000_000_000_000.0
    } else {
        // Plain bytes
        raw.parse::<f64>().unwrap_or(0.0)
    };

    let gb = bytes / (1024.0 * 1024.0 * 1024.0);
    if gb >= 10.0 {
        format!("{:.0}Gi", gb)
    } else {
        format!("{:.1}Gi", gb)
    }
}

fn strip_managed_fields(mut val: Value) -> Result<Value> {
    if let Some(metadata) = val.get_mut("metadata") {
        if let Some(map) = metadata.as_object_mut() {
            map.remove("managedFields");
        }
    }
    Ok(val)
}
