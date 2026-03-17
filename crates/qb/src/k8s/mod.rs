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
                Event,
                Namespace,
                Node,
                PersistentVolume,
                PersistentVolumeClaim,
                Pod,
                Secret,
                Service,
                ServiceAccount,
            },
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    pub fn all_by_category() -> Vec<(Category, Vec<ResourceType>)> {
        vec![
            (Category::Cluster, vec![Self::Node, Self::Namespace, Self::Event]),
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
            (Category::Rbac, vec![
                Self::ServiceAccount,
                Self::Role,
                Self::RoleBinding,
                Self::ClusterRole,
                Self::ClusterRoleBinding,
            ]),
            (Category::Storage, vec![
                Self::PersistentVolumeClaim,
                Self::PersistentVolume,
                Self::StorageClass,
            ]),
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

// ---------------------------------------------------------------------------
// Cluster stats
// ---------------------------------------------------------------------------

pub struct NodeStats {
    pub name: String,
    pub status: String,
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
    pub namespace_count: usize,
    pub pod_count: usize,
    pub pods_running: usize,
    pub pods_pending: usize,
    pub pods_failed: usize,
    pub deployment_count: usize,
    pub service_count: usize,
    pub nodes: Vec<NodeStats>,
}

// ---------------------------------------------------------------------------
// Kubernetes client
// ---------------------------------------------------------------------------

pub struct KubeClient {
    client: Client,
    kubeconfig: Kubeconfig,
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
            current_context,
            current_namespace,
        })
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

            node_stats.push(NodeStats {
                name: meta_name(&n.metadata),
                status: if is_ready { "Ready".into() } else { "NotReady".into() },
                roles: if roles.is_empty() { "<none>".into() } else { roles },
                version: info.map(|i| i.kubelet_version.clone()).unwrap_or_default(),
                os_arch: info
                    .map(|i| format!("{}/{}", i.operating_system, i.architecture))
                    .unwrap_or_default(),
                cpu_capacity: get_res(capacity, "cpu"),
                cpu_allocatable: get_res(allocatable, "cpu"),
                mem_capacity: get_res(capacity, "memory"),
                mem_allocatable: get_res(allocatable, "memory"),
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
        for p in &pod_list.items {
            match p.status.as_ref().and_then(|s| s.phase.as_deref()) {
                | Some("Running") => pods_running += 1,
                | Some("Pending") => pods_pending += 1,
                | Some("Failed") => pods_failed += 1,
                | _ => {},
            }
        }

        // Deployments & Services
        let dep_api: Api<Deployment> = Api::all(self.client.clone());
        let deployment_count = dep_api.list(&ListParams::default()).await?.items.len();
        let svc_api: Api<Service> = Api::all(self.client.clone());
        let service_count = svc_api.list(&ListParams::default()).await?.items.len();

        Ok(ClusterStatsData {
            server_version,
            node_count: node_list.items.len(),
            nodes_ready,
            nodes_not_ready,
            namespace_count: ns_count,
            pod_count,
            pods_running,
            pods_pending,
            pods_failed,
            deployment_count,
            service_count,
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

    // -- Generic list helper -------------------------------------------------

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
    pub async fn fetch_logs(&self, ns: &str, pod: &str, container: &str, tail: i64) -> Result<String> {
        let api: Api<Pod> = Api::namespaced(self.client.clone(), ns);
        let lp = kube::api::LogParams {
            container: Some(container.to_string()),
            tail_lines: Some(tail),
            timestamps: true,
            ..Default::default()
        };
        Ok(api.logs(pod, &lp).await?)
    }

    /// Fetch logs for multiple pod/container pairs and merge them with
    /// prefixes.
    pub async fn fetch_logs_multi(&self, ns: &str, pairs: &[(String, String)], tail: i64) -> Result<Vec<String>> {
        let mut all_lines = Vec::new();
        for (pod, container) in pairs {
            let prefix = format!("[{}/{}] ", pod, container);
            match self.fetch_logs(ns, pod, container, tail).await {
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
            .and_then(|conds| {
                conds
                    .iter()
                    .find(|c| c.type_ == "Ready")
                    .map(|c| if c.status == "True" { "Ready" } else { "NotReady" })
            })
            .unwrap_or("Unknown");
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
                ready.to_string(),
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
            .involved_object
            .name
            .as_ref()
            .map(|n| format!("{}/{}", e.involved_object.kind.as_deref().unwrap_or(""), n))
            .unwrap_or_default();
        let message = e.message.clone().unwrap_or_default();
        let count = e.count.unwrap_or(1);
        // Use lastTimestamp (or creationTimestamp) as sort key — ISO 8601 sorts
        // lexicographically
        let sort_ts = e
            .last_timestamp
            .as_ref()
            .or(e.metadata.creation_timestamp.as_ref())
            .map(|t| t.0.to_string())
            .unwrap_or_default();
        let age = e
            .last_timestamp
            .as_ref()
            .or(e.metadata.creation_timestamp.as_ref())
            .map(|t| format_age(Some(t)))
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

fn strip_managed_fields(mut val: Value) -> Result<Value> {
    if let Some(metadata) = val.get_mut("metadata") {
        if let Some(map) = metadata.as_object_mut() {
            map.remove("managedFields");
        }
    }
    Ok(val)
}
