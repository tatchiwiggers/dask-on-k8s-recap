# Recap 🕵️

Let's create our Kubernetes Cluster and connect Dask to it to scale-up our processing!

<br>

# 0️⃣ Where are we in the lecture?

Today we covered different strategies to make our processing faster:

1. **Improve process efficiency** ⚙️ - faster code / a bigger machine (Moore's law has limits!)
2. **Parallelize** - use all the cores of *one* machine (threads for I/O, processes for CPU)
3. **Distribute** - use the cores of *many* machines 👈 **this recap!**
4. **Spark vs Map Reduce**
5. **Spark usage**

❓ **Ask the class:** we can already parallelize on our VM with `multiprocessing`. Why would we need more than one machine?

<details>
<summary markdown='span'>🎁 Answer</summary>

- A single machine has a **ceiling** on CPUs and RAM - vertical scaling gets expensive fast and eventually stops.
- **1 billion cells** of NY taxi data won't fit in our VM's RAM.
- With a cluster we scale **horizontally**: need more power? Add more nodes/pods.

</details>

Today we'll do it with **Dask** (Python-native, pandas/numpy-like API). Spark solves the same problem - we'll compare at the end.

<br>

# 1️⃣ Create the cluster ☸️

🚨 **Cost alert**: 4 × `e2-standard-2` nodes ≈ **$0.90/hour**. We delete it at the end!

```bash
gcloud container clusters create mydaskcluster \
--machine-type e2-standard-2 \
--num-nodes 2 \
--disk-size "30" \
--zone=europe-west1-b \
--node-locations=europe-west1-b,europe-west1-c
```

❓ `--num-nodes 2` but we get **4** machines. Why?

<details>
<summary markdown='span'>🎁 Answer</summary>

`--num-nodes` is **per zone**: 2 nodes × 2 node-locations = 4 nodes.

</details>

⏰ It takes ~10 minutes to provision - let's talk through the architecture while we wait.

<br>

# 2️⃣ The architecture 🏗️

```
                 Your VM                                  GKE cluster
┌──────────────────────────────┐          ┌──────────────────────────────────────┐
│ Jupyter notebook             │          │  ┌──────────────────┐                │
│   Client(cluster) ───────────┼──────────┼─▶│ scheduler pod    │ :8786 (tasks)  │
│                              │          │  │ (the "brain")    │ :80 (dashboard)│
│ localhost:8000 (dashboard) ◀─┼─port-fwd─┼──└────────┬─────────┘                │
└──────────────────────────────┘          │           │ sends tasks              │
                                          │   ┌───────┼────────┬────────┐        │
                                          │   ▼       ▼        ▼        ▼        │
                                          │ worker  worker   worker   worker     │
                                          │ 1 CPU   1 CPU    1 CPU    1 CPU      │
                                          └──────────────────────────────────────┘
```

- **Client** (our notebook): builds the task graph and submits it.
- **Scheduler**: decides which worker runs which task, tracks results.
- **Workers**: do the actual computation, in parallel.

This is the same **driver / executor** pattern Spark uses!

<br>

# 3️⃣ Install Dask with Helm 📦

**Helm** is the package manager for Kubernetes: one chart = all the deployments, services and pods Dask needs.

```bash
helm repo add dask https://helm.dask.org/
helm repo update
```

Let's look at [`config.yaml`](config.yaml) together - it overrides the chart's defaults:

```yaml
worker:
  replicas: 4 # number of worker pods
  resources:
    limits: # max allowed per pod
      cpu: 1
      memory: 4Gi
    requests: # requested per pod
      cpu: 1
      memory: 3Gi
  env: # Install packages to match the version on our VM (see pyproject.toml)
    - name: EXTRA_PIP_PACKAGES
      value: pyarrow==10.0.1 dask==2022.12.0 pandas==1.3.0 numpy==1.24.1

jupyter:
  enabled: false
```

❓ **Why do the package versions need to match our VM exactly?**

<details>
<summary markdown='span'>🎁 Answer</summary>

Our functions and data are **pickled** on the client and **unpickled** on the workers. If `pandas` is a different version on each side, a DataFrame serialized on one side may not deserialize (or behave the same) on the other. That's also why we downgraded our local Python to **3.8** - to match the chart's image.

</details>

Check we're pointing at the right cluster, then install:

```bash
kubectl config current-context
kubectl get nodes

helm install mydask dask/dask -f config.yaml --version=2022.11.0
```

Inspect what Helm created:

```bash
kubectl get deployments
kubectl get services
kubectl get pods
kubectl top nodes
```

❓ We have 4 nodes × 2 vCPUs = **8 CPUs**, but only 4 workers with 1 CPU each. Where's the rest?

<details>
<summary markdown='span'>🎁 Answer</summary>

- 1 CPU goes to the **scheduler** pod.
- Part of each node is reserved for **Kubernetes system pods** (kube-proxy, logging, etc.).
- The remaining ~3 CPUs are idle - we could tune `replicas` / `resources` to use them.

</details>

Open the dashboard by port-forwarding the scheduler **service**:

```bash
kubectl port-forward services/mydask-scheduler 8000:80
```

👉 http://localhost:8000

<br>

# 4️⃣ Connect Dask to the cluster 🔌

Open `dask.ipynb` in **Jupyter** (not VS Code - some Dask extensions don't work there).

```python
from dask_kubernetes import HelmCluster
from dask.distributed import Client

cluster = HelmCluster(release_name="mydask")  # the name we gave in `helm install`
client = Client(cluster)                       # every .compute() now runs on the cluster
```

⚠️ Version-mismatch warnings are fine as long as they're not about libraries we actually use in our tasks.

## Scaling from Python

```python
import dask.array as da

cluster.scale(1)

x = da.random.random((50_000, 50_000), chunks=(5_000, 5_000))  # 2.5 billion cells!
y = x + x.T
z = y[::2, 25_000:].mean(axis=1)

%time z.compute()
```

Then:

```python
cluster.scale(4)
# re-run the cell above 👆 and watch the dashboard
```

❓ **Ask the class:**
- Does the array ever exist in full in one place? *(No - it's split into 100 chunks of 5,000 × 5,000; each worker only holds a few.)*
- What happens when we call `x + x.T`? *(Nothing! Dask is **lazy** - it only builds a task graph until `.compute()`.)*
- Did 4 workers give us a 4× speedup? Why not exactly? *(Scheduling overhead + data shuffling between workers for `x.T`.)*

<br>

# 5️⃣ A real use case: 24 months of NY taxi data 🚕

**Goal:** summary statistics on yellow taxi trips for `2020-2021` - 24 parquet files, ~60M rows.

## Pandas: one file at a time 🐌

```python
monthly_statistics = []
for year in [2020, 2021]:
    for month in range(1, 13):
        df = pd.read_parquet(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet")
        monthly_statistics.append(df.describe())
```

Sequential: download → describe → download → describe... and everything goes through our VM's RAM.

## Dask: all files at once 🚀

```python
import dask

@dask.delayed
def monthly_describe(year: int, month: int):
    df = pd.read_parquet(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet")
    return df.describe()

monthly_data = [monthly_describe(year, month) for year in [2020, 2021] for month in range(1, 13)]
monthly_data  # 24 Delayed objects - nothing has run yet!

%time monthly_statistics = dask.compute(monthly_data)[0]
```

Combine the results on the client:

```python
global_means = sum(s.loc["mean", :] for s in monthly_statistics) / len(monthly_statistics)
```

❓ **Spot the pattern!** Where have we seen this before?

<details>
<summary markdown='span'>🎁 Answer</summary>

It's **Map Reduce**!
- **Map**: `monthly_describe` runs independently on each file, on whichever worker is free.
- **Reduce**: we combine the 24 small `describe()` results into global means.

Only the tiny summaries travel back to our VM - never the 60M rows. **Move the compute to the data, not the data to the compute.**

</details>

❓ On the dashboard, open a task's profile: is the time spent on **I/O** (downloading parquet) or **CPU** (`describe`)? What does that tell us?

<details>
<summary markdown='span'>🎁 Answer</summary>

Mostly **I/O**. Each worker downloads in parallel, so we multiply our network bandwidth too - not just our CPUs.

⚠️ Small caveat: averaging the 24 monthly means gives each month equal weight, even though months have different row counts (hello, April 2020 🦠). For an exact mean, weight by `count`.

</details>

<br>

# 6️⃣ Dask vs Spark ⚡

| | **Dask** | **Spark** |
|---|---|---|
| Language | Pure Python | Scala/JVM (PySpark API in Python) |
| API | Mirrors pandas / numpy / scikit-learn | Its own DataFrame API + SQL |
| Evaluation | Lazy (`.compute()`) | Lazy (actions like `.show()`, `.collect()`) |
| Architecture | Client → scheduler → workers | Driver → cluster manager → executors |
| Best for | Scaling existing Python code, arrays, custom tasks | Large-scale ETL, SQL, mature big-data ecosystem |
| Managed on GCP | GKE + Helm (what we did today) | **Dataproc** |

Same core ideas: **lazy task graphs**, **partitioned data**, **map-reduce style** computation, **scale horizontally**.

<br>

# 🏁 Delete your cluster 🚨

```bash
helm uninstall mydask
gcloud container clusters delete mydaskcluster --zone=europe-west1-b
```

Double check nothing is left running:

```bash
gcloud container clusters list
```

<br>
