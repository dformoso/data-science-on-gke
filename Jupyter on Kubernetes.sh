# Set defaults
# https://cloud.google.com/sdk/gcloud/reference/config/set
# Login
gcloud auth login
gcloud components update
# Set Environment
gcloud config set project data-science-on-kubs
gcloud config set compute/region australia-southeast1
gcloud config set compute/zone australia-southeast1-a
gcloud config list

# Go to the console and ENABLE the following APIs
#   Kubernetes Engine 
#   Cloud Storage
#   Dataproc
#   BigQuery 

# Create a Kubernetes Engine cluster 
# Note: this will fail if the API is not enabled, please enable the API from the 
# provided link in the error message

# First, let's check all available Kubernetes versions
gcloud container get-server-config
# Create a Kubernetes cluster on 1.10.5-gke.3 or above
gcloud container clusters create data-science-on-kubs \
    --machine-type n1-standard-4 \
    --num-nodes 2 --min-nodes 1 --max-nodes 4 \
    --enable-autoscaling \
    --cluster-version 1.10.7-gke.2

# Viewing namespaces
kubectl get namespaces
kubectl create namespace dsnamespace
kubectl get namespaces

# Create a Service Account to access GCS and Dataproc
# List roles
gcloud beta iam roles list | grep storage
gcloud beta iam roles list | grep dataproc
gcloud beta iam roles list | grep bigquery
# Grant yourself the rights to create keys for service accounts
gcloud projects add-iam-policy-binding data-science-on-kubs \
    --member user:dsnamespace@google.com \
    --role roles/iam.serviceAccountKeyAdmin
# Create a service account
gcloud iam service-accounts create analyst
gcloud iam service-accounts list
# Create keys for service account
gcloud iam service-accounts keys create credentials.json \
    --iam-account analyst@data-science-on-kubs.iam.gserviceaccount.com
cat credentials.json
# Bind needed roles to service account
gcloud projects add-iam-policy-binding data-science-on-kubs \
    --member serviceAccount:analyst@data-science-on-kubs.iam.gserviceaccount.com \
    --role roles/storage.admin
gcloud projects add-iam-policy-binding data-science-on-kubs \
    --member serviceAccount:analyst@data-science-on-kubs.iam.gserviceaccount.com \
    --role roles/dataproc.editor
gcloud projects add-iam-policy-binding data-science-on-kubs \
    --member serviceAccount:analyst@data-science-on-kubs.iam.gserviceaccount.com \
    --role roles/bigquery.admin

# Change jupyter Replication Controller to use service account
# First, create a Kubernetes Secret with the service account json
kubectl create secret generic analyst-key \
    --from-file=key.json=credentials.json \
    --namespace dsnamespace
# Point replication controller to the analyst-key, mount it, create ENV
echo """kind: ReplicationController
apiVersion: v1
metadata:
  name: jupyter-controller
spec:
  replicas: 1
  selector:
    component: jupyter
  template:
    metadata:
      labels:
        component: jupyter
    spec:
      volumes:
        - name: analyst-key
          secret:
            secretName: analyst-key
      containers:
        - name: jupyter
          image: jupyter/all-spark-notebook:latest
          volumeMounts:
          - name: analyst-key
            mountPath: /var/secrets/google
          env:
          - name: GOOGLE_APPLICATION_CREDENTIALS
            value: /var/secrets/google/key.json
          ports:
            - containerPort: 8888
          resources:
            requests:
              cpu: 100m
          command: [\"start-notebook.sh\"]
          args: [\"--NotebookApp.token=''\", \"--ip 0.0.0.0\"]
""" > "jupyter-controller.yaml"
cat jupyter-controller.yaml

kubectl create -f jupyter-controller.yaml --namespace dsnamespace

# Create Jupyter Service
echo """kind: Service
apiVersion: v1
metadata:
  name: jupyter
spec:
  ports:
    - port: 80
      targetPort: 8888
  selector:
    component: jupyter
  type: LoadBalancer""" > "jupyter-service.yaml"
cat jupyter-service.yaml

kubectl create -f jupyter-service.yaml --namespace dsnamespace

# Get all pods in Kubernetes
# Check the Public IP address for Jupyter
kubectl get all --namespace dsnamespace

# Set up the Secure Proxy to Jupyter tunnel (if needed)
# Visit http://localhost:8080/
#kubectl port-forward jupyter-controller-dbjfs 8080:8080 --namespace dsnamespace

# Delete it all
kubectl delete namespace dsnamespace
gcloud container clusters delete data-science-on-kubs

