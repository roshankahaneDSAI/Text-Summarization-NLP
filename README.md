# 📝 **End-to-End Text Summarizer Project**

A complete **production-grade text summarization pipeline** built with modular configuration, reusable components, and deployment readiness. This project follows a proper **ML pipeline architecture** including configuration management, components, pipeline orchestration, API app, and CI/CD for AWS deployment.

---

## ⚙️ **Project Workflow**

The project is structured into multiple configurable and modular steps:

1. **Update `config.yaml`** – Centralized configuration for paths, models, data, etc.
2. **Update `params.yaml`** – Hyperparameters and model-specific settings.
3. **Update entities** – Dataclasses to map config objects (input schema).
4. **Update Configuration Manager** – Logic that reads YAML → entities.
5. **Update components** – Feature engineering, model building, evaluation, etc.
6. **Update pipeline** – Sequence of orchestration scripts.
7. **Update `main.py`** – Entry point to trigger the full pipeline.
8. **Update `app.py`** – Flask/FastAPI app for serving summarization predictions.

---

# 🚀 **How to Run Locally**

### **STEP 1 — Clone this repository**

```bash
git clone https://github.com/entbappy/End-to-end-Text-Summarization
cd End-to-end-Text-Summarization
```

---

### **STEP 2 — Create Conda Environment**

```bash
conda create -n summary python=3.8 -y
conda activate summary
```

---

### **STEP 3 — Install Dependencies**

```bash
pip install -r requirements.txt
```

---

### **STEP 4 — Run the Application**

```bash
python app.py
```

Then open your browser and visit:

```
http://localhost:<port>/
```

---

## 👨‍💻 **Author**

**Roshan Kahane**
Data Scientist
📧 Email: **[roshankahane09@gmail.com](mailto:roshankahane09@gmail.com)**

---

---

# 🐳 **AWS CI/CD Deployment With GitHub Actions**

This project includes detailed steps for deploying your Text Summarizer application to **AWS EC2** using **GitHub Actions**, **Docker**, and **AWS ECR**.

---

## 🔐 **1. Login to AWS Console**

Ensure you have access to AWS IAM, EC2, ECR.

---

## 👤 **2. Create IAM User for Deployment**

The IAM user must have permissions for:

### **Required AWS Services**

* **EC2 Access** → To launch and run your backend server
* **ECR Access** → To store your Docker images

### **IAM Policies to Attach**

* `AmazonEC2ContainerRegistryFullAccess`
* `AmazonEC2FullAccess`

---

## 🗄 **3. Create an ECR Repository**

Example:

```
566373416292.dkr.ecr.us-east-1.amazonaws.com/text-s
```

Save the **ECR URI** – it’s needed for GitHub Secrets.

---

## 💻 **4. Create EC2 Machine (Ubuntu)**

Choose t2.micro (free-tier) or larger depending on load.

---

## 🐳 **5. Install Docker on EC2 Instance**

SSH into EC2 and run:

```bash
sudo apt-get update -y
sudo apt-get upgrade -y
```

Required:

```bash
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker ubuntu
newgrp docker
```

---

## 🤖 **6. Configure EC2 as a GitHub Self-Hosted Runner**

Go to:

```
GitHub Repo → Settings → Actions → Runners → New Self-Hosted Runner
```

Choose OS → Run the commands provided.

This lets GitHub Actions deploy to EC2 automatically.

---

## 🔑 **7. Set Up GitHub Secrets**

Go to:

```
GitHub Repo → Settings → Secrets → Actions
```

Add the following:

| Secret Name             | Example                                         |
| ----------------------- | ----------------------------------------------- |
| `AWS_ACCESS_KEY_ID`     | your IAM key                                    |
| `AWS_SECRET_ACCESS_KEY` | your IAM secret                                 |
| `AWS_REGION`            | us-east-1                                       |
| `AWS_ECR_LOGIN_URI`     | `566373416292.dkr.ecr.ap-south-1.amazonaws.com` |
| `ECR_REPOSITORY_NAME`   | `simple-app`                                    |

---

# ⚙️ Deployment Flow (GitHub Actions)

**Your CI/CD pipeline performs:**

1️⃣ Build Docker image
2️⃣ Authenticate to AWS ECR
3️⃣ Push image to ECR
4️⃣ Trigger EC2 runner
5️⃣ Pull Docker image from ECR
6️⃣ Run the container in EC2

This ensures **automated end-to-end deployment** on every code push.

---

# ✔️ Summary

This project demonstrates:

🔹 End-to-end ML pipeline design
🔹 Config-driven architecture
🔹 Automated summarization app with Flask
🔹 Full CI/CD deployment to AWS using Docker + GitHub Actions
🔹 Modular and extendable production-ready codebase

---


📌 GitHub Actions CI/CD workflow (`deploy.yml`)
📌 Architecture diagram
📌 API documentation for your summarizer

Just tell me!
