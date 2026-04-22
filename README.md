# DSFP in a Box 🚀

Digital Sample Freezing Platform - Complete containerized solution for chemical data analysis and processing.

## 📋 Quick Start

### 1. **Configure Ports (Optional)**
<<<<<<< HEAD
All service ports can be customized in an `.env` file:
=======
All service ports can be customized in the `.env` file:
>>>>>>> 8c86128517b155af26c03b30778a8d7a9fdad1cc

```bash
# Default ports - modify as needed
ELASTICSEARCH_PORT=9200      # Database
STATUS_DASHBOARD_PORT=9000   # Dashboard
SEMIQUANTIFICATION_PORT=8001     # Semiquantification service
SPECTRAL_SIMILARITY_PORT=8002    # Spectral similarity score service
```

### 2. **Start All Services**
```bash
docker compose up -d
```

### 3. **Access the Dashboard**
Open your browser to: `http://localhost:9000` (or your configured `STATUS_DASHBOARD_PORT`)

## 🔧 Port Configuration

### Why Configure Ports?
- **Avoid conflicts** with other running services
- **Run multiple instances** of DSFP simultaneously  
- **Customize for your environment** (development, production, etc.)

### How to Change Ports
1. **Edit `.env` file** - Change any port variables
2. **Restart services** - `docker compose down && docker compose up -d`
3. **Update bookmarks** - Use new port numbers in URLs

📖 **[Complete Port Configuration Guide](PORT_CONFIGURATION.md)**

## 🏗️ Architecture

### Services Overview
| Service | Purpose | Default Port | Configuration |
|---------|---------|-------------|---------------|
| **Status Dashboard** | System management and monitoring | 9000 | `STATUS_DASHBOARD_PORT` |
| **Elasticsearch** | Data storage and search | 9200 | `ELASTICSEARCH_PORT` |
| **Semiquantification** | Function for semiquantification | 8001 | `SEMIQUANTIFICATION_PORT` |
| **Spectral Similarity** | Scoring function for spectral analysis | 8002 | `SPECTRAL_SIMILARITY_PORT` |
| **Data Loader** | Background data processing | N/A | (Internal only) |

### Data Flow
```
Data Loader → Screening index → Screening → Resultsd7a9fdad1cc
```

## 📊 Usage

### 1. **Data Management**
- **Upload**: Use the Data Loader interface to upload ZIP files
- **Browse**: View extracted files in the dashboard
- **Process**: Automatically loads data into Elasticsearch

### 2. **Analysis**
- **API Access**: Use the DSFP Server API for data queries
- **Model Integration**: Automatic integration with analysis models
- **Results**: View and export analysis results

### 3. **Monitoring**
- **Service Status**: Monitor all containers from the dashboard
- **Health Checks**: Automatic monitoring of service health
- **Logs**: Access service logs for troubleshooting

## 🔍 Development

### Project Structure
```
dsfp-in-a-box/
├── .env                    # Port and environment configuration
├── docker-compose.yml     # Service orchestration
├── data/                  # Extracted data files
├── status-dashboard/     # Management interface
├── data-loader/          # Data processing service
├── models/               # Machine learning models
│   ├── semiquantification/
│   └── spectral-similarity/
└── setup/                # Initialization scripts
```

### Environment Files
- **`.env`** - Main configuration (ports, URLs, credentials)

### Custom Configurations
```bash
# Development setup
ELASTICSEARCH_PORT=9201
STATUS_DASHBOARD_PORT=9001

# Production setup  
ELASTICSEARCH_PORT=9200
STATUS_DASHBOARD_PORT=9000

# Multiple instances
# Instance 1: Use default ports
# Instance 2: Add +10 to all ports
```

## 🛠️ Troubleshooting

### Common Issues

**Port Conflicts**
```bash
# Check what's using a port
netstat -an | findstr :9000

# Change port in .env and restart
STATUS_DASHBOARD_PORT=9001
docker compose restart status-dashboard
```

**Service Won't Start**
```bash
# Check service status
docker compose ps

# View service logs
docker compose logs elasticsearch
docker compose logs status-dashboard
```

**Can't Access Dashboard**
1. Verify port configuration in `.env`
2. Check if service is running: `docker compose ps`
3. Try accessing via IP: `http://127.0.0.1:9000`
4. Check firewall settings

### Getting Help
- 📖 [Port Configuration Guide](PORT_CONFIGURATION.md)
- 🐳 Check Docker logs: `docker compose logs [service-name]`
- 🔧 Restart services: `docker compose restart`
- 🆘 Full reset: `docker compose down && docker compose up -d`

## 📝 License
This project is part of the NORMAN Database System.

---

**💡 Tip**: Use `docker compose ps` to see all running services and their port mappings.
