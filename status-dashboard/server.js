const express = require('express');
const Docker = require('dockerode');
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const fs = require('fs');
const StreamZip = require('node-stream-zip');
const axios = require('axios');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const app = express();
const PORT = process.env.STATUS_DASHBOARD_PORT || 3008;
const docker = new Docker();

// Middleware
app.use(cors());
app.use(express.json());

// Disable caching for DSFP API routes to avoid browser 304 responses
app.use('/api/dsfp', (req, res, next) => {
    res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.set('Pragma', 'no-cache');
    res.set('Expires', '0');
    res.set('Surrogate-Control', 'no-store');
    next();
});
app.use((req, res, next) => {
    if (req.path.endsWith('.html')) {
        return res.redirect(301, req.path.slice(0, -5) || '/');
    }

    next();
});
app.use(express.static(path.join(__dirname, 'public'), { extensions: ['html'] }));

app.get('/header', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'header.html'));
});

// Configure multer for file uploads
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        const uploadDir = path.join(__dirname, '..', 'temp-uploads');
        // Create directory if it doesn't exist
        if (!fs.existsSync(uploadDir)) {
            fs.mkdirSync(uploadDir, { recursive: true });
        }
        cb(null, uploadDir);
    },
    filename: function (req, file, cb) {
        // Use timestamp to avoid conflicts
        cb(null, `data-${Date.now()}.zip`);
    }
});

const upload = multer({ 
    storage: storage,
    limits: {
        fileSize: 5 * 1024 * 1024 * 1024 // 5GB limit
    },
    fileFilter: function (req, file, cb) {
        if (file.mimetype === 'application/zip' || file.originalname.toLowerCase().endsWith('.zip')) {
            cb(null, true);
        } else {
            cb(new Error('Only ZIP files are allowed'), false);
        }
    }
});

// Elasticsearch configuration
const ELASTICSEARCH_URL = process.env.ELASTICSEARCH_URL || 'http://elasticsearch:9200';

// Helper function to get the host path for the data directory
// In Docker, __dirname is /app, and data is mounted from host's ./data to /app/data
// We need to return the actual host path instead of the container path
function getHostDataPath() {
    const containerDataPath = path.join(__dirname, 'data');
    
    // Check if we're running in Docker (by checking if we're in /app directory)
    if (__dirname === '/app' || __dirname.startsWith('/app')) {
        // We're in Docker, need to construct the host path
        // Check if HOST_DATA_PATH environment variable is set
        if (process.env.HOST_DATA_PATH) {
            // Return the path with proper separators for the host OS
            // On Windows, Docker Desktop usually runs on Windows, so convert to backslashes
            // On Linux, keep forward slashes
            const isWindowsHost = process.platform === 'win32' || process.env.HOST_DATA_PATH.includes(':\\');
            
            if (isWindowsHost) {
                // Convert forward slashes to backslashes for Windows
                return process.env.HOST_DATA_PATH.replace(/\//g, '\\');
            } else {
                // Keep forward slashes for Linux/Mac
                return process.env.HOST_DATA_PATH;
            }
        }
        
        // Fallback: return container path
        return containerDataPath;
    }
    
    // Not in Docker, return the actual path with native separators
    return containerDataPath;
}

// Discover the actual host path mounted at /app/data in the status-dashboard
// container. This is the path Docker on the host can bind into the one-off
// processing containers.
async function getProcessingHostDataPath() {
    try {
        const container = docker.getContainer('status-dashboard');
        const info = await container.inspect();
        const mount = (info.Mounts || []).find(m => m.Destination === '/app/data');
        if (mount && mount.Source) {
            return mount.Source;
        }
    } catch (error) {
        console.warn('Could not inspect status-dashboard mount for /app/data:', error.message);
    }
    return getHostDataPath();
}
const SCREENING_INDEX = 'dsfp-screening-index';
// Note: TRACKING_INDEX removed - now using DuckDB for tracking

// Elasticsearch helper functions
async function createIndexIfNotExists(indexName, mappings = null) {
    try {
        // Check if index exists
        const response = await axios.head(`${ELASTICSEARCH_URL}/${indexName}`);
        console.log(`Index ${indexName} already exists`);
        return true;
    } catch (error) {
        if (error.response && error.response.status === 404) {
            // Index doesn't exist, create it
            try {
                const createResponse = await axios.put(`${ELASTICSEARCH_URL}/${indexName}`, mappings || {});
                console.log(`Created index ${indexName}`);
                return true;
            } catch (createError) {
                console.error(`Error creating index ${indexName}:`, createError.message);
                return false;
            }
        } else {
            console.error(`Error checking index ${indexName}:`, error.message);
            return false;
        }
    }
}

async function ensureScreeningIndex() {
    const mappingsPath = '/app/setup/mappings.json';
    if (!fs.existsSync(mappingsPath)) {
        const fallbackPath = path.join(__dirname, '..', 'setup', 'mappings.json');
        if (!fs.existsSync(fallbackPath)) {
            throw new Error(`Mappings file not found at: ${mappingsPath} or ${fallbackPath}`);
        }

        const screeningMappings = JSON.parse(fs.readFileSync(fallbackPath, 'utf8'));
        return createIndexIfNotExists(SCREENING_INDEX, screeningMappings);
    }

    const screeningMappings = JSON.parse(fs.readFileSync(mappingsPath, 'utf8'));
    return createIndexIfNotExists(SCREENING_INDEX, screeningMappings);
}

async function isSamplePrepared(sampleId) {
    try {
        const response = await axios.head(`${ELASTICSEARCH_URL}/${SCREENING_INDEX}/_doc/${sampleId}`, { validateStatus: () => true });
        return response.status === 200;
    } catch (error) {
        return false;
    }
}

async function validateJsonFormat(jsonData) {
    // Validate that the JSON has the required fields for DSFP screening
    if (!jsonData.sample_id || !jsonData.short_name || !jsonData.fullscan) {
        return false;
    }
    
    // Validate fullscan structure
    if (!Array.isArray(jsonData.fullscan)) {
        return false;
    }
    
    // Check that fullscan items have required properties
    for (const scan of jsonData.fullscan) {
        if (typeof scan.mz !== 'number' || typeof scan.rt_minutes !== 'number') {
            return false;
        }
    }
    
    return true;
}

async function bulkInsertToElasticsearch(documents, indexName) {
    if (documents.length === 0) return { success: true, inserted: 0 };
    
    // For large documents, insert one by one to avoid 413 errors
    if (documents.length === 1 || JSON.stringify(documents).length > 10 * 1024 * 1024) { // 10MB limit
        return await insertDocumentsOneByOne(documents, indexName);
    }
    
    // Prepare bulk request body
    const bulkBody = [];
    for (const doc of documents) {
        // Index operation
        bulkBody.push({
            index: {
                _index: indexName,
                _id: doc._id || undefined
            }
        });
        // Document data
        bulkBody.push(doc.data || doc);
    }
    
    try {
        const response = await axios.post(`${ELASTICSEARCH_URL}/_bulk`, 
            bulkBody.map(item => JSON.stringify(item)).join('\n') + '\n',
            {
                headers: {
                    'Content-Type': 'application/x-ndjson'
                },
                timeout: 120000, // 2 minute timeout for large uploads
                maxContentLength: 50 * 1024 * 1024, // 50MB max content
                maxBodyLength: 50 * 1024 * 1024 // 50MB max body
            }
        );
        
        if (response.data.errors) {
            console.error('Bulk insert errors:', response.data.items.filter(item => item.index && item.index.error));
            return { 
                success: false, 
                error: 'Some documents failed to insert',
                details: response.data.items.filter(item => item.index && item.index.error)
            };
        }
        
        return { 
            success: true, 
            inserted: response.data.items.length,
            details: response.data
        };
    } catch (error) {
        console.error('Bulk insert error:', error.message);
        
        // If bulk insert fails with 413, try inserting one by one
        if (error.response && error.response.status === 413) {
            console.log('Bulk insert too large, falling back to individual inserts...');
            return await insertDocumentsOneByOne(documents, indexName);
        }
        
        return { success: false, error: error.message };
    }
}

async function insertDocumentsOneByOne(documents, indexName) {
    let insertedCount = 0;
    const errors = [];
    
    for (const doc of documents) {
        try {
            const documentId = doc._id ?? doc?.data?.sample_id ?? doc?.sample_id;
            if (documentId === undefined || documentId === null || documentId === '') {
                throw new Error('Missing document id for screening index insert');
            }

            const documentBody = doc.data || doc;
            const response = await axios.put(`${ELASTICSEARCH_URL}/${indexName}/_doc/${encodeURIComponent(String(documentId))}`, 
                documentBody,
                {
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    timeout: 60000, // 1 minute timeout per document
                    maxContentLength: 50 * 1024 * 1024, // 50MB max content
                    maxBodyLength: 50 * 1024 * 1024 // 50MB max body
                }
            );
            insertedCount++;
            
            // Add small delay to prevent overwhelming Elasticsearch
            await new Promise(resolve => setTimeout(resolve, 50));
            
        } catch (error) {
            // Log detailed error information
            const documentId = doc._id ?? doc?.data?.sample_id ?? doc?.sample_id ?? 'unknown';
            console.error(`Error inserting document ${documentId}:`);
            console.error('  Message:', error.message);
            if (error.response) {
                console.error('  Status:', error.response.status);
                console.error('  Data:', JSON.stringify(error.response.data, null, 2));
            }
            
            const errorDetail = error.response?.data?.error || error.message;
            errors.push({ id: documentId, error: errorDetail });
        }
    }
    
    return {
        success: errors.length === 0,
        inserted: insertedCount,
        errors: errors
    };
}

async function insertTrackingRecord(filename, filepath, sampleId, shortName, sampleType = null, ionizationType = null, status = 'unprocessed') {
    // Note: This function now saves to DuckDB instead of Elasticsearch tracking index
    console.log(`Recording tracking info for sample ${sampleId} (${shortName}) in DuckDB`);
    
    try {
        const { spawn } = require('child_process');
        // Escape single quotes in shortName for Python string
        const escapedShortName = (shortName || '').replace(/'/g, "\\'");
        
        const pythonScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

try:
    # Use context manager to ensure connection is closed
    with TrackingDatabase() as db:
        # Initialize sample with short_name
        db.initialize_sample_if_needed("${sampleId}", "${escapedShortName}")
        print("SUCCESS: Tracking record saved to DuckDB for sample ${sampleId}")
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
        `;

        const result = await new Promise((resolve, reject) => {
            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';
            let errorOutput = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                errorOutput += data.toString();
            });

            python.on('close', (code) => {
                if (code === 0 && output.includes('SUCCESS:')) {
                    resolve({ success: true, output });
                } else {
                    reject(new Error(`Python script failed: ${errorOutput || output}`));
                }
            });
        });

        return { success: true, message: 'Tracking record saved to DuckDB' };
        
    } catch (error) {
        console.error(`Error inserting tracking record for ${filename}:`, error.message);
        return { success: false, error: error.message };
    }
}

// Get container status
app.get('/api/containers', async (req, res) => {
    try {
        const [containers, images] = await Promise.all([
            docker.listContainers({ all: true }),
            docker.listImages()
        ]);
        
        // Include active ephemeral processing jobs. They are the real runtime
        // containers for parse/componentize/jsoncreate and disappear on exit.
        const dsfpContainers = containers.filter(container => {
            const name = container.Names[0].replace('/', '');
            if (PROCESSING_PIPELINE.some(service => name === `dsfp-${service}`)) return false;
            return (name.startsWith('dsfp-') || 
                   name.includes('dsfp-in-a-box') || 
                   name === 'elasticsearch') &&
                   name !== 'dsfp-server';
        });
        
        const containerInfo = dsfpContainers.map(container => {
            const isRunning = container.State === 'running';
            const name = container.Names[0].replace('/', '');
            
            // Extract service info
            let serviceType = 'unknown';
            if (name === 'elasticsearch') serviceType = 'database';
            else if (name === 'dsfp-screening-service') serviceType = 'api';
            else if (name === 'dsfp-semiquantification') serviceType = 'analysis';
            else if (name === 'dsfp-spectral-similarity') serviceType = 'analysis';
            else if (name === 'dsfp-data-loader') serviceType = 'data-loading';
            else if (name.includes('init-elasticsearch')) serviceType = 'setup';
            else if (name.startsWith('dsfp-job-')) serviceType = 'processing-job';
            
            return {
                id: container.Id.substring(0, 12),
                name: name,
                image: container.Image,
                state: container.State,
                status: container.Status,
                isRunning: isRunning,
                serviceType: serviceType,
                ports: (container.Ports || []).map(port => ({
                    privatePort: port.PrivatePort,
                    publicPort: port.PublicPort,
                    type: port.Type
                })),
                created: new Date(container.Created * 1000).toISOString(),
                uptime: isRunning ? getUptime(container.Status) : null,
                manageable: !name.startsWith('dsfp-job-')
            };
        });

        const processingServices = PROCESSING_PIPELINE.map(service => {
            const imageName = PROCESSING_IMAGES[service];
            const image = images.find(item => (item.RepoTags || []).includes(imageName));
            const worker = containers.find(container =>
                container.Names.some(name => name === `/dsfp-${service}`)
            );
            const activeJobs = containerInfo.filter(container =>
                container.name.startsWith(`dsfp-job-${service}-`) && container.isRunning
            ).length;
            const isRunning = worker && worker.State === 'running';
            return {
                id: worker ? worker.Id.substring(0, 12) : 'not-created',
                name: service,
                image: imageName,
                state: worker ? worker.State : (image ? 'not-created' : 'missing'),
                status: activeJobs ? `${worker ? worker.Status + '; ' : ''}${activeJobs} active job${activeJobs === 1 ? '' : 's'}` :
                    (worker ? worker.Status : (image ? 'Run docker compose up to create worker' : 'Image not built')),
                isRunning: !!isRunning,
                serviceType: 'processing-worker',
                ports: [],
                created: worker ? new Date(worker.Created * 1000).toISOString() : null,
                uptime: isRunning ? getUptime(worker.Status) : null,
                manageable: !!worker
            };
        });

        const recentJobs = Array.from(processingJobs.entries())
            .filter(([, job]) => job.state !== 'processing')
            .sort(([, a], [, b]) => (b.finishedAt || 0) - (a.finishedAt || 0))
            .slice(0, 12)
            .map(([jobId, job]) => ({
                id: job.containerId ? job.containerId.substring(0, 12) : jobId,
                name: `${job.service} sample ${job.sampleId}`,
                image: PROCESSING_IMAGES[job.service],
                state: job.state,
                status: job.error || (job.state === 'completed' ? 'Completed successfully' : job.state),
                isRunning: false,
                serviceType: 'processing-job',
                ports: [],
                created: new Date(job.startedAt).toISOString(),
                uptime: null,
                manageable: false
            }));
        
        res.json({
            success: true,
            containers: [...processingServices, ...containerInfo, ...recentJobs],
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('Error fetching containers:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get system info
app.get('/api/system', async (req, res) => {
    try {
        const info = await docker.info();
        res.json({
            success: true,
            system: {
                containers: info.Containers,
                containersRunning: info.ContainersRunning,
                containersPaused: info.ContainersPaused,
                containersStopped: info.ContainersStopped,
                images: info.Images,
                dockerVersion: info.ServerVersion,
                operatingSystem: info.OperatingSystem,
                architecture: info.Architecture,
                totalMemory: info.MemTotal,
                cpus: info.NCPU
            }
        });
    } catch (error) {
        console.error('Error fetching system info:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Container actions
app.post('/api/containers/:id/start', async (req, res) => {
    try {
        const container = docker.getContainer(req.params.id);
        await container.start();
        res.json({ success: true, action: 'started' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/containers/:id/stop', async (req, res) => {
    try {
        const container = docker.getContainer(req.params.id);
        await container.stop();
        res.json({ success: true, action: 'stopped' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/containers/:id/restart', async (req, res) => {
    try {
        const container = docker.getContainer(req.params.id);
        await container.restart();
        res.json({ success: true, action: 'restarted' });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Custom logic endpoint - placeholder for future extensions
app.get('/api/custom/health-check', async (req, res) => {
    try {
        // Get all containers and filter for DSFP project containers
        const containers = await docker.listContainers({ all: true });
        
        // Filter containers that belong to the DSFP project
        const dsfpContainers = containers.filter(container => {
            const name = container.Names[0].replace('/', '');
            return name.startsWith('dsfp-') || 
                   name.includes('dsfp-in-a-box') || 
                   name === 'elasticsearch';
        });
        
        const runningDsfpContainers = dsfpContainers.filter(c => c.State === 'running');
        
        // Define DSFP required services
        const requiredServices = [
            'elasticsearch', 
            'dsfp-screening-service'
        ];
        
        const runningServices = runningDsfpContainers.map(c => c.Names[0].replace('/', ''));
        
        const healthStatus = {
            overall: 'healthy',
            services: {},
            recommendations: [],
            totalDsfpContainers: dsfpContainers.length,
            runningDsfpContainers: runningDsfpContainers.length
        };
        
        // Check each required service
        requiredServices.forEach(service => {
            const isRunning = runningServices.some(name => name === service);
            healthStatus.services[service] = {
                status: isRunning ? 'running' : 'stopped',
                required: true
            };
            
            if (!isRunning) {
                healthStatus.overall = 'unhealthy';
                healthStatus.recommendations.push(`Start ${service} container`);
            }
        });
        
        // Check optional services
        const optionalServices = ['status-dashboard', 'dsfp-data-loader'];
        optionalServices.forEach(service => {
            const isRunning = runningServices.some(name => name === service);
            if (dsfpContainers.some(c => c.Names[0].replace('/', '') === service)) {
                healthStatus.services[service] = {
                    status: isRunning ? 'running' : 'stopped',
                    required: false
                };
            }
        });
        
        res.json({
            success: true,
            health: healthStatus,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get Elasticsearch stats
app.get('/api/elasticsearch/stats', async (req, res) => {
    try {
        // Check if Elasticsearch container is running
        const containers = await docker.listContainers();
        const esContainer = containers.find(c => c.Names[0].replace('/', '') === 'elasticsearch');
        
        if (!esContainer || esContainer.State !== 'running') {
            return res.json({
                success: false,
                error: 'Elasticsearch container is not running'
            });
        }

        // Fetch stats from Elasticsearch
        const fetch = require('node-fetch');
        
        try {
            // Get cluster stats
            const statsResponse = await fetch('http://elasticsearch:9200/_cluster/stats');
            const statsData = await statsResponse.json();
            
            // Get indices count and details
            const indicesResponse = await fetch('http://elasticsearch:9200/_cat/indices?format=json');
            const indicesData = await indicesResponse.json();
            
            // Get specific index document counts using _count API for accuracy
            const compoundsCountResponse = await fetch('http://elasticsearch:9200/dsfp-compounds-1/_count');
            const compoundsCountData = await compoundsCountResponse.json();
            
            const samplesCountResponse = await fetch('http://elasticsearch:9200/dsfp-screening-index/_count');
            const samplesCountData = await samplesCountResponse.json();
            
            // Still get indices details for size info
            const compoundsIndex = indicesData.find(idx => idx.index === 'dsfp-compounds-1');
            const samplesIndex = indicesData.find(idx => idx.index === 'dsfp-screening-index');
            
            const stats = {
                indices: indicesData.length,
                totalDocs: statsData.indices?.docs?.count || 0,
                totalSize: statsData.indices?.store?.size_in_bytes || 0,
                clusterName: statsData.cluster_name,
                status: statsData.status,
                indexDetails: {
                    compounds: {
                        exists: !!compoundsIndex,
                        docCount: compoundsCountData.count || 0,
                        size: compoundsIndex ? compoundsIndex['store.size'] : '0b'
                    },
                    samples: {
                        exists: !!samplesIndex,
                        docCount: samplesCountData.count || 0,
                        size: samplesIndex ? samplesIndex['store.size'] : '0b'
                    }
                    // Note: Tracking index removed - now using DuckDB for tracking
                }
            };

            res.json({
                success: true,
                stats: stats,
                timestamp: new Date().toISOString()
            });
            
        } catch (fetchError) {
            res.json({
                success: false,
                error: 'Failed to fetch Elasticsearch stats: ' + fetchError.message
            });
        }
        
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Multer error handling middleware
function handleMulterError(err, req, res, next) {
    if (err instanceof multer.MulterError) {
        if (err.code === 'LIMIT_FILE_SIZE') {
            return res.status(400).json({
                success: false,
                error: `File too large. Maximum size is 500MB.`
            });
        }
        if (err.code === 'LIMIT_FILE_COUNT') {
            return res.status(400).json({
                success: false,
                error: 'Too many files uploaded.'
            });
        }
        if (err.code === 'LIMIT_UNEXPECTED_FILE') {
            return res.status(400).json({
                success: false,
                error: 'Unexpected file field.'
            });
        }
        return res.status(400).json({
            success: false,
            error: `Upload error: ${err.message}`
        });
    }
    
    if (err) {
        return res.status(500).json({
            success: false,
            error: `Server error: ${err.message}`
        });
    }
    
    next();
}

// File upload endpoint
app.post('/api/upload-data', upload.single('zipFile'), handleMulterError, async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({
                success: false,
                error: 'No ZIP file uploaded'
            });
        }

        const fileSizeMB = req.file.size / (1024 * 1024);
        const zipPath = req.file.path;
        const dataDir = path.join(__dirname, 'data');
        
        // Create data directory if it doesn't exist (but don't clear existing files)
        if (!fs.existsSync(dataDir)) {
            fs.mkdirSync(dataDir, { recursive: true });
        }

        // Extract ZIP file
        const zip = new StreamZip.async({ file: zipPath });
        
        // Get ZIP contents for response
        const entries = await zip.entries();
        const extractedFiles = [];
        
        // Extract all files
        await zip.extract(null, dataDir);
        await zip.close();
        
        // Count files and directories
        let totalFiles = 0;
        let totalDirectories = 0;
        
        for (const [name, entry] of Object.entries(entries)) {
            if (entry.isDirectory) {
                totalDirectories++;
            } else {
                totalFiles++;
                extractedFiles.push({
                    name: name,
                    size: entry.size,
                    isDirectory: false
                });
            }
        }
        
        // Clean up temporary ZIP file
        fs.unlinkSync(zipPath);

        // Process JSON files for Elasticsearch insertion
        let esProcessingResults = {
            processed: 0,
            inserted: 0,
            errors: 0,
            trackingRecords: 0
        };

        try {
            // Create Elasticsearch screening index if it doesn't exist
            const screeningMappings = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'setup', 'mappings.json'), 'utf8'));
            
            await createIndexIfNotExists(SCREENING_INDEX, screeningMappings);

            // Process JSON files
            const jsonFiles = extractedFiles.filter(file => file.name.toLowerCase().endsWith('.json'));
            const documentsToInsert = [];
            const trackingRecords = [];

            for (const jsonFile of jsonFiles) {
                try {
                    const filePath = path.join(dataDir, jsonFile.name);
                    const fileContent = fs.readFileSync(filePath, 'utf8');
                    const jsonData = JSON.parse(fileContent);

                    esProcessingResults.processed++;

                    // Validate JSON format
                    if (validateJsonFormat(jsonData)) {
                        // Prepare document for screening index
                        documentsToInsert.push({
                            _id: `${jsonData.sample_id}`, // Use sample_id as document ID
                            data: jsonData
                        });

                        // Prepare tracking record
                        trackingRecords.push({
                            filename: jsonFile.name,
                            filepath: filePath,
                            sampleId: jsonData.sample_id,
                            shortName: jsonData.short_name,
                            sampleType: jsonData.sample_type || null,
                            ionizationType: jsonData.instrument_setup_used?.ionization_type || null
                        });
                    } else {
                        console.warn(`Invalid JSON format for file: ${jsonFile.name}`);
                        esProcessingResults.errors++;
                    }
                } catch (parseError) {
                    console.error(`Error processing JSON file ${jsonFile.name}:`, parseError.message);
                    esProcessingResults.errors++;
                }
            }

            // Bulk insert to screening index
            if (documentsToInsert.length > 0) {
                const insertResult = await bulkInsertToElasticsearch(documentsToInsert, SCREENING_INDEX);
                if (insertResult.success) {
                    esProcessingResults.inserted = insertResult.inserted;
                } else {
                    console.error('Bulk insert failed:', insertResult.error);
                    esProcessingResults.errors += documentsToInsert.length;
                }
            }

            // Insert tracking records
            for (const trackingRecord of trackingRecords) {
                const trackingResult = await insertTrackingRecord(
                    trackingRecord.filename,
                    trackingRecord.filepath,
                    trackingRecord.sampleId,
                    trackingRecord.shortName,
                    trackingRecord.sampleType,
                    trackingRecord.ionizationType
                );
                if (trackingResult.success) {
                    esProcessingResults.trackingRecords++;
                }
            }

        } catch (esError) {
            console.error('Elasticsearch processing error:', esError.message);
        }
        
        res.json({
            success: true,
            message: 'ZIP file extracted and processed successfully',
            extractedTo: dataDir,
            filename: req.file.originalname,
            size: req.file.size,
            sizeMB: Math.round(fileSizeMB * 100) / 100,
            totalFiles: totalFiles,
            totalDirectories: totalDirectories,
            extractedFiles: extractedFiles.slice(0, 50), // Limit for performance
            elasticsearch: esProcessingResults
        });
        
    } catch (error) {
        console.error('Upload/extraction error:', error);
        
        // Clean up temporary file if it exists
        if (req.file && fs.existsSync(req.file.path)) {
            try {
                fs.unlinkSync(req.file.path);
            } catch (cleanupError) {
                console.error('Cleanup error:', cleanupError);
            }
        }
        
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Alias endpoint for frontend compatibility
app.post('/api/upload', upload.single('zipFile'), handleMulterError, async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({
                success: false,
                error: 'No ZIP file uploaded'
            });
        }

        const fileSizeMB = req.file.size / (1024 * 1024);
        const zipPath = req.file.path;
        const dataDir = path.join(__dirname, 'data');
        
        // Create data directory if it doesn't exist (but don't clear existing files)
        if (!fs.existsSync(dataDir)) {
            fs.mkdirSync(dataDir, { recursive: true });
        }

        // Extract ZIP file
        const zip = new StreamZip.async({ file: zipPath });
        
        // Get ZIP contents for response
        const entries = await zip.entries();
        const extractedFiles = [];
        
        // Extract all files
        await zip.extract(null, dataDir);
        await zip.close();
        
        // Count files and list extracted files for frontend
        let fileCount = 0;
        const files = [];
        
        for (const [name, entry] of Object.entries(entries)) {
            if (!entry.isDirectory) {
                fileCount++;
                files.push(name);
            }
        }
        
        // Clean up temporary ZIP file
        fs.unlinkSync(zipPath);

        // Process JSON files for Elasticsearch insertion
        let esProcessingResults = {
            processed: 0,
            inserted: 0,
            errors: 0,
            trackingRecords: 0
        };

        try {
            // Create Elasticsearch screening index if it doesn't exist
            const screeningMappings = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'setup', 'mappings.json'), 'utf8'));
            
            await createIndexIfNotExists(SCREENING_INDEX, screeningMappings);

            // Process JSON files
            const jsonFiles = files.filter(file => file.toLowerCase().endsWith('.json'));
            const documentsToInsert = [];
            const trackingRecords = [];

            for (const jsonFile of jsonFiles) {
                try {
                    const filePath = path.join(dataDir, jsonFile);
                    const fileContent = fs.readFileSync(filePath, 'utf8');
                    const jsonData = JSON.parse(fileContent);

                    esProcessingResults.processed++;

                    // Validate JSON format
                    if (validateJsonFormat(jsonData)) {
                        // Prepare document for screening index
                        documentsToInsert.push({
                            _id: `${jsonData.sample_id}`, // Use sample_id as document ID
                            data: jsonData
                        });

                        // Prepare tracking record
                        trackingRecords.push({
                            filename: jsonFile,
                            filepath: filePath,
                            sampleId: jsonData.sample_id,
                            shortName: jsonData.short_name,
                            sampleType: jsonData.sample_type || null,
                            ionizationType: jsonData.instrument_setup_used?.ionization_type || null
                        });
                    } else {
                        console.warn(`Invalid JSON format for file: ${jsonFile}`);
                        esProcessingResults.errors++;
                    }
                } catch (parseError) {
                    console.error(`Error processing JSON file ${jsonFile}:`, parseError.message);
                    esProcessingResults.errors++;
                }
            }

            // Bulk insert to screening index
            if (documentsToInsert.length > 0) {
                const insertResult = await bulkInsertToElasticsearch(documentsToInsert, SCREENING_INDEX);
                if (insertResult.success) {
                    esProcessingResults.inserted = insertResult.inserted;
                } else {
                    console.error('Bulk insert failed:', insertResult.error);
                    esProcessingResults.errors += documentsToInsert.length;
                }
            }

            // Insert tracking records
            for (const trackingRecord of trackingRecords) {
                const trackingResult = await insertTrackingRecord(
                    trackingRecord.filename,
                    trackingRecord.filepath,
                    trackingRecord.sampleId,
                    trackingRecord.shortName,
                    trackingRecord.sampleType,
                    trackingRecord.ionizationType
                );
                if (trackingResult.success) {
                    esProcessingResults.trackingRecords++;
                }
            }

        } catch (esError) {
            console.error('Elasticsearch processing error:', esError.message);
        }
        
        res.json({
            success: true,
            message: 'ZIP file extracted and processed successfully to data directory',
            fileCount: fileCount,
            files: files.slice(0, 20), // Limit for frontend display
            elasticsearch: esProcessingResults
        });
        
    } catch (error) {
        console.error('Upload/extraction error:', error);
        
        // Clean up temporary file if it exists
        if (req.file && fs.existsSync(req.file.path)) {
            try {
                fs.unlinkSync(req.file.path);
            } catch (cleanupError) {
                console.error('Cleanup error:', cleanupError);
            }
        }
        
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Start data loading
app.post('/api/load-data', async (req, res) => {
    try {
        const dataDir = path.join(__dirname, 'data');
        
        if (!fs.existsSync(dataDir)) {
            return res.status(400).json({
                success: false,
                error: 'No data folder found. Please upload and extract a ZIP file first.'
            });
        }

        // Check if we have JSON files to process
        function findJsonFiles(dir) {
            const jsonFiles = [];
            const entries = fs.readdirSync(dir, { withFileTypes: true });
            
            for (const entry of entries) {
                const fullPath = path.join(dir, entry.name);
                
                if (entry.isDirectory()) {
                    jsonFiles.push(...findJsonFiles(fullPath));
                } else if (entry.name.toLowerCase().endsWith('.json') || entry.name.toLowerCase().endsWith('.jsonl')) {
                    jsonFiles.push(fullPath);
                }
            }
            
            return jsonFiles;
        }
        
        const jsonFiles = findJsonFiles(dataDir);
        
        if (jsonFiles.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'No JSON files found in the data folder.'
            });
        }

        // Start the data loader container with the local data folder mounted
        const result = await docker.run(
            'dsfp-in-a-box-data-loader',
            ['python', 'load_data.py'],
            null,
            {
                name: 'dsfp-data-loader-run',
                AutoRemove: true,
                Env: [
                    'LOCAL_DATA_PATH=/app/data',
                    'ELASTICSEARCH_URL=http://elasticsearch:9200',
                    'ELASTICSEARCH_INDEX=dsfp-screening-index'
                ],
                HostConfig: {
                    NetworkMode: 'dsfp-in-a-box_default',
                    Binds: [
                        `${dataDir}:/app/data:ro`
                    ]
                }
            }
        );

        res.json({
            success: true,
            message: `Data loading started for ${jsonFiles.length} JSON files`,
            containerId: result[0].id,
            filesFound: jsonFiles.length,
            dataDirectory: dataDir
        });

    } catch (error) {
        console.error('Data loading error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Check data loading status
app.get('/api/load-data/status', async (req, res) => {
    try {
        const containers = await docker.listContainers({ all: true });
        const loaderContainer = containers.find(c => 
            c.Names.some(name => name.includes('dsfp-data-loader-run'))
        );

        if (!loaderContainer) {
            return res.json({
                success: true,
                status: 'not_running',
                message: 'No data loading process found'
            });
        }

        res.json({
            success: true,
            status: loaderContainer.State,
            containerId: loaderContainer.Id,
            created: new Date(loaderContainer.Created * 1000).toISOString()
        });

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get data loading logs
app.get('/api/load-data/logs/:containerId', async (req, res) => {
    try {
        const container = docker.getContainer(req.params.containerId);
        const logs = await container.logs({
            stdout: true,
            stderr: true,
            timestamps: true,
            tail: 100
        });

        res.json({
            success: true,
            logs: logs.toString()
        });

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Helper function to extract uptime
function getUptime(status) {
    const match = status.match(/Up (.+)/);
    return match ? match[1] : null;
}

// Get ZIP file contents for preview
app.get('/api/zip-contents', (req, res) => {
    try {
        const dataDir = path.join(__dirname, 'data');
        
        if (!fs.existsSync(dataDir)) {
            return res.status(404).json({
                success: false,
                error: 'No data folder found. Please upload and extract a ZIP file first.'
            });
        }

        // Recursively read directory contents
        function readDirRecursive(dir, relativePath = '') {
            const items = [];
            const entries = fs.readdirSync(dir, { withFileTypes: true });
            
            for (const entry of entries) {
                const fullPath = path.join(dir, entry.name);
                const relPath = path.join(relativePath, entry.name).replace(/\\/g, '/');
                
                if (entry.isDirectory()) {
                    items.push({
                        name: relPath,
                        isDirectory: true,
                        size: 0
                    });
                    // Recursively read subdirectories (limit depth to avoid performance issues)
                    if (relativePath.split('/').length < 3) {
                        items.push(...readDirRecursive(fullPath, relPath));
                    }
                } else {
                    const stats = fs.statSync(fullPath);
                    items.push({
                        name: relPath,
                        isDirectory: false,
                        size: stats.size,
                        modified: stats.mtime
                    });
                }
            }
            
            return items;
        }
        
        const contents = readDirRecursive(dataDir);
        const totalFiles = contents.filter(item => !item.isDirectory).length;
        const totalDirectories = contents.filter(item => item.isDirectory).length;
        const totalSize = contents.reduce((sum, item) => sum + (item.size || 0), 0);
        
        res.json({
            success: true,
            dataDirectory: dataDir,
            totalFiles: totalFiles,
            totalDirectories: totalDirectories,
            totalSize: totalSize,
            contents: contents
        });
        
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get data folder status for dashboard
app.get('/api/data/status', (req, res) => {
    try {
        const dataDir = path.join(__dirname, 'data');
        const hostDataPath = getHostDataPath();
        
        if (!fs.existsSync(dataDir)) {
            return res.json({
                success: true,
                fileCount: 0,
                totalSize: 0,
                lastModified: null,
                dataPath: hostDataPath  // Return host path instead of container path
            });
        }

        // Count files and calculate total size
        function calculateStats(dir) {
            let fileCount = 0;
            let totalSize = 0;
            let lastModified = null;
            
            const entries = fs.readdirSync(dir, { withFileTypes: true });
            
            for (const entry of entries) {
                const fullPath = path.join(dir, entry.name);
                
                if (entry.isDirectory()) {
                    const subStats = calculateStats(fullPath);
                    fileCount += subStats.fileCount;
                    totalSize += subStats.totalSize;
                    if (subStats.lastModified && (!lastModified || subStats.lastModified > lastModified)) {
                        lastModified = subStats.lastModified;
                    }
                } else if (entry.name !== '.gitignore' && entry.name !== 'README.md') {
                    const stats = fs.statSync(fullPath);
                    fileCount++;
                    totalSize += stats.size;
                    if (!lastModified || stats.mtime > lastModified) {
                        lastModified = stats.mtime;
                    }
                }
            }
            
            return { fileCount, totalSize, lastModified };
        }
        
        const stats = calculateStats(dataDir);
        
        res.json({
            success: true,
            fileCount: stats.fileCount,
            totalSize: stats.totalSize,
            lastModified: stats.lastModified,
            dataPath: hostDataPath  // Return host path instead of container path
        });
        
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get screening status from screening index with tracking status from DuckDB
app.get('/api/screening/status', async (req, res) => {
    try {
        // First, get all documents from the screening index
        const screeningResponse = await axios.post(`${ELASTICSEARCH_URL}/${SCREENING_INDEX}/_search`, {
            size: 5000,
            query: { match_all: {} },
            _source: ['sample_id', 'short_name', 'sample_type', 'instrument_setup_used.ionization_type'],
            sort: [{ sample_id: 'asc' }]
        });
        
        // Get tracking status from DuckDB
        let trackingStatus = {};
        try {
            const { spawn } = require('child_process');
            const pythonScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

try:
    db = TrackingDatabase()
    conn = db.conn
    
    # Get all tracking records
    query = "SELECT sample_id, last_screened FROM screening_tracking"
    results = conn.execute(query).fetchall()
    
    tracking_data = {}
    for row in results:
        sample_id = str(row[0])
        tracking_data[sample_id] = {
            'status': 'tracked',
            'last_processed': row[1] if row[1] else None
        }
    
    db.close()
    print(json.dumps(tracking_data))

except Exception as e:
    print(json.dumps({}))
    import traceback
    traceback.print_exc()
            `;

            const result = await new Promise((resolve, reject) => {
                const python = spawn('python3', ['-c', pythonScript]);
                let output = '';
                let errorOutput = '';

                python.stdout.on('data', (data) => {
                    output += data.toString();
                });

                python.stderr.on('data', (data) => {
                    errorOutput += data.toString();
                });

                python.on('close', (code) => {
                    try {
                        const trackingData = JSON.parse(output.trim());
                        resolve(trackingData);
                    } catch (parseError) {
                        resolve({});
                    }
                });
            });

            trackingStatus = result;
        } catch (trackingError) {
            console.warn('Could not fetch tracking status from DuckDB:', trackingError.message);
        }
        
        // Combine screening data with tracking status
        const documents = screeningResponse.data.hits.hits.map(hit => {
            const sampleId = hit._source.sample_id;
            const tracking = trackingStatus[sampleId] || { status: 'unknown', last_processed: null };
            
            return {
                sample_id: sampleId,
                short_name: hit._source.short_name,
                sample_type: hit._source.sample_type,
                ionization_type: hit._source.instrument_setup_used?.ionization_type,
                status: tracking.status,
                last_processed: tracking.last_processed
            };
        });
        
        res.json({
            success: true,
            total: screeningResponse.data.hits.total.value,
            files: documents
        });
        
    } catch (error) {
        console.error('Error fetching screening status:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Update screening status for a sample in DuckDB
app.post('/api/screening/update-status', async (req, res) => {
    try {
        const { sample_id, status } = req.body;
        
        if (!sample_id || !status) {
            return res.status(400).json({
                success: false,
                error: 'Sample ID and status are required'
            });
        }
        
        if (!['screened', 'unprocessed'].includes(status)) {
            return res.status(400).json({
                success: false,
                error: 'Status must be either "screened" or "unprocessed"'
            });
        }
        
        try {
            const { spawn } = require('child_process');
            const pythonScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

try:
    db = TrackingDatabase()
    conn = db.conn
    
    # Update tracking record
    conn.execute("""
        UPDATE screening_tracking 
        SET last_screened = CURRENT_TIMESTAMP
        WHERE sample_id = ?
    """, ["${sample_id}"])
    
    conn.commit()
    db.close()
    
    print("SUCCESS: Status updated in DuckDB")
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    sys.exit(1)
            `;

            await new Promise((resolve, reject) => {
                const python = spawn('python3', ['-c', pythonScript]);
                let output = '';
                let errorOutput = '';

                python.stdout.on('data', (data) => {
                    output += data.toString();
                });

                python.stderr.on('data', (data) => {
                    errorOutput += data.toString();
                });

                python.on('close', (code) => {
                    if (code === 0 && output.includes('SUCCESS:')) {
                        resolve({ success: true });
                    } else {
                        reject(new Error(`Status update failed: ${errorOutput || output}`));
                    }
                });
            });

            res.json({
                success: true,
                message: 'Status updated successfully in DuckDB'
            });
        } catch (duckdbError) {
            console.error('DuckDB status update error:', duckdbError.message);
            res.status(500).json({
                success: false,
                error: 'Failed to update status in DuckDB: ' + duckdbError.message
            });
        }
        
    } catch (error) {
        console.error('Error updating screening status:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get files in screening index
app.get('/api/screening/files', async (req, res) => {
    try {
        // Get unique sample_ids from the screening index (these represent individual files)
        const response = await axios.post(`${ELASTICSEARCH_URL}/${SCREENING_INDEX}/_search`, {
            size: 0,
            aggs: {
                unique_files: {
                    terms: {
                        field: "sample_id",
                        size: 10000
                    }
                }
            }
        });
        
        const files = response.data.aggregations.unique_files.buckets.map(bucket => ({
            sample_id: bucket.key,
            doc_count: bucket.doc_count
        }));
        
        res.json({
            success: true,
            files: files,
            fileCount: files.length,
            totalDocuments: response.data.hits.total.value
        });
        
    } catch (error) {
        console.error('Error fetching screening files:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            files: []
        });
    }
});

// Clear all screening data from DuckDB database
app.post('/api/screening/clear-database', async (req, res) => {
    try {
        console.log('🗑️ Clearing screening database...');

        const { spawn } = require('child_process');
        const pythonScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import time

try:
    # Add a small delay to reduce lock conflicts
    time.sleep(0.5)
    
    db = TrackingDatabase()
    conn = db.conn
    
    # Clear all screening data
    conn.execute("DELETE FROM screening_results")
    conn.execute("DELETE FROM screening_tracking")
    conn.commit()
    
    # Verify clearing
    results_count = conn.execute("SELECT COUNT(*) FROM screening_results").fetchone()[0]
    tracking_count = conn.execute("SELECT COUNT(*) FROM screening_tracking").fetchone()[0]
    
    db.close()
    
    print(f"SUCCESS: Database cleared. Results: {results_count}, Tracking: {tracking_count}")
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
        `;

        const result = await new Promise((resolve, reject) => {
            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';
            let errorOutput = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                errorOutput += data.toString();
            });

            python.on('close', (code) => {
                if (code === 0 && output.includes('SUCCESS:')) {
                    resolve({ success: true, output });
                } else {
                    reject(new Error(`Python script failed with code ${code}: ${errorOutput || output}`));
                }
            });

            python.on('error', (err) => {
                reject(new Error(`Failed to start Python process: ${err.message}`));
            });
        });

        res.json({
            success: true,
            message: 'Screening database cleared successfully',
            details: result.output.trim()
        });

    } catch (error) {
        console.error('Error clearing database:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get DuckDB schema for debugging
app.get('/api/tracking/schema', async (req, res) => {
    try {
        const { spawn } = require('child_process');
        const pythonScript = `import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

try:
    db = TrackingDatabase()
    conn = db.conn
    
    # Get table information
    tables_query = "SHOW TABLES"
    tables = conn.execute(tables_query).fetchall()
    
    schema_info = {}
    
    for table in tables:
        table_name = table[0]
        # Get column information for each table
        columns_query = f"DESCRIBE {table_name}"
        columns = conn.execute(columns_query).fetchall()
        
        schema_info[table_name] = {
            'columns': [{'name': col[0], 'type': col[1]} for col in columns]
        }
        
        # Count rows
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        count = conn.execute(count_query).fetchone()[0]
        schema_info[table_name]['row_count'] = count;
        
        # Get sample data (first 5 rows)
        try {
            sample_query = f"SELECT * FROM {table_name} LIMIT 5"
            sample_data = conn.execute(sample_query).fetchall();
            schema_info[table_name]['sample_data'] = [list(row) for row in sample_data];
        } except {
            pass
        }
    }
    
    db.close();
    
    print(json.dumps(schema_info, indent=2, default=str))

except Exception as e:
    print(json.dumps({'error': str(e)}, default=str))
    import traceback
    traceback.print_exc()
`;

        const result = await new Promise((resolve, reject) => {
            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';
            let errorOutput = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                errorOutput += data.toString();
            });

            python.on('close', (code) => {
                try {
                    const schemaData = JSON.parse(output.trim());
                    resolve(schemaData);
                } catch (parseError) {
                    reject(new Error(`Failed to parse output: ${output}\nError: ${errorOutput}`));
                }
            });
        });

        res.json({
            success: true,
            schema: result
        });

    } catch (error) {
        console.error('Error fetching schema:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Get detailed file tracking information from DuckDB
app.get('/api/tracking/files', async (req, res) => {
    console.log('=== API /api/tracking/files called ===');
    try {
        let files = [];
        
        // Get total substances count from compounds index
        let totalSubstances = 0;
        try {
            console.log('Fetching compounds count from Elasticsearch...');
            const compoundsCountResponse = await axios.get('http://elasticsearch:9200/dsfp-compounds-1/_count');
            totalSubstances = compoundsCountResponse.data.count || 95139;
            console.log(`Retrieved ${totalSubstances} compounds from index`);
        } catch (compoundsError) {
            console.error('Failed to get compounds count:', compoundsError.message);
            totalSubstances = 95139; // Fallback count
        }

        // Get screening data from DuckDB (without short_name, will fetch from ES)
        try {
            console.log('Querying DuckDB for screening and tracking data...');
            const { spawn } = require('child_process');
            const pythonScript = `import sys
import warnings
warnings.filterwarnings("ignore")
sys.path.append('/app/setup')

import logging
# Redirect all logging to stderr to keep stdout clean for JSON
logging.basicConfig(level=logging.INFO, stream=sys.stderr)

from tracking_db import TrackingDatabase
import json
from datetime import datetime

try:
    db = TrackingDatabase()
    conn = db.conn
    
    # Get ALL samples with short_name from tracking table
    files_data = []
    
    # Get all tracked samples (now includes short_name)
    tracking_query = """
    SELECT st.sample_id, st.short_name, st.collection_id, st.created_at
    FROM screening_tracking st
    ORDER BY st.sample_id
    """
    
    tracking_results = conn.execute(tracking_query).fetchall()
    
    # Get screening results if any exist
    screening_query = """
    SELECT sr.sample_id,
           COUNT(DISTINCT sr.substance_name) as substances_screened,
           COUNT(*) as total_results,
           COUNT(CASE WHEN sr.is_detected THEN 1 END) as detected,
           MAX(sr.created_at) as last_screened
    FROM screening_results sr
    GROUP BY sr.sample_id
    """

    screening_results = conn.execute(screening_query).fetchall()
    
    # Create lookup for screening results
    screening_lookup = {}
    for row in screening_results:
        sample_id = str(row[0])
        screening_lookup[sample_id] = {
            'substances_screened': int(row[1]) if row[1] is not None else 0,
            'total_results': int(row[2]) if row[2] is not None else 0,
            'substances_detected': int(row[3]) if row[3] is not None else 0,
            'last_screened': str(row[4]) if row[4] is not None else None
        }
    
    # Combine ALL samples with their screening status
    for row in tracking_results:
        sample_id = str(row[0])
        short_name = str(row[1]) if row[1] is not None else f'Sample_{sample_id}'
        collection_id = str(row[2]) if row[2] is not None else 'Unknown'
        created_at = str(row[3]) if row[3] is not None else None
        
        if sample_id in screening_lookup:
            # Sample has been screened
            screening_info = screening_lookup[sample_id]
            files_data.append({
                'sample_id': sample_id,
                'short_name': short_name,
                'collection_id': collection_id,
                'substances_screened': screening_info['substances_screened'],
                'total_results': screening_info['total_results'],
                'substances_detected': screening_info['substances_detected'],
                'last_screened': screening_info['last_screened'],
                'total_substances': ${totalSubstances}
            })
        else:
            # Sample exists but not screened yet
            files_data.append({
                'sample_id': sample_id,
                'short_name': short_name,
                'collection_id': collection_id,
                'substances_screened': 0,
                'total_results': 0,
                'substances_detected': 0,
                'last_screened': created_at,
                'total_substances': ${totalSubstances}
            })
    
    db.close()
    
    # Output only clean JSON, no other print statements
    print(json.dumps(files_data))

except Exception as e:
    # Output error as JSON
    print(json.dumps([]))`;

            const result = await new Promise((resolve, reject) => {
                const python = spawn('python3', ['-c', pythonScript]);
                let output = '';
                let errorOutput = '';

                python.stdout.on('data', (data) => {
                    output += data.toString();
                });

                python.stderr.on('data', (data) => {
                    errorOutput += data.toString();
                    console.log('DuckDB query stderr:', data.toString());
                });

                python.on('close', (code) => {
                    console.log(`DuckDB query completed with code ${code}`);
                    console.log('Raw output length:', output.length);
                    
                    if (code === 0) {
                        try {
                            // Parse JSON array from stdout
                            const filesData = JSON.parse(output.trim());
                            console.log(`Retrieved ${filesData.length} samples from DuckDB`);
                            resolve(filesData);
                        } catch (parseError) {
                            console.error('Failed to parse DuckDB output:', parseError.message);
                            console.error('Output was:', output.substring(0, 500));
                            resolve([]); // Return empty array on parse error
                        }
                    } else {
                        console.error(`DuckDB query failed with code ${code}`);
                        console.error('Error output:', errorOutput);
                        resolve([]); // Return empty array on error
                    }
                });
            });

            files = result;
            console.log(`Using ${files.length} files from DuckDB`);
            
            // short_name is now directly in the tracking table, no need to fetch from ES
            
        } catch (duckdbError) {
            console.error('DuckDB screening query failed:', duckdbError.message);
            
            // Fallback: Get samples from Elasticsearch screening index
            try {
                console.log('Falling back to Elasticsearch...');
                const esUrl = 'http://elasticsearch:9200';
                const response = await axios.post(`${esUrl}/dsfp-screening-index/_search`, {
                    size: 10000,
                    query: { match_all: {} },
                    _source: ['sample_id', 'short_name'],
                    sort: [{ sample_id: 'asc' }],
                    collapse: { field: 'sample_id' }
                }, { timeout: 10000 });
                
                if (response.status === 200 && response.data.hits && response.data.hits.hits) {
                    files = response.data.hits.hits.map(hit => ({
                        sample_id: String(hit._source.sample_id),
                        collection_id: 'Unknown',
                        short_name: hit._source.short_name || 'Unknown',
                        substances_screened: 0,
                        substances_detected: 0,
                        total_substances: totalSubstances,
                        last_screened: null
                    }));
                    console.log(`Retrieved ${files.length} samples from Elasticsearch fallback`);
                }
            } catch (esError) {
                console.error('Elasticsearch fallback failed:', esError.message);
                files = [];
            }
        }

        // Sort by sample_id
        files.sort((a, b) => {
            const aId = parseInt(a.sample_id) || 0;
            const bId = parseInt(b.sample_id) || 0;
            return aId - bId;
        });

        const result = {
            success: true,
            files: files,
            total: files.length,
            method: "DuckDB + Elasticsearch"
        };
        
        res.json(result);
        
    } catch (error) {
        console.error('Error in tracking files endpoint:', error.message);
        res.status(500).json({
            success: false,
            error: error.message,
            files: []
        });
    }
});

// Get data folder listing
app.get('/api/data/list', (req, res) => {
    try {
        const dataDir = path.join(__dirname, 'data');
        
        if (!fs.existsSync(dataDir)) {
            return res.status(404).json({
                success: false,
                error: 'No data folder found. Please upload and extract a ZIP file first.'
            });
        }

        // Recursively read directory contents
        function readDirRecursive(dir, relativePath = '') {
            const items = [];
            const entries = fs.readdirSync(dir, { withFileTypes: true });
            
            for (const entry of entries) {
                if (entry.name === '.gitignore' || entry.name === 'README.md') continue;
                
                const fullPath = path.join(dir, entry.name);
                const relPath = path.join(relativePath, entry.name).replace(/\\/g, '/');
                
                if (entry.isDirectory()) {
                    items.push({
                        name: relPath,
                        isDirectory: true,
                        size: 0
                    });
                    // Recursively read subdirectories (limit depth to avoid performance issues)
                    if (relativePath.split('/').length < 3) {
                        items.push(...readDirRecursive(fullPath, relPath));
                    }
                } else {
                    const stats = fs.statSync(fullPath);
                    items.push({
                        name: relPath,
                        isDirectory: false,
                        size: stats.size,
                        modified: stats.mtime
                    });
                }
            }
            
            return items;
        }
        
        const contents = readDirRecursive(dataDir);
        const totalFiles = contents.filter(item => !item.isDirectory).length;
        const totalDirectories = contents.filter(item => item.isDirectory).length;
        const totalSize = contents.reduce((sum, item) => sum + (item.size || 0), 0);
        
        res.json({
            success: true,
            dataDirectory: dataDir,
            totalFiles: totalFiles,
            totalDirectories: totalDirectories,
            totalSize: totalSize,
            contents: contents
        });
        
    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Reload compounds data
app.post('/api/reload-compounds', async (req, res) => {
    try {
        // Check if Elasticsearch container is running
        const containers = await docker.listContainers();
        const esContainer = containers.find(c => c.Names[0].replace('/', '') === 'elasticsearch');
        
        if (!esContainer || esContainer.State !== 'running') {
            return res.status(400).json({
                success: false,
                error: 'Elasticsearch container is not running'
            });
        }

        // Start the init-elasticsearch container to reload compounds
        console.log('Starting compounds data reload...');
        
        const result = await docker.run(
            'dsfp-in-a-box-init-elasticsearch',
            ['/bin/sh', '/reload-compounds.sh'],
            null,
            {
                name: `reload-compounds-${Date.now()}`,
                AutoRemove: true,
                Env: [
                    'ELASTICSEARCH_URL=http://elasticsearch:9200'
                ],
                HostConfig: {
                    NetworkMode: 'dsfp-in-a-box_default'
                }
            }
        );

        res.json({
            success: true,
            message: 'Compounds reload started',
            containerId: result[0].id
        });

    } catch (error) {
        console.error('Compounds reload error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Check compounds reload status
app.get('/api/reload-compounds/status', async (req, res) => {
    try {
        const containers = await docker.listContainers({ all: true });
        const reloadContainer = containers.find(c => 
            c.Names.some(name => name.includes('reload-compounds'))
        );

        if (!reloadContainer) {
            return res.json({
                success: true,
                status: 'not_running',
                message: 'No compounds reload process found'
            });
        }

        res.json({
            success: true,
            status: reloadContainer.State,
            containerId: reloadContainer.Id,
            created: new Date(reloadContainer.Created * 1000).toISOString()
        });

    } catch (error) {
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Delete all compounds data
app.delete('/api/compounds', async (req, res) => {
    try {
        const response = await axios.delete(`${ELASTICSEARCH_URL}/dsfp-compounds-1`);
        
        res.json({
            success: true,
            message: 'Compounds index deleted successfully',
            elasticsearch: response.data
        });

    } catch (error) {
        if (error.response && error.response.status === 404) {
            return res.json({
                success: true,
                message: 'Compounds index was already deleted or does not exist'
            });
        }

        console.error('Delete compounds error:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Serve the main dashboard page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve data directory files (for inspection/download)
app.get('/api/data/*', (req, res) => {
    const requestedPath = req.params[0];
    const dataDir = path.join(__dirname, 'data');
    const fullPath = path.join(dataDir, requestedPath);
    
    // Security check: ensure path is within data directory
    if (!fullPath.startsWith(dataDir)) {
        return res.status(403).json({
            success: false,
            error: 'Access denied'
        });
    }
    
    // Check if file exists
    if (!fs.existsSync(fullPath)) {
        return res.status(404).json({
            success: false,
            error: 'File not found'
        });
    }
    
    // Serve the file
    if (fs.statSync(fullPath).isFile()) {
        res.sendFile(fullPath);
    } else {
        res.status(400).json({
            success: false,
            error: 'Path is not a file'
        });
    }
});

// Note: Tracking index initialization removed - now using DuckDB for tracking

// Sync files API endpoint
let syncInProgress = false;
let syncProgress = { total: 0, processed: 0, current: '', status: 'idle' };

// Function to perform the actual file sync
async function performFileSync() {
    try {
        console.log("=== Starting performFileSync() ===");
        const dataDir = path.join(__dirname, 'data');
        
        if (!fs.existsSync(dataDir)) {
            syncProgress.status = 'error';
            syncProgress.current = 'Data directory not found';
            syncInProgress = false;
            return;
        }

        // Get all JSON files
        function findJsonFiles(dir) {
            const jsonFiles = [];
            const entries = fs.readdirSync(dir, { withFileTypes: true });
            
            for (const entry of entries) {
                if (entry.name === '.gitignore' || entry.name === 'README.md') continue;
                
                const fullPath = path.join(dir, entry.name);
                if (entry.isDirectory()) {
                    jsonFiles.push(...findJsonFiles(fullPath));
                } else if (entry.name.toLowerCase().endsWith('.json')) {
                    jsonFiles.push(fullPath);
                }
            }
            
            return jsonFiles;
        }
        
        const jsonFiles = findJsonFiles(dataDir);
        syncProgress.total = jsonFiles.length;
        syncProgress.processed = 0;
        syncProgress.current = `Found ${jsonFiles.length} JSON files`;
        
        console.log(`Found ${jsonFiles.length} JSON files to process`);

        // Create Elasticsearch screening index if it doesn't exist
        // In Docker: setup directory is mounted at /app/setup (see docker-compose.yml)
        // __dirname is /app/status-dashboard when running in container
        const mappingsPath = '/app/setup/mappings.json';
        console.log(`Reading mappings from: ${mappingsPath}`);
        console.log(`__dirname is: ${__dirname}`);
        
        if (!fs.existsSync(mappingsPath)) {
            // Fallback to relative path for local development
            const fallbackPath = path.join(__dirname, '..', 'setup', 'mappings.json');
            console.log(`Trying fallback path: ${fallbackPath}`);
            
            if (!fs.existsSync(fallbackPath)) {
                throw new Error(`Mappings file not found at: ${mappingsPath} or ${fallbackPath}. Please ensure the setup directory exists.`);
            }
            
            const screeningMappings = JSON.parse(fs.readFileSync(fallbackPath, 'utf8'));
            await createIndexIfNotExists(SCREENING_INDEX, screeningMappings);
        } else {
            const screeningMappings = JSON.parse(fs.readFileSync(mappingsPath, 'utf8'));
            await createIndexIfNotExists(SCREENING_INDEX, screeningMappings);
        }

        // Process each JSON file
        for (const filePath of jsonFiles) {
            try {
                const fileName = path.basename(filePath);
                syncProgress.current = `Processing ${fileName}...`;
                
                const fileContent = fs.readFileSync(filePath, 'utf8');
                const jsonData = JSON.parse(fileContent);

                // Validate JSON format
                if (validateJsonFormat(jsonData)) {
                    // Insert into screening index
                    const insertResult = await bulkInsertToElasticsearch([{
                        _id: `${jsonData.sample_id}`,
                        data: jsonData
                    }], SCREENING_INDEX);

                    // Insert tracking record
                    await insertTrackingRecord(
                        fileName,
                        filePath,
                        jsonData.sample_id,
                        jsonData.short_name,
                        jsonData.sample_type || null,
                        jsonData.instrument_setup_used?.ionization_type || null
                    );
                    
                    syncProgress.processed++;
                    console.log(`Processed ${syncProgress.processed}/${syncProgress.total}: ${fileName}`);
                } else {
                    console.warn(`Invalid JSON format for file: ${fileName}`);
                }
            } catch (fileError) {
                console.error(`Error processing file ${path.basename(filePath)}:`, fileError.message);
            }
        }

        syncProgress.status = 'completed';
        syncProgress.current = 'All files processed successfully';
        syncInProgress = false;
        
        console.log("=== performFileSync() completed successfully ===");
        
    } catch (error) {
        console.error('=== performFileSync() error:', error);
        syncProgress.status = 'error';
        syncProgress.current = `Error: ${error.message}`;
        syncInProgress = false;
    }
}

app.post('/api/sync-files', async (req, res) => {
    if (syncInProgress) {
        return res.json({
            success: false,
            error: 'Sync operation already in progress'
        });
    }
    
    console.log("=== /api/sync-files endpoint called ===");
    syncInProgress = true;
    syncProgress = { total: 0, processed: 0, current: 'Starting sync...', status: 'running' };
    
    res.json({
        success: true,
        message: 'Sync operation started'
    });
    
    console.log("=== About to call performFileSync() ===");
    // Start sync process asynchronously
    performFileSync()
        .then(() => console.log("=== performFileSync() completed ==="))
        .catch(err => console.error("=== performFileSync() error:", err));
});

// Get sync progress
app.get('/api/sync-files/progress', (req, res) => {
    res.json({
        success: true,
        inProgress: syncInProgress,
        progress: syncProgress
    });
});

// New endpoint to reset and re-sync all files
app.post('/api/reset-sync', async (req, res) => {
    if (syncInProgress) {
        return res.json({
            success: false,
            error: 'Sync operation already in progress'
        });
    }
    
    try {
        // Clear screening index
        await axios.post(`${ELASTICSEARCH_URL}/${SCREENING_INDEX}/_delete_by_query`, {
            query: { match_all: {} }
        });
        
        // Clear DuckDB tracking data
        console.log('🗑️ Clearing DuckDB tracking data...');
        const { spawn } = require('child_process');
        const clearScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase

try:
    db = TrackingDatabase()
    conn = db.conn
    conn.execute("DELETE FROM screening_results")
    conn.execute("DELETE FROM screening_tracking")
    conn.commit()
    db.close()
    print("SUCCESS: DuckDB cleared")
except Exception as e:
    print(f"ERROR: {str(e)}")
    sys.exit(1)
`;

        await new Promise((resolve, reject) => {
            const python = spawn('python3', ['-c', clearScript]);
            let output = '';
            let errorOutput = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                errorOutput += data.toString();
            });

            python.on('close', (code) => {
                if (code === 0 && output.includes('SUCCESS:')) {
                    resolve({ success: true });
                } else {
                    console.error(`DuckDB clear failed: ${errorOutput || output}`);
                    resolve({ success: false });
                }
            });
        });
        
        res.json({
            success: true,
            message: 'Reset completed - screening index and DuckDB cleared'
        });
        
    } catch (error) {
        console.error('Reset error:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// API endpoint to call the Python screening service
app.post('/api/screen-sample', async (req, res) => {
    try {
        const { 
 
 
            sample_id, 
            substances = [], 
            mz_tolerance = 0.005, 
            rti_tolerance = 20.0, 
            filter_by_blanks = true,
            collection_id,
            index = 'dsfp-test-index-3'
        } = req.body;
        if (!sample_id) {
            return res.status(400).json({ error: 'sample_id is required' });
        }

        if (!substances || substances.length === 0) {
            return res.status(400).json({ error: 'substances array is required and cannot be empty' });
        }

        // Get sample data from screening index (not tracking index)
        const sampleQuery = {
            query: {
                term: { sample_id: sample_id }
            },
            size: 1
        };

        const sampleResponse = await axios.post(`${ELASTICSEARCH_URL}/${SCREENING_INDEX}/_search`, sampleQuery);
        
        if (!sampleResponse.data.hits.hits.length) {
            return res.status(404).json({ error: `Sample ${sample_id} not found in screening index` });
        }

        const sample_data = sampleResponse.data.hits.hits[0]._source;

        // Prepare request for Python screening service
        const screeningServiceUrl = process.env.SCREENING_SERVICE_URL || 'http://dsfp-screening-service:8003';
        const screeningRequest = {
            sample_id: sample_id+'',
            substances: substances,
            mz_tolerance: mz_tolerance,
            rti_tolerance: rti_tolerance,
            filter_by_blanks: filter_by_blanks
        };
        console.log(screeningRequest);

        console.log(`Calling screening service for sample ${sample_id} with ${substances.length} substances`);

        // Call the Python screening service
        const screeningResponse = await axios.post(`${screeningServiceUrl}/screen`, screeningRequest, {
            timeout: 300000 // 5 minutes timeout
        });

        console.log(`Screening completed for sample ${sample_id}`);

        // Save screening results to DuckDB tracking database
        try {
            const { spawn } = require('child_process');
            
            // Merge sample metadata into screening results
            const screeningResults = screeningResponse.data;
            if (screeningResults.results && Array.isArray(screeningResults.results)) {
                screeningResults.results = screeningResults.results.map(result => ({
                    ...result,
                    // Add sample metadata to each result
                    short_name: sample_data.short_name,
                    collection_id: collection_id || sample_data.collection_id,
                    collection_uid: sample_data.collection_uid,
                    collection_title: sample_data.collection_title,
                    matrix_type: sample_data.matrix_type,
                    matrix_type2: sample_data.matrix_type2,
                    sample_type: sample_data.sample_type,
                    monitored_city: sample_data.monitored_city,
                    sampling_date: sample_data.sampling_date,
                    analysis_date: sample_data.analysis_date,
                    latitude: sample_data.latitude,
                    longitude: sample_data.longitude
                }));
            }
            console.log(screeningResults);
            const trackingData = {
                sample_id: sample_id,
                collection_id: collection_id || sample_data.collection_id,
                last_screened: new Date().toISOString(),
                screening_request: {
                    mz_tolerance: mz_tolerance,
                    rti_tolerance: rti_tolerance,
                    filter_by_blanks: filter_by_blanks
                },
                screening_results: screeningResults
            };
            
            const pythonScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

tracking_data = ${JSON.stringify(JSON.stringify(trackingData))}
tracking_obj = json.loads(tracking_data)

try:
    with TrackingDatabase() as db:
        success = db.save_screening_tracking(tracking_obj)
        print(json.dumps({'success': success}))
except Exception as e:
    print(json.dumps({'success': False, 'error': str(e)}))
    import traceback
    traceback.print_exc(file=sys.stderr)
`;

            const saveResult = await new Promise((resolve, reject) => {
                const python = spawn('python3', ['-c', pythonScript]);
                let output = '';
                let errorOutput = '';

                python.stdout.on('data', (data) => {
                    output += data.toString();
                });

                python.stderr.on('data', (data) => {
                    errorOutput += data.toString();
                    console.error('DuckDB save stderr:', data.toString());
                });

                python.on('close', (code) => {
                    if (code === 0) {
                        try {
                            const result = JSON.parse(output.trim());
                            resolve(result);
                        } catch (e) {
                            console.error('Failed to parse save result:', output);
                            resolve({ success: false });
                        }
                    } else {
                        console.error('Failed to save to DuckDB:', errorOutput);
                        resolve({ success: false });
                    }
                });
            });

            console.log(`DuckDB save result for sample ${sample_id}:`, saveResult);
        } catch (saveError) {
            console.error('Error saving to DuckDB:', saveError.message);
            // Don't fail the request if saving fails
        }

        res.json({
            success: true,
            sample_id: sample_id,
            screening_results: screeningResponse.data
        });

    } catch (error) {
        console.error('Error calling screening service:', error.message);
        
        if (error.response) {
            console.log(error.response.data.detail[0].loc)
            // The screening service returned an error
            res.status(error.response.status).json({
                error: 'Screening service error',
                details: error.response.data,
                sample_id: req.body.sample_id
            });
            
        } else if (error.code === 'ECONNREFUSED') {
            res.status(503).json({
                error: 'Screening service unavailable',
                message: 'The screening service is not running or not accessible',
                sample_id: req.body.sample_id
            });
        } else {
            res.status(500).json({
                error: 'Internal server error',
                message: error.message,
                sample_id: req.body.sample_id
            });
        }
    }
});

// API endpoint to get screened substances for a sample
app.post('/api/tracking/screened-substances', async (req, res) => {
    try {
        const { sample_id } = req.body;
        
        if (!sample_id) {
            return res.status(400).json({
                success: false,
                error: 'sample_id is required'
            });
        }

        const { spawn } = require('child_process');
        const pythonScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

try:
    with TrackingDatabase() as db:
        conn = db.conn
        
        # Get all screened substances for this sample
        query = """
        SELECT DISTINCT substance_name 
        FROM screening_results 
        WHERE sample_id = ?
        ORDER BY substance_name
        """
        
        results = conn.execute(query, ["${sample_id}"]).fetchall()
        substances = [row[0] for row in results]
        
        print(json.dumps({
            'success': True,
            'sample_id': "${sample_id}",
            'substances': substances,
            'count': len(substances)
        }))
    
except Exception as e:
    print(json.dumps({
        'success': False,
        'error': str(e)
    }))
    import traceback
    traceback.print_exc()
        `;

        const result = await new Promise((resolve, reject) => {
            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';
            let errorOutput = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                errorOutput += data.toString();
            });

            python.on('close', (code) => {
                if (code === 0) {
                    try {
                        const data = JSON.parse(output.trim());
                        resolve(data);
                    } catch (parseError) {
                        reject(new Error(`Failed to parse response: ${output}`));
                    }
                } else {
                    reject(new Error(`Python script failed: ${errorOutput || output}`));
                }
            });
        });

        res.json(result);

    } catch (error) {
        console.error('Error fetching screened substances:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// API endpoint to get all compounds (substance names)
app.get('/api/compounds/all', async (req, res) => {
    try {
        const allSubstances = [];
        
        // Initialize scroll
        let scrollResponse = await axios.post('http://elasticsearch:9200/dsfp-compounds-1/_search?scroll=1m', {
            size: 10000,
            _source: ['name']
        });

        let scrollId = scrollResponse.data._scroll_id;
        let hits = scrollResponse.data.hits.hits;
        // Add initial batch
        allSubstances.push(...hits.map(hit => hit._source.name));
        
        // Continue scrolling until no more results
        while (hits.length > 0) {
            scrollResponse = await axios.post('http://elasticsearch:9200/_search/scroll', {
                scroll: '1m',
                scroll_id: scrollId
            });
            
            scrollId = scrollResponse.data._scroll_id;
            hits = scrollResponse.data.hits.hits;
            
            if (hits.length > 0) {
                allSubstances.push(...hits.map(hit => hit._source.name));
            }
        }
        
        // Clear scroll context
        try {
            await axios.delete('http://elasticsearch:9200/_search/scroll', {
                data: { scroll_id: scrollId }
            });
        } catch (clearError) {
            console.warn('Failed to clear scroll context:', clearError.message);
        }

        res.json({
            success: true,
            substances: allSubstances,
            count: allSubstances.length
        });

    } catch (error) {
        console.error('Error fetching compounds:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// API endpoint to get detections from screening_results table
app.get('/api/detections', async (req, res) => {
    try {
        const { sample_id, detected } = req.query;
        
        const { spawn } = require('child_process');
        
        // Build the query based on whether we're filtering by sample_id and/or detection status
        const whereClauses = [];
        if (sample_id) {
            whereClauses.push(`sample_id = '${sample_id}'`);
        }
        if (detected === 'true') {
            whereClauses.push('is_detected = TRUE');
        } else if (detected === 'false') {
            whereClauses.push('is_detected = FALSE');
        }
        const whereClause = whereClauses.length ? `WHERE ${whereClauses.join(' AND ')}` : '';
        
        const pythonScript = `
import sys
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

try:
    with TrackingDatabase() as db:
        conn = db.conn
        
        # Get all screening results with optional filter
        query = """
        SELECT 
            sample_id,
            substance_name,
            short_name,
            sample_type,
            ionization,
            analysis_date,
            spectral_similarity_score,
            ip_score,
            rti_score,
            mz_score,
            fragments_score,
            isotopic_fit_score,
            molecular_formula_fit_score,
            concentration,
            semiquant_method,
            mz_tolerance,
            rti_tolerance,
            filter_by_blanks,
            matches,
            is_detected,
            created_at
        FROM screening_results
        ${whereClause}
        ORDER BY is_detected DESC, spectral_similarity_score DESC, sample_id, substance_name
        """
        
        results = conn.execute(query).fetchall()
        
        detections = []
        for row in results:
            detections.append({
                'sample_id': str(row[0]) if row[0] is not None else None,
                'substance_name': str(row[1]) if row[1] is not None else None,
                'short_name': str(row[2]) if row[2] is not None else None,
                'sample_type': str(row[3]) if row[3] is not None else None,
                'ionization': str(row[4]) if row[4] is not None else None,
                'analysis_date': str(row[5]) if row[5] is not None else None,
                'spectral_similarity_score': float(row[6]) if row[6] is not None else None,
                'ip_score': float(row[7]) if row[7] is not None else None,
                'rti_score': float(row[8]) if row[8] is not None else None,
                'mz_score': float(row[9]) if row[9] is not None else None,
                'fragments_score': float(row[10]) if row[10] is not None else None,
                'isotopic_fit_score': float(row[11]) if row[11] is not None else None,
                'molecular_formula_fit_score': float(row[12]) if row[12] is not None else None,
                'concentration': float(row[13]) if row[13] is not None else None,
                'semiquant_method': str(row[14]) if row[14] is not None else None,
                'mz_tolerance': float(row[15]) if row[15] is not None else None,
                'rti_tolerance': float(row[16]) if row[16] is not None else None,
                'filter_by_blanks': bool(row[17]) if row[17] is not None else None,
                'matches': json.loads(row[18]) if row[18] is not None else [],
                'is_detected': bool(row[19]) if row[19] is not None else False,
                'created_at': str(row[20]) if row[20] is not None else None
            })
        
        print(json.dumps({
            'success': True,
            'detections': detections,
            'count': len(detections)
        }))
    
except Exception as e:
    print(json.dumps({
        'success': False,
        'error': str(e)
    }))
    import traceback
    traceback.print_exc()
        `;

        const result = await new Promise((resolve, reject) => {
            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';
            let errorOutput = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                errorOutput += data.toString();
            });

            python.on('close', (code) => {
                if (code === 0) {
                    try {
                        const data = JSON.parse(output.trim());
                        resolve(data);
                    } catch (parseError) {
                        reject(new Error(`Failed to parse response: ${output}`));
                    }
                } else {
                    reject(new Error(`Python script failed: ${errorOutput || output}`));
                }
            });
        });

        res.json(result);

    } catch (error) {
        console.error('Error fetching detections:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Clear detections endpoint
app.post('/api/detections/clear', async (req, res) => {
    try {
        const { sample_ids } = req.body;
        
        if (!sample_ids || !Array.isArray(sample_ids) || sample_ids.length === 0) {
            return res.status(400).json({
                success: false,
                error: 'Invalid request: sample_ids array is required'
            });
        }

        const { spawn } = require('child_process');
        
        const pythonScript = `
import sys
import warnings
warnings.filterwarnings("ignore")
sys.path.append('/app/setup')
from tracking_db import TrackingDatabase
import json

sample_ids = ${JSON.stringify(sample_ids)}

try:
    with TrackingDatabase() as db:
        conn = db.conn
        
        # Build WHERE clause for multiple sample IDs
        placeholders = ','.join(['?' for _ in sample_ids])
        
        # Get count before deletion
        count_query = f"SELECT COUNT(*) FROM screening_results WHERE sample_id IN ({placeholders})"
        count_before = conn.execute(count_query, sample_ids).fetchone()[0]
        
        # Delete from screening_results
        delete_query = f"DELETE FROM screening_results WHERE sample_id IN ({placeholders})"
        conn.execute(delete_query, sample_ids)
        
        # Update tracking table - set last_screened to NULL or keep it
        # (We'll keep tracking but clear the results)
        
        # Commit changes
        conn.commit()
        
        print(json.dumps({
            'success': True,
            'deleted_count': count_before,
            'message': f'Deleted {count_before} detection(s) for {len(sample_ids)} sample(s)'
        }), flush=True)
    
except Exception as e:
    print(json.dumps({
        'success': False,
        'error': str(e)
    }), flush=True)
    import traceback
    import sys
    traceback.print_exc(file=sys.stderr)
        `;

        const result = await new Promise((resolve, reject) => {
            const python = spawn('python3', ['-c', pythonScript]);
            let output = '';
            let errorOutput = '';

            python.stdout.on('data', (data) => {
                output += data.toString();
            });

            python.stderr.on('data', (data) => {
                errorOutput += data.toString();
            });

            python.on('close', (code) => {
                if (code === 0) {
                    try {
                        const data = JSON.parse(output.trim());
                        resolve(data);
                    } catch (parseError) {
                        reject(new Error(`Failed to parse response: ${output}`));
                    }
                } else {
                    reject(new Error(`Python script failed: ${errorOutput || output}`));
                }
            });
        });

        res.json(result);

    } catch (error) {
        console.error('Error clearing detections:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// ============================================================================
// Venthic Webform API proxy (bulk sample import to the DSFP live site)
// ----------------------------------------------------------------------------
// These endpoints act as a thin server-side proxy so the browser never has to
// deal with CORS or hold the live-site credentials. Authentication is handled
// by a dashboard-wide OAuth session (see /api/dsfp/* below) which obtains a
// Bearer token from the DSFP /oauth/token endpoint and refreshes it on demand.
// ============================================================================

// In-memory multer instance for forwarding uploaded sample files to the live API.
const webformUpload = multer({
    storage: multer.memoryStorage(),
    limits: {
        fileSize: 5 * 1024 * 1024 * 1024, // 5GB per file
        files: 50
    }
});

// ---------------------------------------------------------------------------
// DSFP OAuth session (single in-memory session, single-user "in a box" model)
// ---------------------------------------------------------------------------
// Pre-configured defaults can be provided via environment variables. Only the
// presence of a default client_id / client_secret is exposed to the browser,
// never their values.
// Fixed DSFP site configuration — override via env vars only if needed.
const DSFP_BASE_URL = process.env.DSFP_BASE_URL || 'https://dsfp.norman-data.eu';
const DSFP_CLIENT_ID = process.env.DSFP_CLIENT_ID || 'C0dYu44lrBHNjzyKJMldO8liw-ZUyzunReGL-Rrku0g';
const DSFP_CLIENT_SECRET = process.env.DSFP_CLIENT_SECRET || 'dsfp_box';

// dsfpSession: null when logged out, otherwise:
//   { baseUrl, username, password, clientId, clientSecret, token, expiresAt }
// password / client_secret are kept in memory only so the token can be silently
// refreshed when it expires (password grant has no refresh token).
let dsfpSession = null;

// Normalise a base URL (strip trailing slashes).
function normalizeBaseUrl(baseUrl) {
    if (!baseUrl || typeof baseUrl !== 'string') return '';
    return baseUrl.trim().replace(/\/+$/, '');
}

// Obtain a Bearer token from /oauth/token using the password grant.
async function obtainDsfpToken({ baseUrl, username, password, clientId, clientSecret }) {
    const url = `${baseUrl}/oauth/token`;
    const body = new URLSearchParams({
        grant_type: 'password',
        username,
        password,
        client_id: clientId,
        client_secret: clientSecret
    });
    const response = await axios.post(url, body.toString(), {
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'User-Agent': 'DSFP-Dashboard/1.0'
        },
        validateStatus: () => true,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        timeout: 30000
    });

    if (response.status !== 200 || !response.data || !response.data.access_token) {
        const message = (response.data && (response.data.message || response.data.error_description ||
            response.data.error)) || `HTTP ${response.status}`;
        const err = new Error(message);
        err.status = response.status;
        err.body = response.data;
        throw err;
    }

    const expiresInMs = (Number(response.data.expires_in) || 3600) * 1000;
    // Apply a 60s safety buffer so we refresh just before the upstream considers
    // the token expired.
    return {
        token: response.data.access_token,
        expiresAt: Date.now() + expiresInMs - 60_000
    };
}

// Obtain a Drupal session cookie + CSRF token via /user/login?_format=json.
// Required for routes where the _auth option excludes OAuth2 (e.g. webform submit).
async function obtainDsfpSessionCookie(username, password) {
    const loginUrl = `${DSFP_BASE_URL}/user/login?_format=json`;
    const loginResp = await axios.post(loginUrl, { name: username, pass: password }, {
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'DSFP-Dashboard-Importer/1.0'
        },
        validateStatus: () => true,
        maxRedirects: 0,
        timeout: 30000
    });
    if (loginResp.status < 200 || loginResp.status >= 400) {
        throw new Error(`Drupal session login failed: HTTP ${loginResp.status}`);
    }
    const setCookie = loginResp.headers['set-cookie'] || [];
    const cookies = setCookie.map(c => c.split(';')[0]).filter(Boolean).join('; ');
    let csrfToken = (loginResp.data && loginResp.data.csrf_token) || '';
    if (!csrfToken) {
        const tokenResp = await axios.get(`${DSFP_BASE_URL}/session/token`, {
            headers: { 'Cookie': cookies, 'User-Agent': 'DSFP-Dashboard-Importer/1.0' },
            validateStatus: () => true, timeout: 15000
        });
        csrfToken = tokenResp.status === 200 ? String(tokenResp.data || '').trim() : '';
    }
    return { cookies, csrfToken };
}

// Return { cookies, csrfToken } for the current session, refreshing if absent.
async function getValidDsfpSession() {
    if (!dsfpSession) {
        const err = new Error('Not logged in to DSFP'); err.status = 401; throw err;
    }
    if (!dsfpSession.sessionCookies) {
        const s = await obtainDsfpSessionCookie(dsfpSession.username, dsfpSession.password);
        dsfpSession.sessionCookies = s.cookies;
        dsfpSession.csrfToken = s.csrfToken;
    }
    return { cookies: dsfpSession.sessionCookies, csrfToken: dsfpSession.csrfToken };
}

// Return a non-expired Bearer token for the current session, refreshing it
// transparently via the password grant when needed.
async function getValidDsfpToken() {
    if (!dsfpSession) {
        const err = new Error('Not logged in to DSFP');
        err.status = 401;
        throw err;
    }
    if (Date.now() >= dsfpSession.expiresAt) {
        const fresh = await obtainDsfpToken(dsfpSession);
        dsfpSession.token = fresh.token;
        dsfpSession.expiresAt = fresh.expiresAt;
    }
    return dsfpSession.token;
}

// POST /api/dsfp/login — exchange username+password for a Bearer token.
// The DSFP base URL and OAuth client credentials are hardcoded server-side;
// callers only need to supply the user's own username and password.
app.post('/api/dsfp/login', async (req, res) => {
    try {
        const body = req.body || {};
        const username = (body.username || '').trim();
        const password = body.password || '';

        if (!username || !password) {
            return res.status(400).json({
                success: false,
                error: 'username and password are required'
            });
        }

        const { token, expiresAt } = await obtainDsfpToken({
            baseUrl: DSFP_BASE_URL,
            username,
            password,
            clientId: DSFP_CLIENT_ID,
            clientSecret: DSFP_CLIENT_SECRET
        });

        // Fetch uid + roles so the UI knows whether the user is an admin.
        let uid = null;
        let isAdmin = false;
        try {
            const meResp = await fetch(
                `${DSFP_BASE_URL}/jsonapi/user/user?filter[name][value]=${encodeURIComponent(username)}&include=roles&page[limit]=1`,
                {
                    headers: {
                        'Authorization': `Bearer ${token}`,
                        'Accept': 'application/vnd.api+json',
                        'User-Agent': 'DSFP-Dashboard/1.0'
                    }
                }
            );
            if (meResp.ok) {
                const meData = await meResp.json();
                const meUser = Array.isArray(meData.data) ? meData.data[0] : meData.data;
                uid = meUser?.attributes?.drupal_internal__uid ?? null;
                const includedRoles = Array.isArray(meData.included) ? meData.included : [];
                isAdmin = includedRoles.some(role =>
                    role?.type?.includes('user_role') && (
                        role?.attributes?.drupal_internal__id === 'administrator' ||
                        role?.attributes?.label === 'administrator'
                    )
                );
                if (!isAdmin) {
                    const roleRefs = meUser?.relationships?.roles?.data || [];
                    isAdmin = roleRefs.some(r =>
                        r?.id === 'administrator' ||
                        r?.meta?.drupal_internal__target_id === 'administrator'
                    );
                }
            }
        } catch (e) {
            console.warn('[dsfp-login] could not fetch user info:', e.message);
        }

        dsfpSession = {
            baseUrl: DSFP_BASE_URL,
            username,
            password,
            clientId: DSFP_CLIENT_ID,
            clientSecret: DSFP_CLIENT_SECRET,
            token,
            expiresAt,
            uid,
            isAdmin,
            // Session cookie for routes that disallow Bearer auth.
            sessionCookies: null,
            csrfToken: null
        };

        // Obtain a session cookie in parallel — /api/webform/submit only
        // accepts Drupal cookie auth, not OAuth Bearer.
        obtainDsfpSessionCookie(username, password).then(({ cookies, csrfToken }) => {
            if (dsfpSession) {
                dsfpSession.sessionCookies = cookies;
                dsfpSession.csrfToken = csrfToken;
            }
        }).catch(e => console.warn('[dsfp-login] session cookie failed:', e.message));

        return res.json({ success: true, username, baseUrl: DSFP_BASE_URL, expiresAt });
    } catch (error) {
        console.error('DSFP login failed:', error.message);
        return res.status(error.status && error.status < 500 ? error.status : 502).json({
            success: false,
            error: error.message,
            body: error.body
        });
    }
});

// POST /api/dsfp/logout — discard the in-memory session.
app.post('/api/dsfp/logout', (req, res) => {
    dsfpSession = null;
    res.json({ success: true });
});

// GET /api/dsfp/status — minimal session info for the UI.
app.get('/api/dsfp/status', (req, res) => {
    res.json({
        loggedIn: !!dsfpSession,
        username: dsfpSession ? dsfpSession.username : null,
        baseUrl: DSFP_BASE_URL,
        expiresAt: dsfpSession ? dsfpSession.expiresAt : null,
        uid: dsfpSession ? (dsfpSession.uid ?? null) : null,
        isAdmin: dsfpSession ? (dsfpSession.isAdmin || false) : false
    });
});

// GET /api/dsfp/users?q=<query> — search DSFP users by name (admin only).
app.get('/api/dsfp/users', async (req, res) => {
    if (!dsfpSession) return res.status(401).json({ error: 'Not logged in' });
    if (!dsfpSession.isAdmin) return res.status(403).json({ error: 'Admin only' });
    const q = (req.query.q || '').trim();
    if (q.length < 2) return res.json([]);
    try {
        const token = await getValidDsfpToken();
        const url = `${DSFP_BASE_URL}/jsonapi/user/user` +
            `?filter[name][operator]=CONTAINS&filter[name][value]=${encodeURIComponent(q)}` +
            `&page[limit]=15&sort=name`;
        const r = await fetch(url, {
            headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/vnd.api+json' }
        });
        const data = await r.json();
        const users = (data.data || []).map(u => ({
            uid:  u.attributes?.drupal_internal__uid,
            uuid: u.id,
            name: u.attributes?.name
        }));
        return res.json(users);
    } catch (e) {
        console.error('[api/dsfp/users] error:', e.message);
        return res.status(502).json({ error: e.message });
    }
});

// Helper: convert an axios/fetch upstream 401 into a session-invalidating
// JSON response so the browser can prompt for re-login.
function unauthorizedResponse(res, message) {
    dsfpSession = null;
    return res.status(401).json({
        success: false,
        loggedIn: false,
        error: message || 'Not logged in to DSFP'
    });
}

// ---------------------------------------------------------------------------
// Webform schema helpers
// ---------------------------------------------------------------------------

// Parse Drupal webform elements YAML into our {matrixOptions,envOptions,fields}
// structure without requiring an external YAML parser. The format is predictable
// enough to handle with line-by-line analysis.
function parseWebformElementsYaml(yaml) {
    // Element types that are structural containers — we recurse into them
    // and inherit their visibility conditions downward to child fields.
    const CONTAINER_TYPES = new Set([
        'webform_wizard_page', 'fieldset', 'webform_section', 'container',
        'details', 'webform_flexbox', 'webform_flexbox_item',
        'webform_table', 'webform_table_row', 'webform_table_sort',
        'webform_actions', 'webform_custom_composite'
    ]);
    // Types that produce no user-facing field column.
    const SKIP_TYPES = new Set([
        'webform_markup', 'processed_text', 'webform_message',
        'webform_horizontal_rule', 'label', 'hidden', 'value',
        'managed_file', 'webform_document_file', 'webform_image_file',
        'webform_audio_file', 'webform_video_file', 'webform_computed_twig'
    ]);

    // ---- Pass 1: parse the YAML into a nested JS object ------------------
    // Lightweight parser that handles Drupal webform element YAML:
    // quoted/unquoted scalars, nested mappings of arbitrary depth, blank lines.
    function stripQ(s) { return (s || '').trim().replace(/^['"]|['"]$/g, '').trim(); }

    // Locate the key:value separator on a YAML line. Naive indexOf(':') breaks
    // when the key is a quoted string that itself contains colons — e.g. the
    // webform #states selector ':input[name="matrix"]': has a colon at offset 1.
    // Walk past the quoted region first, then find the next ':'.
    function findKeyColon(s) {
        const q = s[0];
        if (q === '"' || q === "'") {
            const close = s.indexOf(q, 1);
            if (close === -1) return s.indexOf(':');
            const c = s.indexOf(':', close + 1);
            return c;
        }
        return s.indexOf(':');
    }

    function parseBlock(lines, baseIndent) {
        const obj = {};
        let i = 0;
        while (i < lines.length) {
            const line = lines[i];
            if (!line.trim()) { i++; continue; }
            const indent = line.search(/\S/);
            if (indent < baseIndent) break;
            if (indent !== baseIndent) { i++; continue; }

            const trimmed = line.trim();
            const colonIdx = findKeyColon(trimmed);
            if (colonIdx === -1) { i++; continue; }

            const key = stripQ(trimmed.slice(0, colonIdx));
            const val = stripQ(trimmed.slice(colonIdx + 1));

            // Collect all child lines (indent strictly > baseIndent).
            const childLines = [];
            let childIndent = -1;
            let j = i + 1;
            while (j < lines.length) {
                const cl = lines[j];
                if (!cl.trim()) { childLines.push(cl); j++; continue; }
                const ci = cl.search(/\S/);
                if (ci <= baseIndent) break;
                if (childIndent === -1) childIndent = ci;
                childLines.push(cl);
                j++;
            }

            if (childIndent > baseIndent) {
                obj[key] = parseBlock(childLines, childIndent);
                i = j;
            } else {
                obj[key] = val;
                i = j > i ? j : i + 1;
            }
        }
        return obj;
    }

    // Find the base indent of the first non-empty line.
    let baseIndent = 0;
    for (const line of yaml.split('\n')) {
        if (line.trim()) { baseIndent = line.search(/\S/); break; }
    }
    const tree = parseBlock(yaml.split('\n'), baseIndent);

    // ---- Pass 2: walk the object tree, collecting fields with conditions --
    const matrixOptions = [];   // [{value, label}]
    const envOptions = [];      // [{value, label}]
    const fields = [];
    let envMatrixKey = null;    // the matrix #options key under which env_monitoring is nested

    // Convert a Drupal #options object to [{value,label}] preserving order.
    function optionPairs(optsObj) {
        if (!optsObj || typeof optsObj !== 'object') return [];
        return Object.entries(optsObj).map(([k, v]) => ({
            value: k,
            label: (typeof v === 'string' && v) ? v : k
        }));
    }

    // Extract the most specific visible-when condition from a #states object.
    // Prefers env_monitoring over matrix (env is the more specific selector).
    //
    // Real selectors in the DSFP webform look like:
    //   ':input[name="matrix[select]"]'              (webform_options_custom:buttons)
    //   ':input[name="env_monitoring[select]"]'      (same widget)
    //   ':input[name="monitoring_scale"]'            (plain widget)
    // We extract the base field name (before any `[…]` suffix) so all of these
    // collapse back to `matrix`, `env_monitoring`, `monitoring_scale`, etc.
    function extractCond(statesObj) {
        if (!statesObj || typeof statesObj !== 'object') return null;
        const visible = statesObj.visible;
        if (!visible || typeof visible !== 'object') return null;

        let matrixCond = null, envCond = null;
        for (const [selector, condition] of Object.entries(visible)) {
            // Allow `[` inside the captured run, then strip any `[…]` suffix.
            const m = selector.match(/name=["']?([A-Za-z0-9_\-\[\]]+)["']?\]/);
            if (!m) continue;
            const fn = m[1].replace(/\[[^\]]*\]$/, '');   // matrix[select] -> matrix
            let v = null;
            if (condition && typeof condition === 'object') {
                v = String(condition.value != null ? condition.value :
                           condition.checked != null ? condition.checked : '');
            } else if (typeof condition === 'string') {
                v = condition;
            }
            if (!v) continue;
            if (fn === 'env_monitoring') envCond = { parent: 'env_monitoring', value: v };
            else if (fn === 'matrix')    matrixCond = { parent: 'matrix', value: v };
        }
        return envCond || matrixCond || null;
    }

    function walk(obj, inheritedCond) {
        for (const [name, value] of Object.entries(obj)) {
            // Skip YAML properties (#type, #title…) and scalar leaves.
            if (name.startsWith('#') || typeof value !== 'object') continue;

            const type  = String(value['#type']  || '');
            const label = String(value['#title'] || name);
            const required = value['#required'] === 'true' || value['#required'] === true;

            // This element's own #states overrides the inherited condition.
            const cond = extractCond(value['#states']) || inheritedCond;

            // Collect matrix / env_monitoring options as we encounter them.
            if (name === 'matrix') {
                optionPairs(value['#options']).forEach(opt => {
                    if (!matrixOptions.find(o => o.value === opt.value)) matrixOptions.push(opt);
                });
                // matrix itself is not a column — keep recursing to pick up siblings.
                continue;
            }
            if (name === 'env_monitoring') {
                optionPairs(value['#options']).forEach(opt => {
                    if (!envOptions.find(o => o.value === opt.value)) envOptions.push(opt);
                });
                // Record which matrix value reveals env_monitoring — typically env_monitoring
                // is nested inside a section whose #states.visible binds matrix to one value.
                if (cond && cond.parent === 'matrix' && !envMatrixKey) envMatrixKey = cond.value;
                continue;
            }

            // Pure cosmetic / file-upload fields — skip entirely.
            if (SKIP_TYPES.has(type)) continue;

            // Container: recurse, passing the resolved condition downward so that
            // nested fields inherit the section's visibility constraint.
            if (CONTAINER_TYPES.has(type) || type === '') {
                walk(value, cond);
                continue;
            }

            // Leaf input field — extract options if any.
            const options = optionPairs(value['#options']);

            // Determine group from the resolved condition.
            let group = 'common';
            if (cond) {
                group = (cond.parent === 'env_monitoring' ? 'env:' : 'matrix:') + cond.value;
            }

            // Helper: coerce a YAML scalar (string after stripQ) to a number when sensible.
            const num = v => {
                if (v === undefined || v === null || v === '') return undefined;
                const n = Number(v);
                return Number.isFinite(n) ? n : undefined;
            };
            const str = v => (v === undefined || v === null || v === '') ? undefined : String(v);

            // Entity-reference / taxonomy autocomplete metadata. Drupal stores the
            // target entity type at #target_type and the allowed bundles at
            // #selection_settings.target_bundles (an object whose KEYS are bundle
            // machine names — we treat the keys as the bundle list).
            let targetType = null, targetBundles = null;
            if (type === 'entity_autocomplete' || type === 'webform_entity_select' ||
                type === 'webform_entity_checkboxes' || type === 'webform_entity_radios' ||
                type === 'webform_term_select' || type === 'webform_term_checkboxes') {
                targetType = str(value['#target_type']) || null;
                const sel = value['#selection_settings'];
                if (sel && typeof sel === 'object') {
                    const tb = sel.target_bundles;
                    if (tb && typeof tb === 'object') {
                        targetBundles = Object.keys(tb).filter(k => k && !k.startsWith('#'));
                    } else if (typeof tb === 'string' && tb) {
                        targetBundles = [tb];
                    }
                }
                // webform_term_* shortcuts: vocabulary is given by #vocabulary.
                if (!targetType && (type === 'webform_term_select' || type === 'webform_term_checkboxes')) {
                    targetType = 'taxonomy_term';
                }
                if (!targetBundles && value['#vocabulary']) {
                    targetBundles = [str(value['#vocabulary'])];
                }
            }

            fields.push({
                name, label, required, type,
                options: options.length ? options : null,
                group,
                description: str(value['#description']),
                min:        num(value['#min']),
                max:        num(value['#max']),
                step:       num(value['#step']),
                minlength:  num(value['#minlength']),
                maxlength:  num(value['#maxlength']),
                pattern:        str(value['#pattern']),
                patternError:   str(value['#pattern_error']),
                multiple:   value['#multiple'] === 'true' || value['#multiple'] === true,
                targetType,
                targetBundles,
                conditionParent: cond ? cond.parent : null,
                conditionValue:  cond ? cond.value  : null
            });
        }
    }

    walk(tree, null);

    // Diagnostic: dump field-group breakdown so we can spot parser regressions
    // (e.g. mis-detected #states) from the container logs.
    const byGroup = fields.reduce((m, f) => { m[f.group] = (m[f.group] || 0) + 1; return m; }, {});
    console.log('[webform-schema] groups:', byGroup,
        'matrixOptions:', matrixOptions.map(o => o.value),
        'envOptions:', envOptions.map(o => o.value),
        'envMatrixKey:', envMatrixKey);

    return { matrixOptions, envOptions, envMatrixKey, fields };
}

// Convert a JSON Schema (from webform_jsonschema) into the same {matrixOptions,envOptions,fields} structure.
// webform_jsonschema wraps fields in wizard-page objects and encodes conditions via dependencies+oneOf.
function jsonSchemaToInternalSchema(doc) {
    const root = (doc && doc.schema) || doc || {};
    const out = { matrixOptions: [], envOptions: [], envMatrixKey: null, fields: [] };

    // Labels for the matrix abbreviations used by DSFP/NORMAN flexbox containers.
    const MATRIX_LABELS = {
        gw: 'Groundwater', ww: 'Wastewater', sw: 'Surface water', sl: 'Sludge',
        soil: 'Soil', sed: 'Sediment', biota: 'Biota', ia: 'Indoor air',
        spm: 'Suspended particulate matter', aa: 'Ambient air',
        wa: 'Work atmosphere', ea: 'Exhaled air', food: 'Food',
        bio: 'Human biomonitoring'
    };

    // Convert anyOf / enum / items.anyOf to [{value, label}], or null.
    function toOptions(p) {
        if (!p) return null;
        if (p.anyOf && Array.isArray(p.anyOf)) {
            return p.anyOf
                .filter(o => o && o.enum && o.enum.length)
                .map(o => ({ value: String(o.enum[0]), label: o.title || String(o.enum[0]) }));
        }
        if (p.enum && Array.isArray(p.enum)) {
            return p.enum.map(v => ({ value: String(v), label: String(v) }));
        }
        if (p.items) {
            if (p.items.anyOf && Array.isArray(p.items.anyOf)) {
                return p.items.anyOf
                    .filter(o => o && o.enum && o.enum.length)
                    .map(o => ({ value: String(o.enum[0]), label: o.title || String(o.enum[0]) }));
            }
            if (p.items.enum) {
                return p.items.enum.map(v => ({ value: String(v), label: String(v) }));
            }
        }
        return null;
    }

    // Walk a schema object and push leaf fields into out.fields with the given group.
    function walkFields(schema, group) {
        if (!schema || typeof schema !== 'object') return;
        const props = schema.properties || {};
        const requiredHere = Array.isArray(schema.required) ? schema.required : [];

        Object.entries(props).forEach(([name, p]) => {
            if (!p || typeof p !== 'object') return;
            // Container: has nested properties but is not an array type — recurse.
            if (p.properties && p.type !== 'array') { walkFields(p, group); return; }

            out.fields.push({
                name,
                label:    p.title || p.label || name,
                required: requiredHere.includes(name),
                type: p.type === 'string' && p.format === 'date' ? 'date'
                    : (p.type === 'number' || p.type === 'integer') ? 'number'
                    : 'textfield',
                options:  toOptions(p),
                group,
                description: p.description,
                min: p.minimum,  max: p.maximum,
                minlength: p.minLength, maxlength: p.maxLength,
                pattern: p.pattern, patternError: undefined, step: undefined,
                multiple: p.type === 'array'
            });
        });

        // Handle dependencies+oneOf if present (legacy/mixed webform structure).
        Object.entries(schema.dependencies || {}).forEach(([trigger, dep]) => {
            ((dep && dep.oneOf) || []).forEach(item => {
                if (!item || !item.properties) return;
                walkFields(item, group);
            });
        });
    }

    const pages = root.properties || {};
    Object.entries(pages).forEach(([pageName, page]) => {
        if (!page || typeof page !== 'object') return;

        if (pageName === 'matrix_page') {
            // matrix_header contains flexbox_X containers — one per matrix type.
            const mh = page.properties && page.properties.matrix_header;
            if (mh && mh.properties) {
                Object.entries(mh.properties).forEach(([fbKey, fb]) => {
                    const matrixKey = fbKey.replace(/^flexbox_/, '');
                    const label = MATRIX_LABELS[matrixKey] || matrixKey;
                    out.matrixOptions.push({ value: matrixKey, label });
                    if (fb.properties) walkFields(fb, 'matrix:' + matrixKey);
                });
            }
        } else {
            // All other pages (metadata, files, …) are common to every matrix.
            if (page.properties) walkFields(page, 'common');
        }
    });

    return out;
}

// Fetch the webform schema via two strategies:
//   1. JSON:API with Bearer token (GET /jsonapi/webform/webform/{id})
//   2. webform_jsonschema with NO Authorization header (route only allows cookie auth —
//      sending ANY credential header triggers Drupal's AccessDeniedException)
app.post('/api/webform/schema', async (req, res) => {
    const webformId = (req.body && req.body.webformId) || 'sample';
    const errors = [];

    // --- Strategy 1: JSON:API with Bearer token ----------------------------
    // JSON:API requires the entity UUID, not the machine name. We query the
    // collection endpoint filtered by drupal_internal__id to find the webform,
    // which also returns all attributes (including elements YAML) in one request.
    if (dsfpSession) {
        try {
            const token = await getValidDsfpToken();
            const url = `${DSFP_BASE_URL}/jsonapi/webform/webform` +
                `?filter[drupal_internal__id]=${encodeURIComponent(webformId)}` +
                `&fields[webform--webform]=drupal_internal__id,elements`;
            const r = await axios.get(url, {
                headers: {
                    'Accept': 'application/vnd.api+json',
                    'Authorization': `Bearer ${token}`,
                    'User-Agent': 'DSFP-Dashboard-Importer/1.0'
                },
                validateStatus: () => true,
                maxContentLength: Infinity,
                maxBodyLength: Infinity
            });

            if (r.status === 401) {
                dsfpSession = null;
                return unauthorizedResponse(res, 'DSFP rejected the access token');
            }

            if (r.status === 200) {
                // Collection response: data is an array.
                const records = r.data && r.data.data;
                const record = Array.isArray(records) ? records[0] : records;
                const attrs = record && record.attributes;
                const elementsYaml = attrs && (attrs.elements || attrs.element);
                if (elementsYaml && typeof elementsYaml === 'string') {
                    const schema = parseWebformElementsYaml(elementsYaml);
                    console.log(`Webform schema via JSON:API: ${schema.fields.length} fields`);
                    return res.json({ success: true, source: 'jsonapi', url, schema });
                }
                if (Array.isArray(records) && records.length === 0) {
                    errors.push(`JSON:API: webform "${webformId}" not found (check machine name)`);
                } else {
                    errors.push(`JSON:API: HTTP ${r.status} — elements field missing or empty`);
                }
            } else {
                errors.push(`JSON:API: HTTP ${r.status}`);
            }
        } catch (e) {
            errors.push(`JSON:API: ${e.message}`);
        }
    } else {
        errors.push('JSON:API: not logged in');
    }

    // --- Strategy 2: webform_jsonschema ---
    // Cookie auth is supported; Bearer tokens are NOT (Drupal throws AccessDeniedException).
    // Send the logged-in user's session cookie when available so Drupal's per-role
    // permission applies, then fall back to anonymous.
    try {
        const url = `${DSFP_BASE_URL}/webform_jsonschema/${webformId}`;
        const reqHeaders = {
            'Accept': 'application/json',
            'User-Agent': 'DSFP-Dashboard-Importer/1.0'
        };
        let authMethod = 'anon';
        if (dsfpSession) {
            try {
                const { cookies } = await getValidDsfpSession();
                reqHeaders['Cookie'] = cookies;
                authMethod = 'session-cookie';
            } catch (e) { /* session cookie unavailable — proceed anonymously */ }
        }
        const r = await axios.get(url, {
            headers: reqHeaders,
            validateStatus: () => true,
            maxContentLength: Infinity,
            maxBodyLength: Infinity
        });

        if (r.status === 200) {
            const schema = jsonSchemaToInternalSchema(r.data);
            console.log(`Webform schema via webform_jsonschema (${authMethod}): ${schema.fields.length} fields, matrixOptions=${schema.matrixOptions.length}, envOptions=${schema.envOptions.length}`);
            return res.json({ success: true, source: 'jsonschema', url, schema });
        }
        errors.push(`webform_jsonschema (${authMethod}): HTTP ${r.status}`);
    } catch (e) {
        errors.push(`webform_jsonschema: ${e.message}`);
    }

    // Both strategies failed — tell the client so it can use its built-in schema.
    console.warn('Could not fetch webform schema:', errors.join(' | '));
    return res.status(200).json({
        success: false,
        error: 'Could not fetch live schema: ' + errors.join(' | ')
    });
});

// ---------------------------------------------------------------------------
// Taxonomy autocomplete proxy
// ---------------------------------------------------------------------------
// Used by webform fields of #type entity_autocomplete / webform_term_select
// whose #target_type is taxonomy_term. Returns up to 25 suggestions filtered
// by name CONTAINS query, restricted to the given vocabularies.
//
//   GET /api/dsfp/taxonomy?vocab=v1,v2&q=foo
//
// Drupal JSON:API exposes terms at /jsonapi/taxonomy_term/{vocabulary}. We
// query each requested vocabulary in parallel and merge the results.
app.get('/api/dsfp/taxonomy', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    const q = (req.query.q || '').toString().trim();
    const vocabsRaw = (req.query.vocab || '').toString().trim();
    if (!vocabsRaw) return res.json({ success: true, terms: [] });
    const vocabs = vocabsRaw.split(',').map(s => s.trim()).filter(Boolean).slice(0, 5);

    try {
        const token = await getValidDsfpToken();
        const perVocab = 25;

        const fetchOne = async (vocab) => {
            // Drupal vocabulary machine names use underscores in JSON:API URLs.
            const safeVocab = encodeURIComponent(vocab.replace(/-/g, '_'));
            const params = new URLSearchParams();
            params.set('fields[taxonomy_term--' + vocab + ']', 'name,drupal_internal__tid');
            params.set('page[limit]', String(perVocab));
            params.set('sort', 'name');
            if (q) {
                params.set('filter[name-filter][condition][path]', 'name');
                params.set('filter[name-filter][condition][operator]', 'CONTAINS');
                params.set('filter[name-filter][condition][value]', q);
            }
            const url = `${DSFP_BASE_URL}/jsonapi/taxonomy_term/${safeVocab}?${params.toString()}`;
            const r = await axios.get(url, {
                headers: {
                    'Accept': 'application/vnd.api+json',
                    'Authorization': `Bearer ${token}`,
                    'User-Agent': 'DSFP-Dashboard-Importer/1.0'
                },
                validateStatus: () => true
            });
            if (r.status === 401) { dsfpSession = null; throw new Error('unauthorized'); }
            if (r.status !== 200) {
                return { vocab, error: `HTTP ${r.status}`, terms: [] };
            }
            const items = (r.data && r.data.data) || [];
            return {
                vocab,
                terms: items.map(t => ({
                    id: t.id,
                    tid: t.attributes && t.attributes.drupal_internal__tid,
                    name: t.attributes && t.attributes.name,
                    vocab
                }))
            };
        };

        const results = await Promise.all(vocabs.map(v => fetchOne(v).catch(e => ({
            vocab: v, error: e.message, terms: []
        }))));

        const merged = [];
        const seen = new Set();
        for (const r of results) {
            for (const t of r.terms) {
                const k = (t.vocab || '') + '::' + (t.name || '');
                if (!seen.has(k)) { seen.add(k); merged.push(t); }
            }
        }
        res.json({
            success: true,
            terms: merged.slice(0, 25),
            vocabs: results.map(r => ({ vocab: r.vocab, count: r.terms.length, error: r.error || null }))
        });
    } catch (e) {
        if (e.message === 'unauthorized') {
            return unauthorizedResponse(res, 'DSFP rejected the access token');
        }
        res.status(500).json({ success: false, error: e.message });
    }
});

// ---------------------------------------------------------------------------
// Collections + instrument setups (for the Sample Import screen)
// ---------------------------------------------------------------------------
// The DSFP "collection" field on the sample webform is an entity_autocomplete
// pointing at nodes of bundle `data` (DKAN datasets). The "instrument_setup"
// field is a view-restricted entity reference whose allowed options are the
// instrument setups attached to the *source* collection — DSFP publishes the
// machine-readable list as `/data/{nid}/instrument-setups.csv`.

// GET /api/dsfp/collections
//   List `data` nodes (collections) authored by the currently logged-in user.
// GET /api/dsfp/collections
//   Returns the high-level DKAN datasets authored by the currently logged-in
//   user. Other `node/data` bundle entries (keywords, themes, distributions
//   etc.) are excluded by intersecting the user's authored data nodes with
//   the canonical dataset list from DKAN metastore.
app.get('/api/dsfp/collections', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    try {
        const token = await getValidDsfpToken();

        // (1) The site's `node/data` records (gives us nid + uuid + title).
        // Show collections for all users (no author filter) and request
        // a large page size since the site is small in practice.
        const jsonApiParams = new URLSearchParams();
        jsonApiParams.set('page[limit]', '100000');
        jsonApiParams.set('sort', '-created');
        const jsonApiUrl = `${DSFP_BASE_URL}/jsonapi/node/data?${jsonApiParams.toString()}`;

        // (2) The DKAN metastore dataset list (canonical "high-level datasets",
        // excludes keyword / theme / distribution entities that share the
        // `data` bundle). This endpoint is public; no Bearer required.
        // Prefer the `/all` metastore endpoint (contains title/uuid/nid),
        // fall back to `/items` if unavailable.
        const dkanUrls = [
            `${DSFP_BASE_URL}/api/1/metastore/schemas/dataset/all`,
            `${DSFP_BASE_URL}/api/1/metastore/schemas/dataset/items`
        ];

        // Try the metastore `/all` endpoint first — it contains title, UUID
        // and internal IDs. If it returns items, use that as the authoritative
        // collections list (faster and avoids heavy JSON:API post-processing).
        const dkanPrimary = await axios.get(dkanUrls[0], { headers: { 'Accept': 'application/json', 'User-Agent': 'DSFP-Dashboard-Importer/1.0' }, validateStatus: () => true, timeout: 30000 }).catch(e => ({ status: 0, _err: e.message }));

        if (dkanPrimary && dkanPrimary.status === 200 && Array.isArray(dkanPrimary.data) && dkanPrimary.data.length > 0) {
            // Map metastore items into the same `collections` shape the UI expects.
            const collections = dkanPrimary.data.map(item => {
                // item may be shaped as { identifier, title, nid } or { data: { identifier, title, nid }}
                const src = (item && item.data) ? item.data : item;
                const uuid = src && (src.identifier || src.id || src.uuid || item.identifier || item.id || item.uuid) || null;
                const nid = src && (src.nid || src.drupal_internal__nid || src.internal_id || src.internalId) || (item && item.nid) || null;
                const title = src && (src.title || src.name || src.label) || item && (item.title || item.name || item.label) || '';
                return {
                    nid: nid ? Number(nid) : null,
                    uuid: uuid || null,
                    title: title || '',
                    status: true,
                    attributes: {
                        drupal_internal__nid: nid ? Number(nid) : undefined,
                        title: title
                    }
                };
            }).filter(c => c.uuid || c.nid);

            return res.json({ success: true, collections });
        }

        // Metastore `/all` was not available — fall back to fetching JSON:API
        const jsonApiRes = await axios.get(jsonApiUrl, {
            headers: {
                'Accept': 'application/vnd.api+json',
                'Authorization': `Bearer ${token}`,
                'User-Agent': 'DSFP-Dashboard-Importer/1.0'
            },
            validateStatus: () => true,
            timeout: 30000
        });

        if (jsonApiRes.status === 401) return unauthorizedResponse(res);
        if (jsonApiRes.status !== 200) {
            return res.status(502).json({
                success: false,
                error: `JSON:API HTTP ${jsonApiRes.status}`,
                body: jsonApiRes.data
            });
        }

        const userItems = (jsonApiRes.data && jsonApiRes.data.data) || [];
        const userDataNodes = userItems
            .map(t => ({
                nid: t.attributes && t.attributes.drupal_internal__nid,
                uuid: t.id,
                title: (t.attributes && t.attributes.title) || '',
                status: t.attributes && t.attributes.status,
                attributes: t.attributes || {}
            }))
            .filter(c => c.nid);

        // Identify which authored nodes are actual datasets (not distributions,
        // keywords, themes, etc). DSFP publishes a `field_data_type` for many
        // nodes; distributions commonly have `distribution`. Also some nodes
        // embed DCAT JSON in `field_json_metadata` which we can inspect.
        const datasetNodes = userDataNodes.filter(c => {
            const a = c.attributes || {};
            if (a.field_data_type && String(a.field_data_type).toLowerCase() === 'dataset') return true;
            if (a.field_json_metadata && typeof a.field_json_metadata === 'string') {
                if (/"@type"\s*:\s*"dcat:Dataset"/i.test(a.field_json_metadata)) return true;
                try {
                    const parsed = JSON.parse(a.field_json_metadata);
                    const dtype = parsed && (parsed['@type'] || parsed.data && parsed.data['@type']);
                    if (dtype && String(dtype).toLowerCase().includes('dataset')) return true;
                } catch (e) {
                    // ignore parse errors
                }
            }
            return false;
        });

        // Non-admin users should only see datasets they authored. Admins
        // continue to use the metastore intersection/fallback behaviour.
        if (!dsfpSession.isAdmin) {
            const onlyAuthoredDatasets = datasetNodes.map(d => ({ nid: d.nid, uuid: d.uuid, title: d.title, status: d.status, attributes: d.attributes }));
            return res.json({ success: true, collections: onlyAuthoredDatasets });
        }

        // Build the set of UUIDs that the metastore considers actual datasets.
        // If the metastore call failed for any reason, fall back to returning
        // all of the user's `node/data` records (degrade gracefully rather
        // than show an empty dropdown).
        let datasetUuids = null;
        let metastoreNote = null;
        if (dkanRes && dkanRes.status === 200 && Array.isArray(dkanRes.data)) {
            datasetUuids = new Set();
            for (const item of dkanRes.data) {
                if (item && item.identifier) datasetUuids.add(String(item.identifier));
                // Some DKAN deployments wrap the POD record under .data:
                if (item && item.data && item.data.identifier) {
                    datasetUuids.add(String(item.data.identifier));
                }
            }
        } else {
            metastoreNote = dkanRes && dkanRes._err
                ? `DKAN metastore unreachable: ${dkanRes._err}`
                : `DKAN metastore returned HTTP ${dkanRes && dkanRes.status}`;
            console.warn('[collections]', metastoreNote);
        }

        // Build an expanded set of metastore identifiers by including
        // distribution identifiers / URLs so we can match nodes that
        // reference datasets indirectly (e.g. via a file URL or UUID
        // stored in a custom field).
        const metastoreIdentifiers = new Set();
        if (datasetUuids) {
            for (const u of datasetUuids) metastoreIdentifiers.add(u);
            for (const item of (dkanRes.data || [])) {
                try {
                    if (item && item.distribution && Array.isArray(item.distribution)) {
                        for (const dist of item.distribution) {
                            if (!dist) continue;
                            if (dist.identifier) metastoreIdentifiers.add(String(dist.identifier));
                            // Common field names for URLs
                            const url = dist.accessURL || dist.downloadURL || dist.url || dist['@id'] || dist['url'];
                            if (url && typeof url === 'string') {
                                try {
                                    const p = new URL(url);
                                    const seg = (p.pathname || '').split('/').filter(Boolean).pop();
                                    if (seg) metastoreIdentifiers.add(seg);
                                } catch (e) {
                                    // not an absolute URL — try to extract token-like parts
                                    const parts = String(url).split(/[\/?#]+/).filter(Boolean);
                                    if (parts.length) metastoreIdentifiers.add(parts.pop());
                                }
                            }
                        }
                    }
                } catch (e) { /* ignore malformed metastore items */ }
            }
        }

        // Helper: extract candidate identifier strings from a node's attributes.
        const extractIdentifiers = (attrs) => {
            const ids = new Set();
            if (!attrs || typeof attrs !== 'object') return ids;
            // Include node UUID
            if (attrs.drupal_internal__nid) ids.add(String(attrs.drupal_internal__nid));
            // Scan attribute values for identifier-like fields
            for (const k of Object.keys(attrs)) {
                const v = attrs[k];
                if (!v) continue;
                if (typeof v === 'string') ids.add(v);
                else if (typeof v === 'number') ids.add(String(v));
                else if (Array.isArray(v)) {
                    for (const el of v) {
                        if (!el) continue;
                        if (typeof el === 'string') ids.add(el);
                        else if (el.value) ids.add(String(el.value));
                        else if (el.identifier) ids.add(String(el.identifier));
                        else if (el.id) ids.add(String(el.id));
                    }
                } else if (typeof v === 'object') {
                    if (v.value) ids.add(String(v.value));
                    if (v.identifier) ids.add(String(v.identifier));
                    if (v.id) ids.add(String(v.id));
                }
            }
            return ids;
        };

        let collections = [];
        if (!datasetUuids) {
            collections = userDataNodes;
        } else {
            // Only consider authored nodes that look like actual datasets
            const authoredDatasetNids = new Set(datasetNodes.map(d => String(d.nid)));

            // First, try to match authored dataset nodes directly (UUIDs or attribute tokens)
            collections = userDataNodes.filter(c => {
                if (!authoredDatasetNids.has(String(c.nid))) return false;
                if (datasetUuids.has(c.uuid)) return true;
                const cand = extractIdentifiers(c.attributes);
                for (const x of cand) if (metastoreIdentifiers.has(x)) return true;
                return false;
            });

            // If none matched and many authored nodes are distributions, try
            // to resolve dataset NIDs referenced by distribution downloadURLs.
            if (collections.length === 0) {
                const datasetNids = new Set();
                for (const c of userDataNodes) {
                    try {
                        const meta = c.attributes && c.attributes.field_json_metadata;
                        if (!meta) continue;
                        let parsed = null;
                        try { parsed = JSON.parse(meta); } catch (e) { parsed = null; }
                        let url = null;
                        if (parsed && parsed.data && (parsed.data.downloadURL || parsed.data.accessURL)) url = parsed.data.downloadURL || parsed.data.accessURL;
                        if (!url && typeof meta === 'string') {
                            // try to find a URL inside the string
                            const m = meta.match(/https?:\/\/[^"'\\s]+/i);
                            if (m) url = m[0];
                        }
                        if (!url) continue;
                        const m2 = url.match(/\/data\/(\d+)\//);
                        if (m2 && m2[1]) datasetNids.add(m2[1]);
                    } catch (e) { /* ignore */ }
                }

                if (datasetNids.size > 0) {
                    // Fetch the dataset nodes by nid and see if their UUIDs match metastore
                    const fetchedDatasets = [];
                    for (const nid of Array.from(datasetNids)) {
                        try {
                            const url = `${DSFP_BASE_URL}/jsonapi/node/data?filter[drupal_internal__nid]=${encodeURIComponent(nid)}&page[limit]=1`;
                            const r = await axios.get(url, {
                                headers: { 'Accept': 'application/vnd.api+json', 'Authorization': `Bearer ${token}` },
                                validateStatus: () => true,
                                timeout: 15000
                            });
                            if (r.status === 200 && r.data && Array.isArray(r.data.data) && r.data.data.length > 0) {
                                const item = r.data.data[0];
                                fetchedDatasets.push({ nid: nid, uuid: item.id, title: item.attributes && item.attributes.title || '', attributes: item.attributes || {} });
                            }
                        } catch (e) { /* ignore per-nid failures */ }
                    }

                    for (const d of fetchedDatasets) {
                        if (datasetUuids.has(d.uuid) || metastoreIdentifiers.has(d.uuid)) {
                            collections.push({ nid: Number(d.nid), uuid: d.uuid, title: d.title, status: true, attributes: d.attributes });
                        }
                    }
                }
            }
        }

        // If no matches found, fall back to returning all authored nodes
        // but include warnings so the UI can explain what's going on.
        const warnings = [];
        if (metastoreNote) warnings.push(metastoreNote);
        if (datasetUuids && collections.length === 0 && userDataNodes.length > 0) {
            warnings.push('No authored data nodes matched the DKAN metastore list; returning all authored data nodes instead.');
            collections = userDataNodes;
        }

        res.json({ success: true, collections, ...(warnings.length ? { warning: warnings.join(' ') } : {}) });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// DEBUG endpoint: return raw upstream JSON:API + metastore responses
// Useful for diagnosing why the collections list is empty. Only available
// when a DSFP session exists.
app.get('/api/dsfp/debug-collections', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    try {
        const token = await getValidDsfpToken();

        const jsonApiParams = new URLSearchParams();
        // Debug endpoint: do not filter by author; fetch a large page.
        jsonApiParams.set('fields[node--data]', 'title,drupal_internal__nid,status,created');
        jsonApiParams.set('page[limit]', '100000');
        jsonApiParams.set('sort', '-created');
        const jsonApiUrl = `${DSFP_BASE_URL}/jsonapi/node/data?${jsonApiParams.toString()}`;
        const dkanUrls = [
            `${DSFP_BASE_URL}/api/1/metastore/schemas/dataset/all`,
            `${DSFP_BASE_URL}/api/1/metastore/schemas/dataset/items`
        ];

        const [jsonApiRes, dkanRes] = await Promise.all([
            axios.get(jsonApiUrl, {
                headers: { 'Accept': 'application/vnd.api+json', 'Authorization': `Bearer ${token}` },
                validateStatus: () => true
            }).catch(e => ({ status: 0, _err: e.message })),
            axios.get(dkanUrls[0], { headers: { 'Accept': 'application/json' }, validateStatus: () => true }).catch(e => ({ status: 0, _err: e.message }))
        ]);

        return res.json({
            success: true,
            jsonApi: { status: jsonApiRes.status, data: jsonApiRes.data },
            metastore: { status: dkanRes.status, data: dkanRes.data }
        });
    } catch (e) {
        return res.status(500).json({ success: false, error: e.message });
    }
});

// GET /api/dsfp/collection-samples?nid=<collection-nid>
// Attempts a best-effort fetch of the collection's sample records by
// trying a couple of known export paths on the DSFP site. Returns an
// array of sample objects when successful, or a helpful message +
// collection URL when not.
app.get('/api/dsfp/collection-samples', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    const nid = (req.query.nid || '').toString().trim();
    if (!/^[0-9]+$/.test(nid)) return res.status(400).json({ success: false, error: 'nid (numeric) is required' });

    const tried = [];
    try {
        const token = await getValidDsfpToken();

        function normalizeSampleRecord(sample) {
            const source = sample && typeof sample === 'object' ? sample : {};
            const pick = (keys) => {
                for (const key of keys) {
                    if (source[key] !== null && source[key] !== undefined && String(source[key]).trim() !== '') {
                        return source[key];
                    }
                }
                return '';
            };

            return {
                ...source,
                sample_id: pick(['sample_id', 'ID', 'Id', 'sampleId']),
                short_name: pick(['short_name', 'short_name_for_contribution', 'Short name for contribution', 'Short name']),
                short_name_for_contribution: pick(['short_name_for_contribution', 'Short name for contribution', 'short_name', 'Short name']),
                sample_type: pick(['sample_type', 'type', 'Sample type', 'Type']),
                type: pick(['type', 'sample_type', 'Sample type', 'Type'])
            };
        }

        // Strategy 1: public JSON export at /data/{nid}/samples.json
        const tryOne = async (url, headers) => {
            tried.push(url);
            try {
                const r = await axios.get(url, {
                    headers: headers || { 'Accept': 'application/json', 'User-Agent': 'DSFP-Dashboard-Importer/1.0' },
                    responseType: 'text',
                    validateStatus: () => true,
                    transformResponse: x => x
                });
                if (r.status === 200) {
                    const ct = (r.headers['content-type'] || '').toLowerCase();
                    // If it's JSON-like, try parse
                    if (ct.includes('application/json') || r.data.trim().startsWith('{') || r.data.trim().startsWith('[')) {
                        try {
                            const json = JSON.parse(r.data);
                            const samples = Array.isArray(json) ? json : (json.samples || json.data || []);
                            return { success: true, source: url, samples: samples.map(normalizeSampleRecord) };
                        } catch (e) {
                            // Not parseable
                            return { success: false, error: 'Received non-JSON response', status: r.status, body: r.data };
                        }
                    }
                    // Not JSON — skip
                    return { success: false, error: 'Non-JSON response', status: r.status };
                }
                return { success: false, status: r.status };
            } catch (e) {
                return { success: false, error: e.message };
            }
        };

        const base = normalizeBaseUrl(DSFP_BASE_URL);

        // Try with bearer token first (some exports require auth)
        const u1 = `${base}/data/${nid}/samples.json`;
        let result = await tryOne(u1, { 'Accept': 'application/json', 'Authorization': `Bearer ${token}`, 'User-Agent': 'DSFP-Dashboard-Importer/1.0' });
        if (result.success) return res.json({ success: true, samples: result.samples, source: result.source });

        // Try anonymously (some endpoints are public)
        result = await tryOne(u1);
        if (result.success) return res.json({ success: true, samples: result.samples, source: result.source });

        // Try alternate path: /data/{nid}/samples
        const u2 = `${base}/data/${nid}/samples`;
        result = await tryOne(u2, { 'Accept': 'application/json', 'Authorization': `Bearer ${token}` });
        if (result.success) return res.json({ success: true, samples: result.samples, source: result.source });
        result = await tryOne(u2);
        if (result.success) return res.json({ success: true, samples: result.samples, source: result.source });

        // If all heuristics failed, return helpful info including the collection page URL
        const collectionUrl = `${base}/data/${nid}`;
        return res.status(404).json({
            success: false,
            error: 'Could not locate a machine-readable samples export for this collection (best-effort tried paths).',
            tried,
            collectionUrl
        });

    } catch (e) {
        if (e.message === 'unauthorized') return unauthorizedResponse(res, 'DSFP rejected the access token');
        console.error('collection-samples error:', e.message);
        return res.status(500).json({ success: false, error: e.message });
    }
});

// Minimal RFC-4180-ish CSV parser. Handles quoted fields with embedded commas,
// newlines and doubled quotes.
function parseCsv(text) {
    const rows = [];
    let cur = '', row = [], inQ = false;
    for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (inQ) {
            if (ch === '"') {
                if (text[i + 1] === '"') { cur += '"'; i++; }
                else inQ = false;
            } else cur += ch;
        } else {
            if (ch === '"' && cur === '') inQ = true;
            else if (ch === ',') { row.push(cur); cur = ''; }
            else if (ch === '\r') { /* swallow */ }
            else if (ch === '\n') { row.push(cur); rows.push(row); row = []; cur = ''; }
            else cur += ch;
        }
    }
    if (cur.length || row.length) { row.push(cur); rows.push(row); }
    return rows.filter(r => r.length > 1 || (r.length === 1 && r[0].trim() !== ''));
}

// GET /api/dsfp/instrument-setups?nid=<collection-nid>
//   Fetches /data/{nid}/instrument-setups.csv and returns the rows.
app.get('/api/dsfp/instrument-setups', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    const nid = (req.query.nid || '').toString().trim();
    if (!/^\d+$/.test(nid)) {
        return res.status(400).json({ success: false, error: 'nid is required (numeric)' });
    }
    try {
        const token = await getValidDsfpToken();
        const url = `${DSFP_BASE_URL}/data/${nid}/instrument-setups.csv`;
        const r = await axios.get(url, {
            headers: {
                'Accept': 'text/csv, */*;q=0.5',
                'Authorization': `Bearer ${token}`,
                'User-Agent': 'DSFP-Dashboard-Importer/1.0'
            },
            validateStatus: () => true,
            responseType: 'text',
            transformResponse: x => x
        });
        if (r.status === 401) return unauthorizedResponse(res);
        if (r.status === 404) {
            return res.json({ success: true, setups: [], header: [] });
        }
        if (r.status !== 200) {
            return res.status(r.status).json({
                success: false, error: `HTTP ${r.status}`
            });
        }
        const text = typeof r.data === 'string' ? r.data : String(r.data || '');
        const rows = parseCsv(text);
        if (rows.length === 0) {
            return res.json({ success: true, setups: [], header: [] });
        }
        const header = rows[0].map(h => h.trim());
        const setups = rows.slice(1).map(row => {
            const obj = {};
            header.forEach((h, i) => { obj[h] = (row[i] || '').trim(); });
            return obj;
        });
        res.json({ success: true, header, setups });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});


// Submit a single sample (with optional files) to the live webform API.
app.post('/api/webform/import', webformUpload.any(), async (req, res) => {
    try {
        if (!dsfpSession) return unauthorizedResponse(res);

        const webformId = req.body.webform_id || 'sample';

        // Field values arrive as a JSON string under "fields".
        let fields = {};
        if (req.body.fields) {
            try {
                fields = JSON.parse(req.body.fields);
            } catch (e) {
                return res.status(400).json({ success: false, error: 'Invalid "fields" JSON' });
            }
        }

        // The author override UUID (admin only — client passes this when the
        // admin has selected a different owner for the submission).
        const authorUuid = (dsfpSession && dsfpSession.isAdmin)
            ? ((req.body.author_uuid || '').trim() || null)
            : null;

        // Build the multipart payload for the Venthic webform API.
        // Text fields MUST be wrapped under values[field_name] per the API spec.
        // File fields must use the raw field name (PHP puts files in $_FILES
        // by the top-level key, not under $_FILES['values']).
        const form = new FormData();
        form.append('webform_id', webformId);
        for (const [key, value] of Object.entries(fields)) {
            if (value === null || value === undefined) continue;
            const str = String(value);
            if (str.trim() === '') continue;
            form.append(`values[${key}]`, str);
        }

        // Attach uploaded files under their raw webform field names.
        for (const file of req.files || []) {
            const blob = new Blob([file.buffer], {
                type: file.mimetype || 'application/octet-stream'
            });
            form.append(file.fieldname, blob, file.originalname);
        }

        const { cookies, csrfToken } = await getValidDsfpSession();
        const url = `${dsfpSession.baseUrl}/api/webform/submit`;
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Cookie': cookies,
                'X-CSRF-Token': csrfToken,
                'User-Agent': 'DSFP-Dashboard-Importer/1.0'
            },
            body: form
        });

        if (response.status === 401 || response.status === 403) {
            // Session may have expired — clear it so it's refreshed next call.
            if (dsfpSession) { dsfpSession.sessionCookies = null; dsfpSession.csrfToken = null; }
            return unauthorizedResponse(res, 'DSFP rejected the session cookie — please sign in again');
        }

        const text = await response.text();
        let body;
        try {
            body = JSON.parse(text);
        } catch (e) {
            body = text;
        }

        const submissionId = body && typeof body === 'object'
            ? (body.submission_id || body.sid)
            : undefined;

        // If the admin specified an owner override, PATCH the submission's uid
        // relationship immediately after creation.
        if (authorUuid && submissionId) {
            try {
                const token = await getValidDsfpToken();
                // 1. Look up the submission UUID by SID.
                const lookupResp = await fetch(
                    `${dsfpSession.baseUrl}/jsonapi/webform_submission/${webformId}` +
                    `?filter[drupal_internal__sid]=${submissionId}`,
                    { headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/vnd.api+json' } }
                );
                const lookupData = await lookupResp.json();
                const submissionUuid = lookupData?.data?.[0]?.id;
                if (submissionUuid) {
                    // 2. PATCH the uid relationship.
                    await fetch(
                        `${dsfpSession.baseUrl}/jsonapi/webform_submission/${webformId}` +
                        `/${submissionUuid}/relationships/uid`,
                        {
                            method: 'PATCH',
                            headers: {
                                'Authorization': `Bearer ${token}`,
                                'Content-Type': 'application/vnd.api+json',
                                'Accept': 'application/vnd.api+json'
                            },
                            body: JSON.stringify({ data: { type: 'user--user', id: authorUuid } })
                        }
                    );
                }
            } catch (e) {
                console.warn('[webform/import] author PATCH failed:', e.message);
            }
        }

        return res.status(200).json({
            success: response.ok,
            status: response.status,
            submission_id: submissionId,
            response: body
        });
    } catch (error) {
        if (error.status === 401) return unauthorizedResponse(res, error.message);
        console.error('Error importing sample to webform API:', error.message);
        return res.status(502).json({ success: false, error: error.message });
    }
});

// ─── Sample processing pipeline (parse → componentize → jsoncreate) ────────
//
// Each step runs as a one-off Docker container (dockerode `docker.run`,
// AutoRemove: true) — the same mechanism already used above for the
// data-loader and reload-compounds containers — instead of a long-running
// REST service, since these R scripts are single-shot batch jobs.
//
// AWS credentials are never stored in this server or committed to the repo.
// For every job we fetch short-lived STS credentials from DSFP on behalf of
// the signed-in user (see getProcessingAwsCredentials). This assumes DSFP
// exposes an authenticated endpoint returning temporary credentials; adjust
// DSFP_STS_CREDENTIALS_PATH via env if the real path differs, and see the
// implementation notes shared separately for what that endpoint needs to
// return.
const STS_CREDENTIALS_PATH = process.env.DSFP_STS_CREDENTIALS_PATH || '/api/aws/sts-credentials';
const DOCKER_NETWORK = process.env.DSFP_DOCKER_NETWORK || 'dsfp-in-a-box_default';
// The pipeline's own S3 artefacts are read back by componentize/jsoncreate via
// plain `load(url(...))` calls with no credentials, so this prefix is
// public-read — status checks below need no AWS credentials at all.
const S3_PUBLIC_INDEX_BASE = process.env.S3_PUBLIC_INDEX_BASE || 'https://files.dsfp.norman-data.eu/index';

const PROCESSING_PIPELINE = ['parse', 'componentize', 'jsoncreate'];
const PROCESSING_WORKFLOW = ['parse', 'componentize', 'jsoncreate', 'prepare'];

// Image names for ephemeral processing containers
const PROCESSING_IMAGES = {
    parse: 'dsfp-in-a-box-models-parse:latest',
    componentize: 'dsfp-in-a-box-models-componentize:latest',
    jsoncreate: 'dsfp-in-a-box-models-jsoncreate:latest'
};

// Default processing settings - concurrency and resource limits per service
const DEFAULT_PROCESSING_SETTINGS = {
    parse: { concurrency: 5, cpus: 1, memoryMB: 2048 },
    componentize: { concurrency: 5, cpus: 1, memoryMB: 2048 },
    jsoncreate: { concurrency: 5, cpus: 1, memoryMB: 1024 },
    screening: { maxConcurrentRequests: 2, substancesBatchSize: 5, requestDelayMs: 100 }
};

const PROCESSING_SETTINGS_FILE = path.join(__dirname, 'data', 'processing-settings.json');

// Load processing settings from disk, or return defaults
function loadProcessingSettings() {
    try {
        if (fs.existsSync(PROCESSING_SETTINGS_FILE)) {
            const data = JSON.parse(fs.readFileSync(PROCESSING_SETTINGS_FILE, 'utf8'));
            // Merge with defaults to ensure all keys exist
            return {
                parse: { ...DEFAULT_PROCESSING_SETTINGS.parse, ...data.parse },
                componentize: { ...DEFAULT_PROCESSING_SETTINGS.componentize, ...data.componentize },
                jsoncreate: { ...DEFAULT_PROCESSING_SETTINGS.jsoncreate, ...data.jsoncreate },
                screening: { ...DEFAULT_PROCESSING_SETTINGS.screening, ...data.screening }
            };
        }
    } catch (err) {
        console.warn('Failed to load processing settings, using defaults:', err.message);
    }
    return { ...DEFAULT_PROCESSING_SETTINGS };
}

// Save processing settings to disk
function saveProcessingSettings(settings) {
    const dir = path.dirname(PROCESSING_SETTINGS_FILE);
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(PROCESSING_SETTINGS_FILE, JSON.stringify(settings, null, 2));
}

// In-memory settings cache
let processingSettings = loadProcessingSettings();

// jobId -> { service, sampleId, collectionId, uid, state, error, startedAt, finishedAt, containerId }
const processingJobs = new Map();

// Cached STS credentials, keyed by the current session's uid.
let cachedAwsCredentials = null;

const PROCESSING_INDEX_DIR = path.join(__dirname, 'data', 'index');

// Maps each pipeline step to the artifact filename it produces locally.
const PROCESSING_ARTIFACT_FILENAMES = {
    parse: 'parse.RData',
    componentize: 'componentize.RData',
    jsoncreate: 'standard.json'
};

// Uploads a single step's local artifact to S3 right after that step
// completes successfully, so results are backed up immediately instead of
// only during the later "Prepare" step. The local copy in /data/index is
// always kept — this is a copy, not a move.
async function uploadStepArtifactToS3(service, sampleId, credentials) {
    const filename = PROCESSING_ARTIFACT_FILENAMES[service];
    if (!filename) return;
    const localPath = path.join(PROCESSING_INDEX_DIR, String(sampleId), `${sampleId}-${filename}`);
    if (!fs.existsSync(localPath)) return;

    const s3Key = `index/${sampleId}/${sampleId}-${filename}`;
    const s3Client = new S3Client({
        region: credentials.region,
        credentials: {
            accessKeyId: credentials.accessKeyId,
            secretAccessKey: credentials.secretAccessKey,
            sessionToken: credentials.sessionToken
        }
    });
    console.log(`[jobs/${service}] Uploading ${localPath} -> s3://norman-data/${s3Key}`);
    const fileContent = fs.readFileSync(localPath);
    const contentType = localPath.endsWith('.json') ? 'application/json' : 'application/octet-stream';
    await s3Client.send(new PutObjectCommand({
        Bucket: 'norman-data',
        Key: s3Key,
        Body: fileContent,
        ContentType: contentType
    }));
    console.log(`[jobs/${service}] Upload complete for sample ${sampleId}`);
}

// Downloads a single step's S3 artifact into the local /data/index folder,
// mirroring uploadStepArtifactToS3's naming conventions. Used by the
// "Synchronize data" feature to pull down artifacts that exist on S3 but
// are missing locally.
async function downloadStepArtifactFromS3(service, sampleId) {
    const filename = PROCESSING_ARTIFACT_FILENAMES[service];
    if (!filename) return false;
    const localDir = path.join(PROCESSING_INDEX_DIR, String(sampleId));
    const localPath = path.join(localDir, `${sampleId}-${filename}`);
    const s3Url = `${S3_PUBLIC_INDEX_BASE}/${sampleId}/${sampleId}-${filename}`;
    const response = await axios.get(s3Url, { responseType: 'arraybuffer', timeout: 120000 });
    fs.mkdirSync(localDir, { recursive: true });
    fs.writeFileSync(localPath, response.data);
    console.log(`[sync/${service}] Downloaded ${s3Url} -> ${localPath}`);
    return true;
}

// Docker Desktop on Windows reports bind-mount sources in its internal
// WSL-style form (e.g. "/run/desktop/mnt/host/c/Users/me/project/data")
// rather than a native Windows path. Convert that form into a real
// "C:\Users\me\project\data" path so it can be opened via a file:// link
// in the browser. Non-matching inputs (e.g. native Linux hosts) are
// returned unchanged.
function dockerDesktopMountToWindowsPath(mountSource) {
    const match = /^\/run\/desktop\/mnt\/host\/([a-zA-Z])\/(.*)$/.exec(mountSource || '');
    if (match) {
        const drive = match[1].toUpperCase();
        const rest = match[2].replace(/\//g, '\\');
        return `${drive}:\\${rest}`;
    }
    return mountSource;
}

// Runs a processing step in an ephemeral container that auto-removes on completion.
// Returns { exitCode, containerId }.
async function runProcessingStepInContainer(service, sampleId, collectionId, credentials) {
    const imageName = PROCESSING_IMAGES[service];
    if (!imageName) {
        throw new Error(`Unknown processing image for service ${service}`);
    }

    const settings = processingSettings[service] || DEFAULT_PROCESSING_SETTINGS[service];
    const hostDataPath = await getProcessingHostDataPath();
    const containerName = `dsfp-job-${service}-${sampleId}-${Date.now()}`;

    // Create the container with resource limits
    const container = await docker.createContainer({
        Image: imageName,
        name: containerName,
        Cmd: ['Rscript', '/app/runtime.R', String(collectionId), String(sampleId)],
        Env: [
            `AWS_ACCESS_KEY_ID=${credentials.accessKeyId}`,
            `AWS_SECRET_ACCESS_KEY=${credentials.secretAccessKey}`,
            ...(credentials.sessionToken ? [`AWS_SESSION_TOKEN=${credentials.sessionToken}`] : []),
            `AWS_DEFAULT_REGION=${credentials.region}`
        ],
        WorkingDir: '/app',
        HostConfig: {
            AutoRemove: true,
            Binds: [`${hostDataPath}:/data`],
            NanoCpus: Math.round(settings.cpus * 1e9),
            Memory: settings.memoryMB * 1024 * 1024
        },
        AttachStdout: true,
        AttachStderr: true
    });

    const containerId = container.id;
    console.log(`[jobs/${service}] Starting ephemeral container ${containerName} (${containerId.slice(0, 12)})`);

    // Attach to get logs before starting
    const stream = await container.attach({ stream: true, stdout: true, stderr: true });
    stream.on('data', chunk => {
        const text = chunk.toString();
        for (const line of text.split(/\r?\n/)) {
            if (line) {
                console.log(`[jobs/${service}] ${line}`);
            }
        }
    });

    await container.start();

    // Wait for the container to finish
    const { StatusCode } = await container.wait();
    console.log(`[jobs/${service}] Container ${containerName} exited with code ${StatusCode}`);

    return { exitCode: StatusCode, containerId };
}

async function getProcessingAwsCredentials() {
    if (!dsfpSession) {
        const err = new Error('Not logged in to DSFP'); err.status = 401; throw err;
    }
    if (cachedAwsCredentials && cachedAwsCredentials.uid === dsfpSession.uid &&
        Date.now() < cachedAwsCredentials.expiresAt - 30_000) {
        return cachedAwsCredentials.credentials;
    }
    const token = await getValidDsfpToken();
    const url = `${DSFP_BASE_URL}${STS_CREDENTIALS_PATH}`;
    const response = await axios.get(url, {
        headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' },
        validateStatus: () => true,
        timeout: 15000
    });
    if (response.status === 401) {
        const err = new Error('DSFP rejected the access token'); err.status = 401; throw err;
    }
    if (response.status !== 200 || !response.data || !response.data.accessKeyId) {
        const err = new Error(
            `Could not obtain AWS credentials from DSFP (HTTP ${response.status}). Expected ` +
            `${STS_CREDENTIALS_PATH} to return { accessKeyId, secretAccessKey, sessionToken, region, expiresAt }.`
        );
        err.status = 502;
        throw err;
    }
    const credentials = {
        accessKeyId: response.data.accessKeyId,
        secretAccessKey: response.data.secretAccessKey,
        sessionToken: response.data.sessionToken,
        region: response.data.region || 'eu-central-1'
    };
    const expiresAt = response.data.expiresAt ? new Date(response.data.expiresAt).getTime() : (Date.now() + 15 * 60 * 1000);
    cachedAwsCredentials = { uid: dsfpSession.uid, credentials, expiresAt };
    return credentials;
}

// Resolve the Drupal author (internal uid) of a collection node so we can
// check "does the current user manage this collection?". Admins bypass
// this check entirely (see canUserProcessCollection).
async function getCollectionOwnerUid(nid) {
    const token = await getValidDsfpToken();
    const url = `${DSFP_BASE_URL}/jsonapi/node/data?filter[drupal_internal__nid]=${encodeURIComponent(nid)}` +
        `&include=uid&page[limit]=1`;
    const response = await axios.get(url, {
        headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/vnd.api+json' },
        validateStatus: () => true,
        timeout: 20000
    });
    if (response.status === 401) { const err = new Error('unauthorized'); err.status = 401; throw err; }
    if (response.status !== 200) return null;
    const node = response.data && response.data.data && response.data.data[0];
    if (!node) return null;
    const authorRef = node.relationships && node.relationships.uid && node.relationships.uid.data;
    if (!authorRef) return null;
    const included = response.data.included || [];
    const authorEntity = included.find(e => e.id === authorRef.id);
    return (authorEntity && authorEntity.attributes && authorEntity.attributes.drupal_internal__uid) || null;
}

async function canUserProcessCollection(nid) {
    if (!dsfpSession) return false;
    if (dsfpSession.isAdmin) return true;
    const ownerUid = await getCollectionOwnerUid(nid);
    return ownerUid !== null && Number(ownerUid) === Number(dsfpSession.uid);
}

// GET /api/dsfp/collection/:nid/can-process
app.get('/api/dsfp/collection/:nid/can-process', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    const nid = (req.params.nid || '').trim();
    if (!/^[0-9]+$/.test(nid)) return res.status(400).json({ success: false, error: 'nid (numeric) is required' });
    try {
        const canProcess = await canUserProcessCollection(nid);
        res.json({ success: true, canProcess });
    } catch (e) {
        if (e.status === 401) return unauthorizedResponse(res, e.message);
        res.status(500).json({ success: false, error: e.message });
    }
});

// GET /api/dsfp/sample/:sampleId/processing-status
app.get('/api/dsfp/sample/:sampleId/processing-status', async (req, res) => {
    const sampleId = (req.params.sampleId || '').trim();
    if (!/^[0-9]+$/.test(sampleId)) return res.status(400).json({ success: false, error: 'sampleId (numeric) is required' });
    try {
        // Local paths for artifacts (written by R containers)
        const localDir = path.join(PROCESSING_INDEX_DIR, sampleId);
        const localArtifacts = {
            parse: path.join(localDir, `${sampleId}-parse.RData`),
            componentize: path.join(localDir, `${sampleId}-componentize.RData`),
            jsoncreate: path.join(localDir, `${sampleId}-standard.json`)
        };
        // S3 URLs for fallback (legacy samples)
        const s3Artifacts = {
            parse: `${S3_PUBLIC_INDEX_BASE}/${sampleId}/${sampleId}-parse.RData`,
            componentize: `${S3_PUBLIC_INDEX_BASE}/${sampleId}/${sampleId}-componentize.RData`,
            jsoncreate: `${S3_PUBLIC_INDEX_BASE}/${sampleId}/${sampleId}-standard.json`
        };
        
        // Check each artifact: local first, then S3 fallback
        const checks = await Promise.all(Object.entries(localArtifacts).map(async ([step, localPath]) => {
            // Check local file first
            if (fs.existsSync(localPath)) {
                return [step, true];
            }
            // Fall back to S3 HEAD request for legacy samples
            try {
                const r = await axios.head(s3Artifacts[step], { validateStatus: () => true, timeout: 10000 });
                return [step, r.status === 200];
            } catch (e) {
                return [step, false];
            }
        }));
        const done = Object.fromEntries(checks);
        done.prepare = done.jsoncreate ? await isSamplePrepared(sampleId) : false;
        let status = 0;
        if (done.parse) status = 1;
        if (done.componentize) status = 2;
        if (done.jsoncreate) status = 3;
        if (done.prepare) status = 4;
        const nextService = PROCESSING_WORKFLOW[status] || null; // null once status === 4 (complete)
        // downloadUrl points to local JSON if it exists, else S3
        const downloadUrl = done.jsoncreate
            ? (fs.existsSync(localArtifacts.jsoncreate) ? `/api/dsfp/sample/${sampleId}/artifact/standard.json` : s3Artifacts.jsoncreate)
            : null;
        res.json({ success: true, status, done, nextService, downloadUrl });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// GET /api/dsfp/sample/:sampleId/artifact/:filename
// Serves local artifacts (parse.RData, componentize.RData, standard.json).
app.get('/api/dsfp/sample/:sampleId/artifact/:filename', (req, res) => {
    const sampleId = (req.params.sampleId || '').trim();
    const filename = (req.params.filename || '').trim();
    if (!/^[0-9]+$/.test(sampleId)) {
        return res.status(400).json({ success: false, error: 'sampleId (numeric) is required' });
    }
    const allowedFiles = ['parse.RData', 'componentize.RData', 'standard.json'];
    if (!allowedFiles.includes(filename)) {
        return res.status(400).json({ success: false, error: `filename must be one of: ${allowedFiles.join(', ')}` });
    }
    const localPath = path.join(PROCESSING_INDEX_DIR, sampleId, `${sampleId}-${filename}`);
    if (!fs.existsSync(localPath)) {
        return res.status(404).json({ success: false, error: 'Artifact not found locally' });
    }
    const contentType = filename.endsWith('.json') ? 'application/json' : 'application/octet-stream';
    res.setHeader('Content-Type', contentType);
    res.sendFile(localPath);
});

// GET /api/dsfp/sample/:sampleId/local-folder
// Reports whether any processing artifact exists locally for this sample
// and, if so, the host filesystem path to its /data/index/{sampleId}
// folder (so the front-end can offer a "View files" link that opens it
// via file:// in the browser). Stays hidden (exists: false) otherwise.
app.get('/api/dsfp/sample/:sampleId/local-folder', async (req, res) => {
    const sampleId = (req.params.sampleId || '').trim();
    if (!/^[0-9]+$/.test(sampleId)) {
        return res.status(400).json({ success: false, error: 'sampleId (numeric) is required' });
    }
    try {
        const localDir = path.join(PROCESSING_INDEX_DIR, sampleId);
        const exists = Object.values(PROCESSING_ARTIFACT_FILENAMES).some(filename =>
            fs.existsSync(path.join(localDir, `${sampleId}-${filename}`))
        );
        if (!exists) {
            return res.json({ success: true, exists: false });
        }
        const mountSource = await getProcessingHostDataPath();
        const windowsDataPath = dockerDesktopMountToWindowsPath(mountSource);
        const hostPath = `${String(windowsDataPath).replace(/[\\/]+$/, '')}\\index\\${sampleId}`;
        res.json({ success: true, exists: true, hostPath });
    } catch (e) {
        res.status(500).json({ success: false, error: e.message });
    }
});

// POST /api/dsfp/sample/:sampleId/synchronize  { collection_id }
// Reconciles local vs. S3 artifact availability for one sample: downloads
// any artifact present on S3 but missing locally, and uploads any artifact
// present locally but missing on S3. Used by the "Synchronize data" bulk
// action (called once per sample in the selected collection).
app.post('/api/dsfp/sample/:sampleId/synchronize', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    const sampleId = (req.params.sampleId || '').trim();
    const collectionId = String((req.body && req.body.collection_id) || '').trim();
    if (!/^[0-9]+$/.test(sampleId) || !/^[0-9]+$/.test(collectionId)) {
        return res.status(400).json({ success: false, error: 'sampleId and collection_id (numeric) are required' });
    }

    try {
        const allowed = await canUserProcessCollection(collectionId);
        if (!allowed) {
            return res.status(403).json({ success: false, error: 'You do not manage this collection.' });
        }

        const credentials = await getProcessingAwsCredentials();
        const localDir = path.join(PROCESSING_INDEX_DIR, sampleId);
        const actions = {};

        for (const service of Object.keys(PROCESSING_ARTIFACT_FILENAMES)) {
            const filename = PROCESSING_ARTIFACT_FILENAMES[service];
            const localPath = path.join(localDir, `${sampleId}-${filename}`);
            const localExists = fs.existsSync(localPath);
            const s3Url = `${S3_PUBLIC_INDEX_BASE}/${sampleId}/${sampleId}-${filename}`;

            let remoteExists = false;
            try {
                const head = await axios.head(s3Url, { validateStatus: () => true, timeout: 10000 });
                remoteExists = head.status === 200;
            } catch (e) {
                remoteExists = false;
            }

            try {
                if (!localExists && remoteExists) {
                    await downloadStepArtifactFromS3(service, sampleId);
                    actions[service] = 'downloaded';
                } else if (localExists && !remoteExists) {
                    await uploadStepArtifactToS3(service, sampleId, credentials);
                    actions[service] = 'uploaded';
                } else {
                    actions[service] = 'noop';
                }
            } catch (stepErr) {
                console.error(`[sync/${service}] Failed for sample ${sampleId}:`, stepErr.message);
                actions[service] = 'error';
            }
        }

        res.json({ success: true, actions });
    } catch (e) {
        if (e.status === 401) return unauthorizedResponse(res, e.message);
        console.error('[sample/synchronize] error:', e.message);
        res.status(e.status || 500).json({ success: false, error: e.message });
    }
});

// POST /api/dsfp/sample/:sampleId/prepare  { collection_id }
// Indexes the generated standard.json for one sample into the screening index.
// Also uploads local artifacts to S3 if they exist.
app.post('/api/dsfp/sample/:sampleId/prepare', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    const sampleId = (req.params.sampleId || '').trim();
    const collectionId = String((req.body && req.body.collection_id) || '').trim();
    if (!/^[0-9]+$/.test(sampleId) || !/^[0-9]+$/.test(collectionId)) {
        return res.status(400).json({ success: false, error: 'sampleId and collection_id (numeric) are required' });
    }

    try {
        const allowed = await canUserProcessCollection(collectionId);
        if (!allowed) {
            return res.status(403).json({ success: false, error: 'You do not manage this collection.' });
        }

        // Local paths for artifacts
        const localDir = path.join(PROCESSING_INDEX_DIR, sampleId);
        const localArtifacts = {
            parse: { local: path.join(localDir, `${sampleId}-parse.RData`), s3Key: `index/${sampleId}/${sampleId}-parse.RData` },
            componentize: { local: path.join(localDir, `${sampleId}-componentize.RData`), s3Key: `index/${sampleId}/${sampleId}-componentize.RData` },
            jsoncreate: { local: path.join(localDir, `${sampleId}-standard.json`), s3Key: `index/${sampleId}/${sampleId}-standard.json` }
        };
        const localJsonPath = localArtifacts.jsoncreate.local;
        const hasLocalJson = fs.existsSync(localJsonPath);

        // Upload local artifacts to S3 if they exist
        const hasAnyLocalFiles = Object.values(localArtifacts).some(a => fs.existsSync(a.local));
        if (hasAnyLocalFiles) {
            console.log(`[prepare] Uploading local artifacts for sample ${sampleId} to S3...`);
            const credentials = await getProcessingAwsCredentials();
            const s3Client = new S3Client({
                region: credentials.region,
                credentials: {
                    accessKeyId: credentials.accessKeyId,
                    secretAccessKey: credentials.secretAccessKey,
                    sessionToken: credentials.sessionToken
                }
            });
            
            for (const [step, artifact] of Object.entries(localArtifacts)) {
                if (fs.existsSync(artifact.local)) {
                    console.log(`[prepare] Uploading ${step}: ${artifact.local} -> s3://norman-data/${artifact.s3Key}`);
                    const fileContent = fs.readFileSync(artifact.local);
                    const contentType = artifact.local.endsWith('.json') ? 'application/json' : 'application/octet-stream';
                    await s3Client.send(new PutObjectCommand({
                        Bucket: 'norman-data',
                        Key: artifact.s3Key,
                        Body: fileContent,
                        ContentType: contentType
                    }));
                }
            }
            console.log(`[prepare] Upload complete for sample ${sampleId}`);
        }

        // Read JSON data: local first, then S3 fallback
        let jsonData;
        const jsonUrl = `${S3_PUBLIC_INDEX_BASE}/${sampleId}/${sampleId}-standard.json`;
        if (hasLocalJson) {
            console.log(`[prepare] Reading JSON from local: ${localJsonPath}`);
            const rawJson = fs.readFileSync(localJsonPath, 'utf8');
            jsonData = JSON.parse(rawJson);
        } else {
            console.log(`[prepare] Reading JSON from S3: ${jsonUrl}`);
            const response = await axios.get(jsonUrl, { timeout: 20000 });
            jsonData = response.data;
        }

        if (!validateJsonFormat(jsonData)) {
            return res.status(400).json({ success: false, error: 'Generated JSON is missing required screening fields' });
        }

        await ensureScreeningIndex();

        const insertResult = await bulkInsertToElasticsearch([{
            _id: `${jsonData.sample_id}`,
            data: jsonData
        }], SCREENING_INDEX);

        if (!insertResult.success) {
            return res.status(500).json({ success: false, error: insertResult.error || 'Could not index sample for screening' });
        }

        await insertTrackingRecord(
            `${sampleId}-standard.json`,
            jsonUrl,
            jsonData.sample_id,
            jsonData.short_name,
            jsonData.sample_type || null,
            jsonData.instrument_setup_used?.ionization_type || null,
            'prepared'
        );

        res.json({ success: true, prepared: true, downloadUrl: jsonUrl });
    } catch (e) {
        if (e.status === 401) return unauthorizedResponse(res, e.message);
        console.error('[sample/prepare] error:', e.message);
        res.status(e.status || 500).json({ success: false, error: e.message });
    }
});

// POST /api/jobs/enqueue  { service, collection_id, sample_id }
// Launches the requested pipeline step as an ephemeral Docker container and
// returns immediately with a jobId; poll GET /api/jobs/:jobId/status for
// completion.
app.post('/api/jobs/enqueue', async (req, res) => {
    if (!dsfpSession) return unauthorizedResponse(res);
    const { service, collection_id, sample_id } = req.body || {};
    if (!PROCESSING_PIPELINE.includes(service)) {
        return res.status(400).json({ success: false, error: `service must be one of: ${PROCESSING_PIPELINE.join(', ')}` });
    }
    const collectionId = String(collection_id || '').trim();
    const sampleId = String(sample_id || '').trim();
    if (!/^[0-9]+$/.test(collectionId) || !/^[0-9]+$/.test(sampleId)) {
        return res.status(400).json({ success: false, error: 'collection_id and sample_id (numeric) are required' });
    }

    try {
        const allowed = await canUserProcessCollection(collectionId);
        if (!allowed) {
            return res.status(403).json({ success: false, error: 'You do not manage this collection.' });
        }

        // Count active jobs for this service
        const settings = processingSettings[service] || DEFAULT_PROCESSING_SETTINGS[service];
        const activeJobCount = Array.from(processingJobs.values()).filter(
            job => job.service === service && job.state === 'processing'
        ).length;
        if (activeJobCount >= settings.concurrency) {
            return res.status(409).json({
                success: false,
                error: `Service ${service} has reached its concurrency limit (${settings.concurrency} active jobs)`
            });
        }

        const credentials = await getProcessingAwsCredentials();

        const jobId = `${service}-${sampleId}-${Date.now()}`;
        processingJobs.set(jobId, {
            service, sampleId, collectionId,
            uid: dsfpSession.uid,
            state: 'processing',
            error: null,
            startedAt: Date.now(),
            finishedAt: null,
            containerId: null
        });

        runProcessingStepInContainer(service, sampleId, collectionId, credentials).then(async ({ exitCode, containerId }) => {
            const job = processingJobs.get(jobId);
            if (!job) return;
            job.finishedAt = Date.now();
            job.containerId = containerId;
            if (exitCode === 0) {
                job.state = 'completed';
                try {
                    await uploadStepArtifactToS3(service, sampleId, credentials);
                } catch (uploadErr) {
                    // The step itself succeeded and the local artifact is intact;
                    // only the S3 backup failed, so don't fail the job for it.
                    console.error(`[jobs/${service}] S3 upload failed for sample ${sampleId}:`, uploadErr.message);
                }
            } else {
                job.state = 'failed';
                job.error = `Container exited with status ${exitCode}`;
            }
        }).catch(err => {
            const job = processingJobs.get(jobId);
            if (!job) return;
            job.state = 'failed';
            job.error = err.message;
            job.finishedAt = Date.now();
        });

        res.json({ success: true, jobId });
    } catch (e) {
        if (e.status === 401) return unauthorizedResponse(res, e.message);
        console.error('[jobs/enqueue] error:', e.message);
        res.status(e.status || 500).json({ success: false, error: e.message });
    }
});

// GET /api/jobs/:jobId/status
app.get('/api/jobs/:jobId/status', (req, res) => {
    const job = processingJobs.get(req.params.jobId);
    if (!job) return res.status(404).json({ success: false, error: 'Unknown jobId' });
    res.json({ success: true, ...job });
});

// GET /api/processing-settings
// Returns current processing settings (concurrency, CPU, memory limits per service)
app.get('/api/processing-settings', (req, res) => {
    res.json({ success: true, settings: processingSettings });
});

// PUT /api/processing-settings
// Updates processing settings. Expects { parse: {...}, componentize: {...}, jsoncreate: {...}, screening: {...} }
app.put('/api/processing-settings', (req, res) => {
    const newSettings = req.body || {};
    
    // Validate and merge with defaults for processing pipeline services
    const validated = {};
    for (const service of PROCESSING_PIPELINE) {
        const incoming = newSettings[service] || {};
        const defaults = DEFAULT_PROCESSING_SETTINGS[service];
        validated[service] = {
            concurrency: Math.max(1, Math.min(10, parseInt(incoming.concurrency) || defaults.concurrency)),
            cpus: Math.max(0.5, Math.min(4, parseFloat(incoming.cpus) || defaults.cpus)),
            memoryMB: Math.max(512, Math.min(8192, parseInt(incoming.memoryMB) || defaults.memoryMB))
        };
    }
    
    // Validate screening settings separately (different fields)
    const screeningIncoming = newSettings.screening || {};
    const screeningDefaults = DEFAULT_PROCESSING_SETTINGS.screening;
    validated.screening = {
        maxConcurrentRequests: Math.max(1, Math.min(10, parseInt(screeningIncoming.maxConcurrentRequests) || screeningDefaults.maxConcurrentRequests)),
        substancesBatchSize: Math.max(1, Math.min(50, parseInt(screeningIncoming.substancesBatchSize) || screeningDefaults.substancesBatchSize)),
        requestDelayMs: Math.max(0, Math.min(5000, parseInt(screeningIncoming.requestDelayMs) || screeningDefaults.requestDelayMs))
    };
    
    processingSettings = validated;
    try {
        saveProcessingSettings(validated);
        console.log('[processing-settings] Saved:', validated);
        res.json({ success: true, settings: validated });
    } catch (err) {
        console.error('[processing-settings] Failed to save:', err.message);
        res.status(500).json({ success: false, error: 'Failed to persist settings' });
    }
});

app.listen(PORT, async () => {
    console.log(`DSFP Status Dashboard running on http://localhost:${PORT}`);
    console.log('Features:');
    console.log('- Simple container status view');
    console.log('- Basic container controls (start/stop/restart)');
    console.log('- Custom health checks');
    console.log('- Extensible for future custom logic');
    console.log('- DuckDB tracking database for screening results');
    
    console.log('Server ready - tracking now uses DuckDB instead of Elasticsearch');
});
