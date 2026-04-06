const express = require('express');
const Docker = require('dockerode');
const cors = require('cors');
const path = require('path');
const multer = require('multer');
const fs = require('fs');
const StreamZip = require('node-stream-zip');
const axios = require('axios');

const app = express();
const PORT = process.env.STATUS_DASHBOARD_PORT || 3008;
const docker = new Docker();

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

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
            const response = await axios.post(`${ELASTICSEARCH_URL}/${indexName}/_doc/${doc._id || ''}`, 
                doc.data || doc,
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
            console.error(`Error inserting document ${doc._id}:`);
            console.error('  Message:', error.message);
            if (error.response) {
                console.error('  Status:', error.response.status);
                console.error('  Data:', JSON.stringify(error.response.data, null, 2));
            }
            
            const errorDetail = error.response?.data?.error || error.message;
            errors.push({ id: doc._id, error: errorDetail });
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
        const containers = await docker.listContainers({ all: true });
        
        // Filter to only show DSFP-related containers
        const dsfpContainers = containers.filter(container => {
            const name = container.Names[0].replace('/', '');
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
            
            return {
                id: container.Id.substring(0, 12),
                name: name,
                image: container.Image,
                state: container.State,
                status: container.Status,
                isRunning: isRunning,
                serviceType: serviceType,
                ports: container.Ports.map(port => ({
                    privatePort: port.PrivatePort,
                    publicPort: port.PublicPort,
                    type: port.Type
                })),
                created: new Date(container.Created * 1000).toISOString(),
                uptime: isRunning ? getUptime(container.Status) : null
            };
        });
        
        res.json({
            success: true,
            containers: containerInfo,
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
    
    conn.close()
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
    conn.close()
    
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
    
    conn.close()
    
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
    
    conn.close();
    
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
           COUNT(CASE WHEN sr.spectral_similarity_score > 0.7 THEN 1 END) as detected,
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
    
    conn.close()
    
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

// Serve the data loader page
app.get('/data-loader.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'data-loader.html'));
});

// Serve the screening page
app.get('/screening.html', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'screening.html'));
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
    conn.close()
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
        const { sample_id } = req.query;
        
        const { spawn } = require('child_process');
        
        // Build the query based on whether we're filtering by sample_id
        const whereClause = sample_id ? `WHERE sample_id = '${sample_id}'` : '';
        
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
            created_at
        FROM screening_results
        ${whereClause}
        ORDER BY spectral_similarity_score DESC, sample_id, substance_name
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
                'created_at': str(row[19]) if row[19] is not None else None
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
