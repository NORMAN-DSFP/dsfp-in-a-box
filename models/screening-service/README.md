# DSFP Screening Service

A Python-based microservice that transforms the JavaScript batch screening functionality into a containerized service. This service processes individual JSON files from the screening index and performs substance screening with scoring.

## Features

- **Single Sample Processing**: Processes one JSON file at a time from the screening index
- **Substance Search**: Searches for specified substances in the compounds index
- **Multi-modal Scoring**: Calculates RTI, m/z, fragment, and spectral similarity scores
- **External Service Integration**: Calls semiquantification and spectral similarity containers
- **RESTful API**: Provides HTTP endpoints for screening operations

## API Endpoints

### `GET /health`
Health check endpoint that returns service status.

**Response:**
```json
{
  "status": "healthy",
  "service": "screening-service"
}
```

### `POST /screen`
Main screening endpoint that processes a sample against specified substances. The service automatically retrieves sample data from the screening index using the provided sample_id.

**Request Body:**
```json
{
  "sample_id": "example_sample_001",
  "substances": ["Caffeine", "Atrazine"],
  "mz_tolerance": 0.005,
  "rti_tolerance": 20.0,
  "filter_by_blanks": true
}
```

**Parameters:**
- `sample_id` (required): Sample identifier to retrieve from the screening index
- `substances` (required): List of substance names/IDs to search for
- `mz_tolerance` (optional): Mass tolerance in Da (default: 0.005)
- `rti_tolerance` (optional): RTI tolerance percentage (default: 20.0)
- `filter_by_blanks` (optional): Whether to filter by blanks (default: true)

**Note:** The service automatically:
- Retrieves sample data from the hardcoded screening index (`dsfp-test-index-3`)
- Extracts the collection ID from the sample data for filtering
- No need to specify index or collection ID in the request

**Response:**
```json
{
  "results": [
    {
      "detection_id": "collection_123_substance_sample",
      "sample_id": 123,
      "substance_id": "substance_id",
      "short_name": "Sample Name",
      "sample_type": "water",
      "scores": {
        "rti": 85.23,
        "mz": 92.45,
        "fragments": 78.12,
        "spectral_similarity": 0.85
      }
    }
  ],
  "substance_count": 2
}
```

## Parameters

### Required Parameters
- **sample_data**: JSON object containing sample information from screening index
- **substances**: List of substance names/IDs to search for

### Optional Parameters
- **selected_collection**: Collection ID to search within (if not provided, will be derived from sample_data.collection_id)
- **mz_tolerance**: Mass tolerance for m/z matching (default: 0.005)
- **rti_tolerance**: RTI tolerance percentage (default: 20.0)
- **filter_by_blanks**: Whether to filter results by blank samples (default: true)
- **index**: Elasticsearch index to search (default: "dsfp-test-index-3")

## Service Dependencies

The screening service depends on the following services:

1. **Elasticsearch**: For substance and sample data queries
2. **Semiquantification Service**: For concentration calculations (port 8001)
3. **Spectral Similarity Service**: For spectral matching (port 8002)

## Environment Variables

The service uses these environment variables:

- `SCREENING_CLIENT`: Elasticsearch URL for screening data (default: http://elasticsearch:9200)
- `SUBSTANCE_CLIENT`: Elasticsearch URL for compound data (default: http://elasticsearch:9200)
- `SEMIQUANTIFICATION_URL`: URL for semiquantification service (default: http://dsfp-semiquantification:8001/semiquantification)
- `SPECTRAL_SIMILARITY_URL`: URL for spectral similarity service (default: http://dsfp-spectral-similarity:8002/spectral_similarity_score)

**Note**: All Elasticsearch connections use no authentication as per the containerized setup.

## Docker Configuration

The service runs on port 8003 and is configured in docker-compose.yml:

```yaml
models-screening-service:
  build: ./models/screening-service
  container_name: dsfp-screening-service
  ports:
    - "${SCREENING_SERVICE_PORT:-8003}:${SCREENING_SERVICE_PORT:-8003}"
  env_file:
    - ./.env
  depends_on:
    - elasticsearch
    - models-semiquantification
    - models-spectral-similarity
```

## Usage Example

### Python
```python
import requests

sample_data = {
    "sample_id": 123,
    "collection_id": "test-collection",
    "sample_type": "water"
}

request = {
    "sample_data": sample_data,
    "substances": ["Caffeine"],
    "mz_tolerance": 0.005,
    "rti_tolerance": 20.0,
    "selected_collection": "test-collection"
}

response = requests.post("http://localhost:8003/screen", json=request)
results = response.json()
```

### JavaScript/Node.js
```javascript
const axios = require('axios');

const requestData = {
  sample_data: {
    sample_id: 123,
    collection_id: "test-collection",
    sample_type: "water"
  },
  substances: ["Caffeine"],
  mz_tolerance: 0.005,
  rti_tolerance: 20.0,
  selected_collection: "test-collection"
};

const response = await axios.post('http://localhost:8003/screen', requestData);
const results = response.data;
```

## Testing

Run the test script to verify the service is working:

```bash
python test_screening.py
```

## Key Differences from Original JS Function

1. **Single Sample Focus**: Processes one sample at a time instead of batch processing
2. **Containerized**: Runs as a separate Docker service
3. **RESTful API**: HTTP-based instead of direct function calls
4. **Simplified Parameters**: Removes batch-specific parameters like samples_from/samples_to
5. **Async Processing**: Uses async/await for better performance
6. **Error Handling**: Comprehensive error handling and logging

## Scoring Methods

The service calculates four types of scores:

1. **RTI Score**: Based on retention time index matching
2. **m/z Score**: Based on mass-to-charge ratio accuracy
3. **Fragment Score**: Based on MS/MS fragment matching
4. **Spectral Similarity**: Based on spectral pattern matching (via external service)

## Performance Considerations

- The service processes one sample at a time for better resource management
- Async HTTP calls to external services (semiquantification, spectral similarity)
- Elasticsearch queries are optimized for single-sample processing
- Timeouts are configured for external service calls

## Troubleshooting

1. **Service not responding**: Check if all dependent services are running
2. **Elasticsearch connection failed**: Verify elasticsearch container is healthy
3. **External service timeouts**: Check semiquantification and spectral similarity services
4. **No results returned**: Verify substance names exist in compounds index
