from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import requests
import json
import math
import os
from elasticsearch import Elasticsearch
import asyncio
import aiohttp
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="DSFP Screening Service", version="1.0.0")

# Configuration
ELASTICSEARCH_URL = os.getenv('SCREENING_CLIENT', 'http://elasticsearch:9200')
SUBSTANCE_CLIENT_URL = os.getenv('SUBSTANCE_CLIENT', 'http://elasticsearch:9200')
SEMIQUANTIFICATION_URL = os.getenv('SEMIQUANTIFICATION_URL', 'http://dsfp-semiquantification:8001/semiquantification')
SPECTRAL_SIMILARITY_URL = os.getenv('SPECTRAL_SIMILARITY_URL', 'http://dsfp-spectral-similarity:8002/spectral_similarity_score')

# Hardcoded screening index
SCREENING_INDEX = "dsfp-screening-index"
# Note: TRACKING_INDEX removed - now using DuckDB for tracking

# Elasticsearch clients (no authentication required)
es_client = Elasticsearch([ELASTICSEARCH_URL])
substance_client = Elasticsearch([SUBSTANCE_CLIENT_URL])

class ScreeningRequest(BaseModel):
    sample_id: str  # Sample ID to retrieve from the screening index
    substances: List[str]  # List of substance names/IDs to search for
    mz_tolerance: float = 0.005
    rti_tolerance: float = 20.0
    filter_by_blanks: bool = True

class SubstanceData(BaseModel):
    id: str
    name: str
    cas: str
    smiles: str
    lc_rti: float
    model_coverage: str
    ionization: str
    exp_records: List[Any]
    mz: float
    compound_mol: str
    compound_adducts: List[str]
    fragments: List[float]

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "screening-service"}

async def get_sample_data(sample_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve sample data from the screening index"""
    try:
        # Convert sample_id to integer for the search since it's stored as integer in ES
        try:
            sample_id_int = int(sample_id)
        except ValueError:
            logger.error(f"Invalid sample_id format: {sample_id}. Must be a valid integer.")
            return None
            
        query = {
            "query": {
                "term": {
                    "sample_id": sample_id_int
                }
            },
            "size": 1
        }
        
        response = es_client.search(index=SCREENING_INDEX, body=query)
        
        if response['hits']['hits']:
            return response['hits']['hits'][0]['_source']
        else:
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving sample data for {sample_id}: {str(e)}")
        return None

async def save_screening_result(request: ScreeningRequest, sample_data: Dict, screening_results: List[Dict]):
    """Save screening results directly to DuckDB tracking database with proper connection handling"""
    try:
        logger.info(f"[SAVE DEBUG] Starting save_screening_result for sample {request.sample_id}")
        logger.info(f"[SAVE DEBUG] Number of screening_results: {len(screening_results)}")
        logger.info(f"[SAVE DEBUG] Number of request.substances: {len(request.substances)}")
        
        # Import tracking database
        import sys
        sys.path.append('/app/setup')
        from tracking_db import TrackingDatabase
        
        # Create a new database instance with a fresh connection
        db = TrackingDatabase()
        logger.info(f"[SAVE DEBUG] Database connection created successfully")
        
        # Create a set of substances that had actual detection results
        substances_with_results = set()
        logger.info(f"[SAVE DEBUG] Processing substances with detection results...")
        for result in screening_results:
            substance_name = result.get('substance_name')
            if substance_name:
                logger.info(f"[SAVE DEBUG] Saving result for substance: {substance_name}")
                substances_with_results.add(substance_name)
                # Save the actual detection result
                success = db.save_screening_result(
                    sample_id=request.sample_id,
                    substance_name=substance_name,
                    substance_id=result.get('substance_id', substance_name),
                    result_data=result,
                    timestamp=datetime.utcnow().isoformat() + 'Z'
                )
                if not success:
                    logger.warning(f"Failed to save screening result for substance {substance_name}")
                else:
                    logger.info(f"[SAVE DEBUG] Successfully saved detection result for {substance_name}")
        
        logger.info(f"[SAVE DEBUG] Substances with results: {substances_with_results}")
        logger.info(f"[SAVE DEBUG] Processing substances without detection results...")
        # For substances that were screened but had NO results, still track them
        for substance_name in request.substances:
            if substance_name not in substances_with_results:
                logger.info(f"[SAVE DEBUG] Saving no-result tracking for substance: {substance_name}")
                # Create a minimal result_data with sample info but no detection
                minimal_result = {
                    'collection_id': sample_data.get('collection_id'),
                    'collection_uid': sample_data.get('collection_uid'),
                    'collection_title': sample_data.get('collection_title'),
                    'short_name': sample_data.get('short_name'),
                    'matrix_type': sample_data.get('matrix_type'),
                    'matrix_type2': sample_data.get('matrix_type2'),
                    'sample_type': sample_data.get('sample_type'),
                    'monitored_city': sample_data.get('monitored_city'),
                    'sampling_date': sample_data.get('sampling_date'),
                    'analysis_date': sample_data.get('analysis_date'),
                    'latitude': sample_data.get('latitude'),
                    'longitude': sample_data.get('longitude'),
                    'instrument_setup_used': sample_data.get('instrument_setup_used', {}),
                    'mz_tolerance': request.mz_tolerance,
                    'rti_tolerance': request.rti_tolerance,
                    'filter_by_blanks': request.filter_by_blanks,
                    'scores': {},  # Empty scores
                    'semiquantification': None,
                    'matches': []  # No matches
                }
                
                success = db.save_screening_result(
                    sample_id=request.sample_id,
                    substance_name=substance_name,
                    substance_id=substance_name,
                    result_data=minimal_result,
                    timestamp=datetime.utcnow().isoformat() + 'Z'
                )
                if not success:
                    logger.warning(f"Failed to save no-result tracking for substance {substance_name}")
                else:
                    logger.info(f"[SAVE DEBUG] Successfully saved no-result tracking for {substance_name}")
        
        # Properly close the database connection
        db.close()
        logger.info(f"[SAVE DEBUG] Database connection closed")
        
        logger.info(f"Successfully saved screening results to DuckDB for sample {request.sample_id}")
        return {"success": True, "screening_id": f"{request.sample_id}_screening"}
            
    except Exception as e:
        logger.error(f"Failed to save screening results to DuckDB: {str(e)}")
        # Log the full traceback for debugging
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return None

@app.post("/screen")
async def screen_sample(request: ScreeningRequest):
    """
    Screen a single sample against specified substances.
    The service will retrieve the sample data from the screening index using the provided sample_id.
    """
    try:
        logger.info(f"Starting screening for sample {request.sample_id}")
        
        # Step 1: Retrieve sample data from the screening index
        sample_data = await get_sample_data(request.sample_id)
        
        if not sample_data:
            return {"results": [], "message": f"Sample {request.sample_id} not found in screening index"}
        
        # Step 2: Get substance data from compounds index
        substance_data = await get_substance_data(request.substances)
        if not substance_data:
            # Even if no substance data found, save tracking records for failed substance loads
            await save_screening_result(request, sample_data, [])
            return {"results": [], "substance_count": 0, "message": "No valid substances found", "success": False}
        
        # Step 3: Get RTI bounds for search windows
        rti_bounds = await get_rti_bounds()
        
        # Step 4: Perform primary search
        primary_results = await perform_primary_search(
            request, sample_data, substance_data, rti_bounds
        )
        
        # Step 5: Process results and calculate scores
        final_results = await process_results(
            request, sample_data, substance_data, primary_results, rti_bounds
        )
        
        # Step 6: Save screening results to DuckDB tracking database
        await save_screening_result(request, sample_data, final_results)
        
        logger.info(f"Screening completed with {len(final_results)} results")
        return {"results": final_results, "substance_count": len(substance_data), "success": True}
        
    except Exception as e:
        logger.error(f"Screening failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Screening failed: {str(e)}")

async def get_substance_data(substances: List[str]) -> List[SubstanceData]:
    """Get substance data from compounds index"""
    substance_data = []
    
    for substance in substances:
        try:
            query = {
                "query": {
                    "query_string": {
                        "query": substance,
                        "fields": ["name", "cas", "norman_id"]
                    }
                },
                "index": "dsfp-compounds-1"
            }
            
            response = substance_client.search(body=query)
            
            if response['hits']['hits']:
                source = response['hits']['hits'][0]['_source']
                
                # Process substance options (similar to JS logic)
                options = []
                experimental_pos = False
                outside_pos = False
                experimental_neg = False
                outside_neg = False
                
                # Positive ionization options
                if source['rti']['uncertainty_rti_pos'] == "Covered by Model":
                    options.extend([
                        {
                            "pred_rti": source['rti']['pred_rti_positive_esi'],
                            "adduct_mz": source['ms_information'][0]['exp_mz_adduct'],
                            "fragments": source['ms_information'][0]['exp_fragments']
                        },
                        {
                            "pred_rti": source['rti']['pred_rti_positive_esi'],
                            "adduct_mz": source['ms_information'][0]['pred_mz_adduct'],
                            "fragments": source['ms_information'][0]['pred_cmfid_fragments']
                        }
                    ])
                else:
                    if "Experimental proof" in source['rti']['uncertainty_rti_pos']:
                        experimental_pos = True
                    if "outside" in source['rti']['uncertainty_rti_pos'].lower():
                        outside_pos = True
                    
                    if experimental_pos or outside_pos:
                        options.extend([
                            {
                                "pred_rti": source['rti']['pred_rti_positive_esi'],
                                "adduct_mz": source['ms_information'][0]['exp_mz_adduct'],
                                "fragments": source['ms_information'][0]['exp_fragments']
                            },
                            {
                                "pred_rti": source['rti']['pred_rti_positive_esi'],
                                "adduct_mz": source['ms_information'][0]['pred_mz_adduct'],
                                "fragments": source['ms_information'][0]['pred_cmfid_fragments']
                            }
                        ])
                    else:
                        options.extend([{"pred_rti": "", "adduct_mz": "", "fragments": ""}] * 2)
                
                # Negative ionization options
                if source['rti']['uncertainty_rti_neg'] == "Covered by Model":
                    options.extend([
                        {
                            "pred_rti": source['rti']['pred_rti_negative_esi'],
                            "adduct_mz": source['ms_information'][1]['exp_mz_adduct'],
                            "fragments": source['ms_information'][1]['exp_fragments']
                        },
                        {
                            "pred_rti": source['rti']['pred_rti_negative_esi'],
                            "adduct_mz": source['ms_information'][1]['pred_mz_adduct'],
                            "fragments": source['ms_information'][1]['pred_cmfid_fragments']
                        }
                    ])
                else:
                    if "Experimental proof" in source['rti']['uncertainty_rti_neg']:
                        experimental_neg = True
                    if "outside" in source['rti']['uncertainty_rti_neg'].lower():
                        outside_neg = True
                    
                    if experimental_neg or outside_neg:
                        options.extend([
                            {
                                "pred_rti": source['rti']['pred_rti_negative_esi'],
                                "adduct_mz": source['ms_information'][1]['exp_mz_adduct'],
                                "fragments": source['ms_information'][1]['exp_fragments']
                            },
                            {
                                "pred_rti": source['rti']['pred_rti_negative_esi'],
                                "adduct_mz": source['ms_information'][1]['pred_mz_adduct'],
                                "fragments": source['ms_information'][1]['pred_cmfid_fragments']
                            }
                        ])
                    else:
                        options.extend([{"pred_rti": "", "adduct_mz": "", "fragments": ""}] * 2)
                
                # Determine preselection
                preselection = determine_preselection(source, options)
                
                # Get adducts
                adducts = []
                if preselection < 2:
                    ms_info = [ms for ms in source["ms_information"] if ms.get("ionization") == "Positive"]
                    if ms_info:
                        adducts = ms_info[0].get('exp_adduct', [])
                else:
                    ms_info = [ms for ms in source["ms_information"] if ms.get("ionization") == "Negative"]
                    if ms_info:
                        adducts = ms_info[0].get('exp_adduct', [])
                
                if not adducts:
                    adducts = ['[M+H]+'] if preselection < 2 else ['[M-H]-']
                
                # Create substance data
                model_coverage = 'covered'
                if (preselection < 2 and experimental_pos) or (preselection >= 2 and experimental_neg):
                    model_coverage = 'experimental'
                elif (preselection < 2 and outside_pos) or (preselection >= 2 and outside_neg):
                    model_coverage = 'outside'
                
                fragments = []
                if options[preselection].get('fragments'):
                    fragments = flatten_list(options[preselection]['fragments'])
                
                substance_data.append(SubstanceData(
                    id=source['norman_id'],
                    name=source['name'],
                    cas=source['cas'],
                    smiles=source['structure']['smiles'],
                    lc_rti=options[preselection].get('pred_rti', 0),
                    model_coverage=model_coverage,
                    ionization="Positive" if preselection < 2 else "Negative",
                    exp_records=source['ms_information'][0 if preselection < 2 else 1].get('exp_records', []),
                    mz=options[preselection].get('adduct_mz', [0])[0] if options[preselection].get('adduct_mz') else 0,
                    compound_mol=source['mol_formula'],
                    compound_adducts=adducts,
                    fragments=fragments
                ))
                
        except Exception as e:
            logger.error(f"Error processing substance {substance}: {str(e)}")
            continue
    
    return substance_data

def determine_preselection(source, options):
    """Determine the best preselection based on preferable ionization"""
    preselection = 0
    preferable = source.get('preferable_ionization', 'Positive')
    
    if preferable == "Positive":
        if not options[0].get('adduct_mz') or len(options[0].get('adduct_mz', [])) == 0:
            preselection = 1
        else:
            preselection = 0
    elif preferable == "Negative":
        if not options[2].get('adduct_mz') or len(options[2].get('adduct_mz', [])) == 0:
            preselection = 3
        else:
            preselection = 2
    elif preferable == "Positive/Negative":
        if not options[0].get('adduct_mz') or len(options[0].get('adduct_mz', [])) == 0:
            if options[1].get('adduct_mz') and len(options[1].get('adduct_mz', [])) > 0:
                preselection = 1
            else:
                if not options[2].get('adduct_mz') or len(options[2].get('adduct_mz', [])) == 0:
                    if not options[3].get('adduct_mz') or len(options[3].get('adduct_mz', [])) == 0:
                        preselection = 0
                    else:
                        preselection = 3
                else:
                    preselection = 2
        else:
            preselection = 0
    
    return preselection

def flatten_list(nested_list):
    """Flatten nested lists"""
    if not nested_list:
        return []
    
    result = []
    for item in nested_list:
        if isinstance(item, list):
            result.extend(flatten_list(item))
        else:
            result.append(item)
    return result

async def get_rti_bounds():
    """Get RTI bounds for search windows"""
    try:
        query = {
            "size": 0,
            "aggs": {
                "min_rti_pos": {"min": {"field": "rti.pred_rti_positive_esi"}},
                "max_rti_pos": {"max": {"field": "rti.pred_rti_positive_esi"}},
                "min_rti_neg": {"min": {"field": "rti.pred_rti_negative_esi"}},
                "max_rti_neg": {"max": {"field": "rti.pred_rti_negative_esi"}}
            }
        }
        
        response = substance_client.search(body=query)
        return response['aggregations']
    except Exception as e:
        logger.error(f"Error getting RTI bounds: {str(e)}")
        return {
            "min_rti_pos": {"value": 0},
            "max_rti_pos": {"value": 1000},
            "min_rti_neg": {"value": 0},
            "max_rti_neg": {"value": 1000}
        }

async def perform_primary_search(request: ScreeningRequest, sample_data: Dict[str, Any], substance_data: List[SubstanceData], rti_bounds):
    """Perform primary search against the index"""
    try:
        # Calculate RTI windows
        rti_window_pos = request.rti_tolerance * (rti_bounds['max_rti_pos']['value'] - rti_bounds['min_rti_pos']['value']) * 0.01 / 2
        rti_window_neg = request.rti_tolerance * (rti_bounds['max_rti_neg']['value'] - rti_bounds['min_rti_neg']['value']) * 0.01 / 2
        rti_window_experimental_pos = 50 * (rti_bounds['max_rti_pos']['value'] - rti_bounds['min_rti_pos']['value']) * 0.01 / 2
        rti_window_experimental_neg = 50 * (rti_bounds['max_rti_neg']['value'] - rti_bounds['min_rti_neg']['value']) * 0.01 / 2

        # Build search query
        should_clauses = []
        for s in substance_data:
            if s.model_coverage == 'covered':
                rti_window = rti_window_pos if s.ionization == 'Positive' else rti_window_neg
            elif s.model_coverage == 'experimental':
                rti_window = rti_window_experimental_pos if s.ionization == 'Positive' else rti_window_experimental_neg
            else:  # outside
                rti_window = None
            
            gte_rti = s.lc_rti - rti_window if rti_window else rti_bounds['min_rti_pos']['value'] if s.ionization == 'Positive' else rti_bounds['min_rti_neg']['value']
            lte_rti = s.lc_rti + rti_window if rti_window else rti_bounds['max_rti_pos']['value'] if s.ionization == 'Positive' else rti_bounds['max_rti_neg']['value']
            gte_mz = s.mz - request.mz_tolerance
            lte_mz = s.mz + request.mz_tolerance
            
            if s.model_coverage == 'outside':
                if s.ionization == 'Positive':
                    gte_rti = rti_bounds['min_rti_pos']['value']
                    lte_rti = rti_bounds['max_rti_pos']['value']
                else:
                    gte_rti = rti_bounds['min_rti_neg']['value']
                    lte_rti = rti_bounds['max_rti_neg']['value']
            
            should_clauses.append({
                "bool": {
                    "_name": s.id,
                    "filter": [
                        {
                            "range": {
                                "fullscan.lc_retention_index": {
                                    "gte": gte_rti if not math.isnan(gte_rti) else -1,
                                    "lte": lte_rti if not math.isnan(lte_rti) else -1
                                }
                            }
                        },
                        {
                            "range": {
                                "fullscan.mz": {
                                    "gte": gte_mz if not math.isnan(gte_mz) else -1,
                                    "lte": lte_mz if not math.isnan(lte_mz) else -1
                                }
                            }
                        }
                    ]
                }
            })
        
        query = {
            "size": 10000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "match": {
                                "collection_id": {
                                    "query": sample_data.get("collection_id")
                                }
                            }
                        },
                        {
                            "term": {
                                "sample_id": sample_data.get("sample_id")
                            }
                        },
                        {
                            "nested": {
                                "path": "fullscan",
                                "query": {
                                    "bool": {
                                        "should": should_clauses
                                    }
                                },
                                "inner_hits": {}
                            }
                        }
                    ]
                }
            },
            "_source_includes": ["_id", "short_name", "collection_id", "collection_uid", "collection_title", 
                               "matrix_type", "matrix_type2", "sample_type", "monitored_city", 
                               "sampling_date", "analysis_date", "latitude", "longitude", "instrument_setup_used"],
            "sort": [
                {
                    "sample_id": "asc",
                    "fullscan.lc_retention_index": {
                        "order": "asc",
                        "mode": "sum",
                        "nested": {
                            "path": "fullscan"
                        }
                    }
                }
            ]
        }
        
        
        response = es_client.search(index=SCREENING_INDEX, body=query)
        return response['hits']['hits']
        
    except Exception as e:
        logger.error(f"Primary search failed: {str(e)}")
        return []

async def process_results(request: ScreeningRequest, sample_data: Dict[str, Any], substance_data: List[SubstanceData], primary_results, rti_bounds):
    """Process search results and calculate scores"""
    final_results = []
    
    for substance in substance_data:
        if substance.lc_rti and substance.mz:
            # Filter results for this substance using manual m/z and RTI matching
            # since Elasticsearch matched_queries field is not reliable in nested queries
            substance_hits = []
            
            for hit in primary_results:
                # Check if this hit matches our substance criteria
                inner_hits = hit.get('inner_hits', {}).get('fullscan', {}).get('hits', {}).get('hits', [])
                
                # Filter inner_hits to only include those matching THIS substance
                matching_inner_hits = []
                for inner_hit in inner_hits:
                    source = inner_hit.get('_source', {})
                    mz_obs = source.get('mz', 0)
                    rti_obs = source.get('lc_retention_index', 0)
                    
                    # Check if m/z and RTI are within tolerance for this substance
                    mz_match = abs(mz_obs - substance.mz) <= request.mz_tolerance
                    
                    # Calculate RTI tolerance window
                    if substance.model_coverage == 'covered':
                        rti_window = request.rti_tolerance * (rti_bounds['max_rti_pos']['value'] - rti_bounds['min_rti_pos']['value']) * 0.01 / 2 if substance.ionization == 'Positive' else request.rti_tolerance * (rti_bounds['max_rti_neg']['value'] - rti_bounds['min_rti_neg']['value']) * 0.01 / 2
                        rti_match = abs(rti_obs - substance.lc_rti) <= rti_window
                    else:
                        # For experimental or outside, use broader tolerance
                        rti_match = True  # Accept all RTI values for now
                    
                    if mz_match and rti_match:
                        matching_inner_hits.append(inner_hit)
                
                # Only add this hit if it has matching inner_hits for this substance
                if matching_inner_hits:
                    # Create a copy of the hit with only the matching inner_hits
                    filtered_hit = hit.copy()
                    filtered_hit['inner_hits'] = {
                        'fullscan': {
                            'hits': {
                                'hits': matching_inner_hits
                            }
                        }
                    }
                    substance_hits.append(filtered_hit)
            
            if not substance_hits:
                continue
            
            
            # Get fragment data
            fragment_data = await get_fragment_data(request, substance_hits)
            
            # Calculate scores
            rti_scores = await calculate_rti_scores(substance_hits, substance.lc_rti)
            mz_scores = await calculate_mz_scores(substance_hits, substance.mz, request.mz_tolerance)
            fragment_scores = await calculate_fragment_scores(substance_hits, fragment_data, substance.fragments, request.mz_tolerance)
            
            # Call external services
            spectral_similarity_scores = await call_spectral_similarity(substance_hits, substance)
            genform_scores = await call_genform(substance_hits, substance, request.mz_tolerance)
            semiquantification_results = await call_semiquantification(substance_hits, substance)
            
            # Combine results
            for i, hit in enumerate(substance_hits):
                result = hit['_source'].copy()
                result['scores'] = {
                    'rti': rti_scores[i] if i < len(rti_scores) else 0,
                    'mz': mz_scores[i] if i < len(mz_scores) else 0,
                    'fragments': fragment_scores[i] if i < len(fragment_scores) else 0,
                    'spectral_similarity': spectral_similarity_scores[i] if i < len(spectral_similarity_scores) else 0,
                    'isotopic_fit': genform_scores["isotopic_fit"][i] if genform_scores and i < len(genform_scores["isotopic_fit"]) else None,
                    'molecular_formula_fit': genform_scores["molecular_formula_fit"][i] if genform_scores and i < len(genform_scores["molecular_formula_fit"]) else None
                }
                
                # Add semiquantification data
                if semiquantification_results and i < len(semiquantification_results):
                    result['semiquantification'] = semiquantification_results[i]
                else:
                    result['semiquantification'] = None
                result['substance_id'] = substance.id
                result['substance_name'] = substance.name  # Add substance name for tracking database
                result['detection_id'] = f"{result['collection_id']}_{substance.id}_{result.get('id', hit['_id'])}"
                
                # Extract matching inner hits data
                matching_inner_hits_data = []
                inner_hits = hit.get('inner_hits', {}).get('fullscan', {}).get('hits', {}).get('hits', [])
                for inner_hit in inner_hits:
                    inner_source = inner_hit.get('_source', {})
                    matching_inner_hits_data.append({
                        "peak_area": inner_source.get('peak_area'),
                        "mz": inner_source.get('mz'),
                        "max_intensity": inner_source.get('max_intensity'),
                        "rt_minutes": inner_source.get('rt_minutes'),
                        "ms_ms_available": inner_source.get('ms_ms_available'),
                        "lc_retention_index": inner_source.get('lc_retention_index'),
                        "gc_retention_time_index": inner_source.get('gc_retention_time_index'),
                        "isotopes_mz": inner_source.get('isotopes_mz'),
                        "isotopes_int": inner_source.get('isotopes_int'),
                        "isotopes_rt": inner_source.get('isotopes_rt'),
                        "adducts_mz": inner_source.get('adducts_mz'),
                        "adducts_int": inner_source.get('adducts_int'),
                        "adducts_rt": inner_source.get('adducts_rt'),
                        "hrmsms_mz": inner_source.get('hrmsms_mz'),
                        "hrmsms_int": inner_source.get('hrmsms_int')
                    })
                
                result['matches'] = matching_inner_hits_data
                
                # Calculate IP score
                ip_score = ipscore(result)
                result['scores']['ip_score'] = round(ip_score, 4)
                
                final_results.append(result)
    
    return final_results

async def get_fragment_data(request, hits):
    """Get fragment data for hits"""
    try:
        ids = [hit['_id'] for hit in hits]
        
        query = {
            "size": 10000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "ids": {
                                "values": ids
                            }
                        }
                    ]
                }
            },
            "_source_includes": ["sample_id", "fullscan.hrmsms_mz", "fullscan.hrmsms_int", "fullscan.mz", "data_independent.mz"]
        }
        
        response = es_client.search(index=SCREENING_INDEX, body=query)
        return response['hits']['hits']
        
    except Exception as e:
        logger.error(f"Error getting fragment data: {str(e)}")
        return []

async def calculate_rti_scores(hits, rti_exp):
    """Calculate RTI scores"""
    scores = []
    for hit in hits:
        rti_scores = []
        inner_hits = hit.get('inner_hits', {}).get('fullscan', {}).get('hits', {}).get('hits', [])
        
        for inner_hit in inner_hits:
            source = inner_hit.get('_source', {})
            rti_obs = source.get('lc_retention_index', 0)
            score = 1 - (abs(rti_exp - rti_obs) / 1000)
            rti_scores.append(score)
        
        if rti_scores:
            scores.append(round(max(rti_scores) * 100, 2))
        else:
            scores.append(0)
    
    return scores

async def calculate_mz_scores(hits, mz_theoretical, tolerance):
    """Calculate m/z scores"""
    scores = []
    print(f"[MZ DEBUG] Processing {len(hits)} hits, theoretical mz: {mz_theoretical}, tolerance: {tolerance}")
    
    for i, hit in enumerate(hits):
        matched_mz = []
        inner_hits = hit.get('inner_hits', {}).get('fullscan', {}).get('hits', {}).get('hits', [])
        print(f"[MZ DEBUG] Hit {i}: Found {len(inner_hits)} inner hits")
        
        for inner_hit in inner_hits:
            source = inner_hit.get('_source', {})
            mz_exp = source.get('mz', 0)
            print(f"[MZ DEBUG] Inner hit mz: {mz_exp}")
            if mz_exp > 0:
                # EXACT JavaScript: Math.abs(1-(Math.abs(mzExp - mzTheoretical)/Math.min(mzExp, mzTheoretical)*((10^6)/(tolerance*(10^6)/mzExp))))
                # Step by step with EXACT operator precedence:
                
                # Math.abs(mzExp - mzTheoretical)
                abs_diff = abs(mz_exp - mz_theoretical)
                print(f"[MZ DEBUG] abs_diff: {abs_diff}")
                
                # Math.min(mzExp, mzTheoretical)
                min_mz = min(mz_exp, mz_theoretical)
                print(f"[MZ DEBUG] min_mz: {min_mz}")
                
                # (10^6) - This is 1000000 in JavaScript (Math.pow(10,6))
                power_of_10_6 = 1000000
                
                # tolerance*(10^6)
                tolerance_times_power = tolerance * power_of_10_6
                print(f"[MZ DEBUG] tolerance_times_power: {tolerance_times_power}")
                
                # tolerance*(10^6)/mzExp
                tolerance_power_div_mz = tolerance_times_power / mz_exp
                print(f"[MZ DEBUG] tolerance_power_div_mz: {tolerance_power_div_mz}")
                
                # (10^6)/(tolerance*(10^6)/mzExp)
                fraction_part = power_of_10_6 / tolerance_power_div_mz
                print(f"[MZ DEBUG] fraction_part: {fraction_part}")
                
                # Math.abs(mzExp - mzTheoretical)/Math.min(mzExp, mzTheoretical)
                relative_error = abs_diff / min_mz
                print(f"[MZ DEBUG] relative_error: {relative_error}")
                
                # Math.abs(mzExp - mzTheoretical)/Math.min(mzExp, mzTheoretical)*((10^6)/(tolerance*(10^6)/mzExp))
                multiplied_error = relative_error * fraction_part
                print(f"[MZ DEBUG] multiplied_error: {multiplied_error}")
                
                # 1-(Math.abs(mzExp - mzTheoretical)/Math.min(mzExp, mzTheoretical)*((10^6)/(tolerance*(10^6)/mzExp)))
                subtracted = 1 - multiplied_error
                print(f"[MZ DEBUG] subtracted: {subtracted}")
                
                # Math.abs(1-(Math.abs(mzExp - mzTheoretical)/Math.min(mzExp, mzTheoretical)*((10^6)/(tolerance*(10^6)/mzExp))))
                score = abs(subtracted)
                print(f"[MZ DEBUG] Final raw score: {score}")
                
                matched_mz.append(score)
        
        if matched_mz:
            final_score = round(max(matched_mz) * 100, 2)
            print(f"[MZ DEBUG] Hit {i}: Final score: {final_score}")
            scores.append(final_score)
        else:
            print(f"[MZ DEBUG] Hit {i}: No matched mz values, score: 0")
            scores.append(0)
    
    return scores

async def calculate_fragment_scores(hits, fragment_hits, fragments, tolerance):
    """Calculate fragment scores"""
    scores = []
    print(fragments)
    for fragment_hit in fragment_hits:
        hit_id = fragment_hit['_id']
        count = 0
        
        for fragment in fragments:
            found_once = False
            source = fragment_hit.get('_source', {})
            
            # Check fullscan fragments
            fullscan = source.get('fullscan', [])
            for fs in fullscan:
                hrmsms_mz = fs.get('hrmsms_mz', [])
                for mz in hrmsms_mz:
                    if fragment - tolerance <= mz <= fragment + tolerance:
                        count += 1
                        found_once = True
                        break
                if found_once:
                    break
            
            # Check data_independent if not found
            if not found_once:
                data_independent = source.get('data_independent', [])
                if data_independent:
                    mz_list = data_independent[0].get('mz', [])
                    for mz in mz_list:
                        if fragment - tolerance <= mz <= fragment + tolerance:
                            count += 1
                            found_once = True
                            break
        
        if fragments:
            scores.append(round((count / len(fragments)) * 100, 2))
        else:
            scores.append(0)
    
    return scores

async def call_spectral_similarity(hits, substance):
    """Call spectral similarity service"""
    scores = []
    
    try:
        logger.info("[Searching...] Starting spectral similarity...")
        
        async with aiohttp.ClientSession() as session:
            for i, hit in enumerate(hits):
                inner_hits = hit.get('inner_hits', {}).get('fullscan', {}).get('hits', {}).get('hits', [])
                
                # Extract hrmsms data exactly like JavaScript: hits[i]['inner_hits']['fullscan'].hits.hits.map(...)
                hrmsms_int = []
                hrmsms_mz = []
                
                for j, inner_hit in enumerate(inner_hits):
                    source = inner_hit.get('_source', {})
                    mz_data = source.get('hrmsms_mz', [])
                    int_data = source.get('hrmsms_int', [])
                    hrmsms_int.append(int_data)
                    hrmsms_mz.append(mz_data)
                
                # Check if we have valid MS/MS data (matching JavaScript condition)
                if hrmsms_int and hrmsms_mz and len(hrmsms_int) > 0 and len(hrmsms_int[0]) > 0 and len(hrmsms_mz[0]) > 0:
                    logger.info(f"Valid MS/MS data found for hit {i}, calling spectral similarity service...")
                    
                    # Flatten the arrays exactly like the R script expects
                    # The R script does: hrmsms_mz<-c(hrmsms_mz) and hrmsms_int<-c(hrmsms_int)
                    # This flattens nested lists into single vectors
                    flattened_mz = []
                    flattened_int = []
                    for mz_array, int_array in zip(hrmsms_mz, hrmsms_int):
                        flattened_mz.extend(mz_array)
                        flattened_int.extend(int_array)
                    
                    # Prepare payload exactly like JavaScript
                    payload = {
                        "exp_records": substance.exp_records,
                        "hrmsms_mz": flattened_mz,  # Send as flat array, not nested
                        "hrmsms_int": flattened_int  # Send as flat array, not nested
                    }
                    
                    try:
                        async with session.post(SPECTRAL_SIMILARITY_URL, json=payload) as response:
                            if response.status == 200:
                                response_text = await response.text()
                                # JavaScript does: JSON.parse(response.data)
                                result = json.loads(response_text)
                                
                                # Extract the numerical score from the R service response
                                # The R service returns a JSON string that needs to be parsed again
                                if isinstance(result, list) and len(result) > 0:
                                    # The result is a list containing a JSON string
                                    inner_result = json.loads(result[0])
                                    if 'result' in inner_result and len(inner_result['result']) > 0:
                                        score = inner_result['result'][0].get('score', 0)
                                        logger.info(f"Spectral similarity score: {score}")
                                        scores.append(float(score) * 100)  # Convert to percentage like other scores
                                    else:
                                        logger.info(f"No spectral similarity result found in response")
                                        scores.append(0)
                                else:
                                    logger.info(f"Unexpected spectral similarity response format")
                                    scores.append(0)
                            else:
                                logger.error(f"Spectral similarity service returned status {response.status}")
                                scores.append(0)
                    except Exception as e:
                        logger.error(f"Error calling spectral similarity service: {str(e)}")
                        scores.append(0)
                else:
                    logger.info('No hrmsms_mz and hrmsms_int available for spectral similarity.')
                    scores.append(0)
    
    except Exception as e:
        logger.error(f"Spectral similarity call failed: {str(e)}")
        scores = [0] * len(hits)
    
    return scores

async def call_genform(hits, substance, tolerance):
    """Call GenForm container for isotopic fit and molecular formula scoring"""
    scores = {"isotopic_fit": [], "molecular_formula_fit": []}
    
    try:
        for hit in hits:
            inner_hits = hit.get('inner_hits', {}).get('fullscan', {}).get('hits', {}).get('hits', [])
            
            if not inner_hits:
                scores["isotopic_fit"].append(None)
                scores["molecular_formula"].append(None)
                continue
                
            # Get the first inner hit data
            source = inner_hits[0].get('_source', {})
            
            # Calculate PPM tolerance
            ppm = (tolerance * 1000000) / substance.mz if substance.mz > 0 else 5
            
            # Prepare MS data
            request_ms = ""
            isotopes_mz = source.get('isotopes_mz', [])
            isotopes_int = source.get('isotopes_int', [])
            
            if isotopes_mz and len(isotopes_mz) > 0:
                for j in range(len(isotopes_mz)):
                    if j < len(isotopes_int):
                        request_ms += f"{isotopes_mz[j]} {isotopes_int[j]}\n"
            else:
                mz = source.get('mz', 0)
                max_intensity = source.get('max_intensity', 0)
                if mz > 0:
                    request_ms = f"{mz} {max_intensity}\n"
            
            # Prepare MS/MS data
            request_msms = ""
            hrmsms_mz = source.get('hrmsms_mz', [])
            hrmsms_int = source.get('hrmsms_int', [])
            
            if hrmsms_mz and len(hrmsms_mz) > 0:
                for j in range(len(hrmsms_mz)):
                    if j < len(hrmsms_int):
                        request_msms += f"{hrmsms_mz[j]} {hrmsms_int[j]}\n"
            
            # Get adduct (first one if available)
            adduct = substance.compound_adducts[0] if substance.compound_adducts else ""
            
            # Call GenForm container
            result = await call_genform_container(
                request_ms, 
                request_msms, 
                substance.compound_mol, 
                adduct, 
                ppm
            )
            
            if result:
                # Parse GenForm results
                isotopic_fit = result.get('isotopic_fit', None)
                molecular_formula_fit = result.get('molecular_formula_fit', None)
                
                # Convert string values to float if needed
                try:
                    isotopic_fit = float(isotopic_fit) if isotopic_fit is not None else None
                except (ValueError, TypeError):
                    isotopic_fit = None
                    
                try:
                    molecular_formula_fit = float(molecular_formula_fit) if molecular_formula_fit is not None else None
                except (ValueError, TypeError):
                    molecular_formula_fit = None
                
                scores["isotopic_fit"].append(round(isotopic_fit * 100, 2) if isotopic_fit is not None else None)
                scores["molecular_formula_fit"].append(round(molecular_formula_fit * 100, 2) if molecular_formula_fit is not None else None)
            else:
                scores["isotopic_fit"].append(None)
                scores["molecular_formula_fit"].append(None)
    
    except Exception as e:
        logger.error(f"GenForm ranking failed: {str(e)}")
        # Fill with None values for failed calls
        scores["isotopic_fit"] = [None] * len(hits)
        scores["molecular_formula_fit"] = [None] * len(hits)
    
    return scores

async def call_genform_container(ms_data, msms_data, compound, adduct, ppm):
    """Call the GenForm container with mass spectrometry data"""
    import subprocess
    import json
    import urllib.parse
    
    try:
        # Prepare data in the format expected by the Python script
        input_data = {
            "ms": urllib.parse.quote(ms_data),
            "msms": urllib.parse.quote(msms_data),
            "compound": compound,
            "adduct": adduct,
            "ppm": ppm
        }
        
        # Convert to JSON
        json_input = json.dumps(input_data)
        
        # Run the GenForm container with JSON input via stdin
        result = subprocess.run([
            "docker", "exec", "-i", "dsfp-genform", "python3", "/app/run_genform.py"
        ], input=json_input, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            # Parse JSON output
            output_data = json.loads(result.stdout.strip())
            return output_data
        else:
            logger.error(f"GenForm execution failed: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error("GenForm execution timed out")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse GenForm output: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"GenForm container call failed: {str(e)}")
        return None

async def call_semiquantification(hits, substance):
    """Call semiquantification service for concentration estimation"""
    try:
        logger.info("[Searching...] Starting semiquantification...")
        
        # Group hits by instrument setup_id (similar to JS reduce function)
        setups = {}
        for hit in hits:
            setup_id = hit['_source']['instrument_setup_used']['setup_id']
            if setup_id not in setups:
                setups[setup_id] = []
            setups[setup_id].append(hit)
        
        promises = []
        
        async with aiohttp.ClientSession() as session:
            for setup_id, setup_hits in setups.items():
                # Extract sample IDs and peak areas for this setup
                # Use _id field instead of _source.id
                sample_ids = [hit['_id'] for hit in setup_hits]
                peak_areas = []
                
                for hit in setup_hits:
                    # Get peak area from matches (similar to JS: sample['_source'].matches[0].peak_area)
                    inner_hits = hit.get('inner_hits', {}).get('fullscan', {}).get('hits', {}).get('hits', [])
                    if inner_hits:
                        # Use the first inner hit's peak_area
                        peak_area = inner_hits[0].get('_source', {}).get('peak_area', 0)
                        peak_areas.append(peak_area)
                    else:
                        peak_areas.append(0)
                
                # Prepare payload for semiquantification service
                payload = {
                    "sample_id": sample_ids,
                    "peak_areas": peak_areas,
                    "smilessuspect": substance.smiles,
                    "collection_id": setup_hits[0]['_source']['collection_id'],
                    "preconcentration": 1
                }
                
                logger.info(f"Sending semiquantification payload: {payload}")
                
                try:
                    async with session.post(SEMIQUANTIFICATION_URL, json=payload) as response:
                        if response.status == 200:
                            response_text = await response.text()
                            data = json.loads(response_text)
                            
                            result = {
                                "setup_id": setup_id,
                                "samples": sample_ids,
                                "concentrations": data.get('semiqconcentration', []),
                                "methods": data.get('semiqmethod', [])
                            }
                            promises.append(result)
                        else:
                            logger.error(f"Semiquantification service returned status {response.status}")
                            promises.append(None)
                            
                except Exception as e:
                    logger.error(f"Error calling semiquantification service: {str(e)}")
                    promises.append(None)
        
        # Map the results back to individual hits (similar to JS Promise.all.then logic)
        mapped_values = []
        if promises and promises[0] is not None:
            for hit in hits:
                hit_sample_id = hit['_id']  # Use _id instead of _source.id
                hit_setup_id = hit['_source']['instrument_setup_used']['setup_id']
                
                # Find the corresponding result for this hit
                for result in promises:
                    if result is not None and str(result['setup_id']) == str(hit_setup_id):
                        # Find this sample in the result
                        try:
                            sample_index = result['samples'].index(hit_sample_id)
                            
                            # Extract concentration and method
                            concentrations = result['concentrations']
                            methods = result['methods']
                            
                            if isinstance(concentrations, list) and sample_index < len(concentrations):
                                concentration = concentrations[sample_index]
                            else:
                                concentration = concentrations if not isinstance(concentrations, list) else None
                                
                            if isinstance(methods, list) and sample_index < len(methods):
                                method = methods[sample_index]
                            else:
                                method = methods if not isinstance(methods, list) else None
                            
                            mapped_values.append({
                                "method": method,
                                "concentration": concentration
                            })
                            break
                        except (ValueError, IndexError) as e:
                            logger.error(f"Error mapping semiquantification result: {str(e)}")
                            mapped_values.append(None)
                else:
                    # No result found for this hit
                    mapped_values.append(None)
        else:
            # No valid results
            mapped_values = [None] * len(hits)
        
        logger.info("Semiquantification is done...")
        return mapped_values
        
    except Exception as e:
        logger.error(f"Semiquantification call failed: {str(e)}")
        return [None] * len(hits)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)

def check_if_null(value):
    """Helper function to handle null values in score calculations"""
    if value is None:
        return 0
    return value

def ipscore(detection):
    """Calculate IP (Identification Point) score based on all available scores"""
    try:
        scores = detection.get('scores', {})
        
        # Ensure all score types are present with default values
        rti_score = check_if_null(scores.get('rti', 0))
        mz_score = check_if_null(scores.get('mz', 0))
        fragments_score = check_if_null(scores.get('fragments', 0))
        spectral_similarity_score = check_if_null(scores.get('spectral_similarity', 0))
        isotopic_fit_score = check_if_null(scores.get('isotopic_fit', 0))
        molecular_formula_fit_score = check_if_null(scores.get('molecular_formula_fit', 0))
        
        # Use the substance's exp_records to determine which coefficients to use
        # For simplicity, we'll check if spectral similarity > 0 as an indicator of experimental data availability
        has_experimental_data = spectral_similarity_score > 0
        
        if not has_experimental_data:
            # Coefficients when no experimental records are available
            ip_score = (0.1254 * isotopic_fit_score * 0.01 + 
                       0.1038 * molecular_formula_fit_score * 0.01 + 
                       0.0972 * rti_score * 0.01 + 
                       0.0846 * mz_score * 0.01 + 
                       0.135 * fragments_score * 0.01 + 
                       0.054 * spectral_similarity_score * 0.01)
        else:
            # Coefficients when experimental records are available
            ip_score = (0.15675 * isotopic_fit_score * 0.01 + 
                       0.12975 * molecular_formula_fit_score * 0.01 + 
                       0.1215 * rti_score * 0.01 + 
                       0.10575 * mz_score * 0.01 + 
                       0.16875 * fragments_score * 0.01 + 
                       0.0675 * spectral_similarity_score * 0.01)
        
        return ip_score
    
    except Exception as e:
        logger.error(f"Error calculating IP score: {str(e)}")
        return 0