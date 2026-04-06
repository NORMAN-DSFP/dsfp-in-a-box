#!/usr/bin/env python3

import json
import sys
import os
import subprocess
import tempfile
import uuid
from datetime import datetime
import urllib.parse
import re

def process_adduct(adduct):
    """Process adduct format like the JavaScript version"""
    # Remove brackets
    adduct = adduct.replace('[', '').replace(']', '')
    
    if adduct not in ['M+', 'M-']:
        # Remove trailing + or - if present
        if adduct.endswith('-') or adduct.endswith('+'):
            adduct = adduct[:-1]
    else:
        # Convert M+ and M- to full format
        if adduct == 'M+':
            adduct = 'M+H'
        elif adduct == 'M-':
            adduct = 'M-H'
    
    return adduct

def run_genform(command, run_number, compound):
    """Run GenForm command and parse output"""
    print(f"Run #{run_number + 1}: {command}", file=sys.stderr)
    
    if run_number >= 4:
        return None
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=60)
        stdout = result.stdout
        stderr = result.stderr
        
        if stderr:
            print(f"GenForm stderr: {stderr}", file=sys.stderr)
        
        lines = stdout.split('\n')
        
        # Check if output contains valid/total indicator
        if len(lines) >= 2 and 'valid/total' in lines[-2]:
            # Remove header lines
            lines = lines[2:]
            
            # Look for matching compound
            for line in lines[:-2]:  # Exclude last 2 lines
                if not line.strip():
                    continue
                    
                data = line.split('\t')
                if len(data) > 0 and f"{compound} " in data[0]:
                    # Found matching compound
                    if len(data) > 3:
                        return {
                            "isotopic_fit": data[2],
                            "molecular_formula_fit": data[3]
                        }
                    else:
                        return {
                            "isotopic_fit": data[2],
                            "molecular_formula_fit": None
                        }
            
            return None
        
        return None
        
    except subprocess.TimeoutExpired:
        print("GenForm command timed out", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error running GenForm: {e}", file=sys.stderr)
        return None

def main():
    try:
        # Read JSON input from stdin
        input_data = json.loads(sys.stdin.read())
        
        # Extract parameters
        compound = input_data['compound']
        ms_data = urllib.parse.unquote(input_data['ms'])
        msms_data = urllib.parse.unquote(input_data['msms'])
        adduct = input_data['adduct']
        ppm = float(input_data['ppm'])
        
        # Generate unique filenames
        timestamp = str(int(datetime.now().timestamp() * 1000))
        unique_id = str(uuid.uuid4())
        base = f"input_{timestamp}_{compound}_{unique_id}"
        
        # Create temporary directory
        temp_dir = "/tmp/genform/"
        os.makedirs(temp_dir, exist_ok=True)
        
        ms_file = os.path.join(temp_dir, f"{base}_ms.txt")
        msms_file = os.path.join(temp_dir, f"{base}_msms.txt")
        
        try:
            # Write temporary files
            with open(ms_file, 'w') as f:
                f.write(ms_data)
            
            with open(msms_file, 'w') as f:
                f.write(msms_data)
            
            # Process adduct
            processed_adduct = process_adduct(adduct)
            
            # Remove numbers from compound for element specification
            elements = re.sub(r'[0-9]', '', compound)
            
            # Build GenForm command like the JavaScript version
            command = (f"/opt/genform/GenForm "
                      f"ms={ms_file} "
                      f"msms={msms_file} "
                      f"out= "
                      f"ion={processed_adduct} "
                      f"exist=TRUE "
                      f"el={elements} "
                      f"ppm={ppm} "
                      f"acc={ppm * 2} "
                      f"rej={ppm * 2}")
            
            # Try running GenForm (up to 2 attempts like JavaScript version)
            result = run_genform(command, 0, compound)
            
            if result is None:
                result = run_genform(command, 1, compound)
            
            if result is not None:
                print(json.dumps(result))
            else:
                print(json.dumps({"error": "GenForm did not return valid results"}))
                sys.exit(1)
        
        finally:
            # Clean up temporary files
            try:
                if os.path.exists(ms_file):
                    os.unlink(ms_file)
                if os.path.exists(msms_file):
                    os.unlink(msms_file)
            except Exception as e:
                print(f"Error cleaning up files: {e}", file=sys.stderr)
    
    except json.JSONDecodeError as e:
        print(f"Invalid JSON input: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyError as e:
        print(f"Missing required parameter: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()