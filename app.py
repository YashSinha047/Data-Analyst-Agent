import os
import io
import base64
import json
import traceback
import re
import sys
import subprocess
import tempfile
import asyncio
import signal
from functools import wraps
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Form
from typing import List, Any, Dict, Optional
from dotenv import load_dotenv
import anthropic

# --- Configuration ---
load_dotenv()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    raise RuntimeError("ANTHROPIC_API_KEY not found in .env file")

# Configure Claude
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# Constants
MAX_DEBUG_RETRIES = 3
MAX_ACQUISITION_RETRIES = 1  # New constant for data acquisition retries
DOCKER_EXECUTION_TIMEOUT = 180 # 3 minutes as requested
DOCKER_IMAGE_NAME = "analyst-sandbox"
MAX_PIPELINE_TIMEOUT = 300  # 5 minutes total timeout for entire pipeline

# --- 1. Secure Code Execution using Docker Sandbox ---
DOCKER_CONTAINER_NAME = "analyst-sandbox-running"

def ensure_docker_container_running(data_dir):
    """Ensure the persistent Docker container is running with /data mounted."""
    result = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name={DOCKER_CONTAINER_NAME}", "--format", "{{.Status}}"],
        capture_output=True, text=True
    )
    status = result.stdout.strip().lower()

    if not status:
        print(f"🚀 Starting persistent container '{DOCKER_CONTAINER_NAME}'...")
        subprocess.run([
            "docker", "run", "-dit",
            "--name", DOCKER_CONTAINER_NAME,
            "-v", f"{data_dir}:/data",  # mount host data_dir
            DOCKER_IMAGE_NAME, "bash"
        ], check=True)
    elif not status.startswith("up"):
        print(f"🔄 Starting stopped container '{DOCKER_CONTAINER_NAME}'...")
        subprocess.run(["docker", "start", DOCKER_CONTAINER_NAME], check=True)
    else:
        print(f"✅ Container '{DOCKER_CONTAINER_NAME}' is already running.")

def execute_python_code_docker(code: str, data_dir: str) -> dict:
    """Executes Python code in an isolated Docker container with a fresh mount each time."""
    # Use the timeout-enabled version with default timeout
    return execute_python_code_docker_with_timeout(code, data_dir, DOCKER_EXECUTION_TIMEOUT)

# --- Helper function to run pipeline with timeout ---
async def run_with_timeout(coro, timeout_seconds: int, fallback_question: str):
    """Run a coroutine with a timeout and return fallback response if timeout occurs."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        print(f"⏰ Pipeline timed out after {timeout_seconds} seconds")
        print("🔄 Generating fallback response due to timeout...")
        fallback_response = generate_fallback_response(fallback_question)
        print(f"✅ Timeout fallback response generated: {fallback_response}")
        return fallback_response
    except Exception as e:
        print(f"❌ Unexpected error in timeout wrapper: {str(e)}")
        fallback_response = generate_fallback_response(fallback_question)
        return fallback_response

# --- Helper function to execute with timeout ---
def execute_python_code_docker_with_timeout(code: str, data_dir: str, timeout: int = DOCKER_EXECUTION_TIMEOUT) -> dict:
    """Executes Python code in Docker with configurable timeout."""
    print("\n" + "="*60)
    print("🐍 EXECUTING GENERATED CODE IN A DOCKER SANDBOX 🐍")
    print(f"⏰ Timeout set to {timeout} seconds")
    print("-" * 60)

    script_filename = ""
    try:
        # Write the Python code to a file in data_dir
        with tempfile.NamedTemporaryFile(mode='w+', dir=data_dir, delete=False, suffix='.py', encoding='utf-8') as tmp_file:
            tmp_file.write(code)
            script_filename = os.path.basename(tmp_file.name)

        # Run container with data_dir mounted as /data
        docker_command = [
            "docker", "run", "--rm",
            "-v", f"{data_dir}:/data",
            DOCKER_IMAGE_NAME,
            "python", f"/data/{script_filename}"
        ]
        print(f"--- Executing Docker command: {' '.join(docker_command)} ---")

        process = subprocess.run(
            docker_command,
            capture_output=True,
            text=True,
            timeout=timeout
        )

        if process.returncode == 0:
            output = process.stdout.strip()
            print("✅ Docker execution SUCCESSFUL.")
            if not output:
                print("⚠️ WARNING: Script ran successfully but produced NO output.")
                return {
                    "output": None,
                    "error": "Script ran without errors but produced no output. The final print() statement is likely missing."
                }

            try:
                parsed_output = json.loads(output)
                if isinstance(parsed_output, dict) and "error" in parsed_output:
                    print(f"❌ Docker execution FAILED. Script returned a JSON error.")
                    print(f">>> CAPTURED ERROR:\n{parsed_output['error']}")
                    return {"output": None, "error": parsed_output['error']}
            except json.JSONDecodeError:
                pass

            print(f">>> CAPTURED OUTPUT (Truncated):\n{output[:1000]}...")
            return {"output": output, "error": None}
        else:
            error_output = process.stderr.strip()
            print(f"❌ Docker execution FAILED.")
            print(f">>> CAPTURED ERROR:\n{error_output}")
            return {"output": None, "error": error_output}

    except subprocess.TimeoutExpired:
        print(f"⏰ Docker command timed out after {timeout} seconds")
        return {"output": None, "error": f"Docker command timed out after {timeout} seconds"}
    except Exception as e:
        print(f"❌ Docker launch ERROR: {e}")
        if "No such file or directory" in str(e) or "not found" in str(e):
            return {
                "output": None,
                "error": f"Docker command failed. Is Docker installed and running? Have you built the '{DOCKER_IMAGE_NAME}' image?"
            }
        return {"output": None, "error": str(e)}
def generate_fallback_response(question: str) -> str:
    """Generate a fallback JSON response when analysis fails."""
    print("\n--- Generating Fallback Response ---")
    
    fallback_prompt = f"""
You are tasked with generating a valid JSON response structure based on the user's question/request, even though the actual analysis failed.

**Instructions:**
1. Analyze the user's request to understand what format they expect in the response
2. Generate a JSON response in the exact format they would expect if the analysis had succeeded
3. For data values, use placeholder values like "N/A", "Error: Data not available", null, or empty arrays/objects as appropriate
4. Maintain the exact structure they would expect but indicate that data is not available
5. Do not explain the failure - just provide the expected JSON structure with placeholder values

**User Request:**
{question}

**Your Fallback JSON Response:**
"""
    
    try:
        fallback_response = claude_client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=2000,
            messages=[{"role": "user", "content": fallback_prompt}]
        )
        
        response_text = fallback_response.content[0].text.strip()
        
        # Extract JSON from response
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            fallback_json = json_match.group(0)
            # Validate it's proper JSON
            json.loads(fallback_json)  # This will raise an exception if invalid
            print(f"✅ Fallback JSON Response Generated")
            return fallback_json
        else:
            # If no JSON found, create a basic error response
            return json.dumps({"error": "Analysis failed", "result": "Data not available"})
            
    except Exception as e:
        print(f"❌ Error generating fallback response: {e}")
        # Last resort fallback
        return json.dumps({"error": "Analysis failed", "result": "Data not available"})

# --- 2. Helper Function to Create File Previews ---
def create_file_previews(files: Dict[str, bytes]) -> str:
    preview_parts = []
    for name, content in files.items():
        preview_parts.append(f"--- File: {name} ---")
        try:
            text_content = content.decode('utf-8')
            lines = text_content.splitlines()
            preview_parts.append("\n".join(lines[:20]))
            if len(lines) > 20:
                preview_parts.append("... (file truncated)")
        except UnicodeDecodeError:
            # Check if it's an image file
            if name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp')):
                preview_parts.append(f"Image file, Size: {len(content)} bytes")
            else:
                preview_parts.append(f"Binary file, Size: {len(content)} bytes")
        preview_parts.append("-" * (len(name) + 10))
    return "\n".join(preview_parts)

# --- 2.5. Helper Function to Process Images with LLM ---
def process_images_with_llm(question: str, files: Dict[str, bytes]) -> Dict[str, Any]:
    """Process images using LLM to extract relevant data based on questions."""
    image_files = {name: content for name, content in files.items() 
                  if name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))}
    
    if not image_files:
        return {}
    
    image_analysis_results = {}
    
    for image_name, image_content in image_files.items():
        print(f"\n--- Processing Image: {image_name} with LLM ---")
        
        # Convert image bytes to base64 for LLM processing
        import base64
        image_base64 = base64.b64encode(image_content).decode('utf-8')
        
        image_analysis_prompt = f"""
You are an expert data analyst specializing in extracting structured information from images. 
Analyze the provided image and extract ALL relevant data that could help answer the user's questions.

**User Questions/Request:**
{question}

**Your Task:**
1. Examine the image carefully
2. Extract ALL numerical data, text data, tables, charts, or any structured information
3. If there are tables, extract them in a structured format (rows and columns)
4. If there are charts/graphs, extract the data points and values
5. If there's text, extract key information relevant to the questions
6. Return the extracted data in a structured JSON format

**Output Format:**
Return a JSON object with the following structure:
{{
    "extracted_data": {{
        "tables": [
            {{"table_name": "table1", "headers": ["col1", "col2"], "rows": [["val1", "val2"], ...]}},
            ...
        ],
        "charts": [
            {{"chart_type": "bar/line/pie", "title": "chart_title", "data": {{"labels": [...], "values": [...]}}}},
            ...
        ],
        "key_values": {{
            "metric1": "value1",
            "metric2": "value2",
            ...
        }},
        "text_content": "Any relevant text content...",
        "summary": "Brief summary of what was found in the image"
    }},
    "image_filename": "{image_name}"
}}

**Important:**
- Extract actual values, not placeholders
- Be precise with numbers and data
- If you can't extract certain data, indicate "not_available"
- Focus on data relevant to answering the user's questions
"""

        try:
            # Determine image media type
            image_extension = image_name.split('.')[-1].lower()
            media_type_map = {
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'png': 'image/png',
                'gif': 'image/gif',
                'bmp': 'image/bmp',
                'webp': 'image/webp'
            }
            media_type = media_type_map.get(image_extension, 'image/jpeg')
            
            response = claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4000,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": image_analysis_prompt
                            },
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": image_base64
                                }
                            }
                        ]
                    }
                ]
            )
            
            response_text = response.content[0].text
            
            # Extract JSON from response
            import re, json
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                image_data = json.loads(json_match.group(0))
                image_analysis_results[image_name] = image_data
                print(f"✅ Successfully extracted data from {image_name}")
            else:
                print(f"❌ Could not extract structured data from {image_name}")
                image_analysis_results[image_name] = {"error": "Could not parse LLM response"}
                
        except Exception as e:
            print(f"❌ Error processing image {image_name}: {str(e)}")
            image_analysis_results[image_name] = {"error": str(e)}
    
    return image_analysis_results

# --- 3. Main Analysis Pipeline ---
async def run_analysis_pipeline(question: str, files: Dict[str, bytes], data_dir: str) -> Any:
    """Runs the full, dynamic analysis pipeline using Claude models."""
   
    # --- STAGE 1: STRATEGIC PLANNING ---
    print("\n--- STAGE 1: Calling Chief Strategist LLM ---")
    file_previews = create_file_previews(files)
    
    # Check if we have images
    has_images = any(name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp')) 
                    for name in files.keys())
   
    strategist_prompt = f"""
You are a Chief Strategist for a data analysis agent. Your job is to analyze the user's request and decide the analysis approach.

**Decision Criteria:**
1. **Image Processing REQUIRED**: If there are image files that contain data relevant to answering the questions
2. **Data Scouting REQUIRED**: For local data files (CSV, JSON, PDF, Excel, TXT, etc.), web scraping, AND API data fetching that need structure analysis to understand their schema and content
3. **Data Scouting NOT REQUIRED**: Only for images (handled separately) and large pre-configured databases with known schemas (S3 data warehouses, established database connections)

**Scouting Rules:**
- **TRUE**: For all local structured data files like CSV, JSON, PDF, Excel, TXT, XML, etc., web scraping, AND API endpoints/JSON APIs - these need inspection to understand their structure and response format
- **FALSE**: Only for images (processed separately) and large pre-configured data warehouses with established schemas (S3 data lakes, pre-configured database connections)

**Important Notes:**
- API endpoints (like JSON APIs, REST APIs) ALWAYS need scouting to understand the response structure
- Web scraping ALWAYS needs scouting to understand the page structure
- Only exclude scouting for images and large enterprise data warehouses with known schemas

Your output must be a single, valid JSON object with these keys:
- "has_images": boolean (true if image files are present)
- "image_processing_required": boolean (true if images contain relevant data)
- "scouting_required": boolean (TRUE for local files/web scraping/APIs, FALSE only for images and pre-configured data warehouses)
- "data_source_type": string ("web", "database", "local_file", "images_only", or "mixed")

**User Request (from questions.txt):**
{question}

**Available Data File Previews:**
{file_previews}

**Has Images:** {has_images}

**Your JSON Decision:**
"""
   
    strategist_response = claude_client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=4000,
        messages=[{"role": "user", "content": strategist_prompt}]
    )
    raw_response_text = strategist_response.content[0].text
    json_match = re.search(r'\{.*\}', raw_response_text, re.DOTALL)
    if not json_match:
        raise ValueError("Strategist LLM did not return a valid JSON object.")
    strategy_decision_text = json_match.group(0)
    strategy = json.loads(strategy_decision_text)
    print(f"✅ Strategy Decided: {strategy}")

    # --- STAGE 1.5: IMAGE PROCESSING (if required) ---
    image_extracted_data = {}
    if strategy.get("image_processing_required", False):
        print("\n--- STAGE 1.5: Processing Images with LLM ---")
        image_extracted_data = process_images_with_llm(question, files)
        print(f"✅ Image Data Extracted: {len(image_extracted_data)} images processed")

    # --- STAGE 2: DATA ACQUISITION WITH ERROR HANDLING (for non-image files) ---
    actual_data_structure = "Not available. Plan must be based on the user's provided schema."
    if strategy.get("scouting_required", False):
        print("\n--- STAGE 2: Generating and Executing Data Acquisition Script ---")
        
        # Filter out image files for traditional data acquisition
        non_image_files = [name for name in files.keys() 
                          if not name.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'))]
        
        acquisition_success = False
        acquisition_attempt = 0
        
        while acquisition_attempt <= MAX_ACQUISITION_RETRIES and not acquisition_success:
            acquisition_attempt += 1
            print(f"--- Data Acquisition Attempt {acquisition_attempt} ---")
            
            # Calculate remaining time for acquisition timeout
            remaining_time = max(60, DOCKER_EXECUTION_TIMEOUT // 2)  # At least 1 minute, or half the default timeout
            print(f"⏰ Data acquisition timeout set to {remaining_time} seconds")
            
            acquisition_prompt = f"""
You are a data acquisition script generator. Based on the user's request, write a Python script to load a SMALL SAMPLE of the NON-IMAGE data and inspect its structure.

**Instructions:**
- If the data source is a web page, use `pandas.read_html()`.
- If the source is a CSV file, read it from the `/data/` directory (e.g., `pd.read_csv('/data/edges.csv', nrows=100)`).
- If the source is a PDF file, use one of these libraries to extract tables: `pymupdf`, `pypdf`, or `pdfplumber`. Read from `/data/filename.pdf`.
- For PDF files, extract ALL tables and concatenate them into a single DataFrame if there are multiple tables.
- For other types of files, use the most common and reliable Python libraries for that type.
- Get all the important data preview required for solving the questions. Like the column names, unique values in the categorical columns, and at max 50 rows of data or any other important information that will be used in the code and hence we need the correct names or values of that. So that we have enough data context.

**IMPORTANT:** 
- DO NOT process image files - they are handled separately
- Focus only on structured data files (CSV, JSON, PDF, etc.)
- The script MUST print structured dataset previews (`print("COLUMNS:", df.columns)` and `print("HEAD:", df.head().to_json(orient='split'))`)

CRITICAL - Your output must be ONLY the raw Python code inside ```python ... ``` tags, no explanation text.

**Available Non-Image Files:**
{non_image_files}

**User Request (from questions.txt):**
{question}

{"**Previous Acquisition Error:** " + str(acquisition_error) if acquisition_attempt > 1 else ""}

**Your Data Acquisition Script:**
"""
            
            acquisition_response = claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2000,
                messages=[{"role": "user", "content": acquisition_prompt}]
            )
            acquisition_code = acquisition_response.content[0].text.strip().lstrip("```python").rstrip("```")

            print(acquisition_code)
           
            # Use timeout-enabled execution for data acquisition
            acquisition_result = execute_python_code_docker_with_timeout(acquisition_code, data_dir, remaining_time)
            
            if acquisition_result["output"]:
                actual_data_structure = acquisition_result["output"]
                print(f"✅ Data Structure Captured:\n{actual_data_structure}")
                acquisition_success = True
            else:
                acquisition_error = acquisition_result["error"]
                print(f"❌ Data Acquisition Failed (Attempt {acquisition_attempt}): {acquisition_error}")
                
                if acquisition_attempt > MAX_ACQUISITION_RETRIES:
                    print(f"⚠️ Data acquisition failed after {MAX_ACQUISITION_RETRIES + 1} attempts. Continuing without data structure context...")
                    actual_data_structure = f"Data acquisition failed. Error: {acquisition_error}. Proceeding without detailed data structure."

    # --- STAGE 3: ANALYSIS PLANNING ---
    print("\n--- STAGE 3: Calling Analysis Planner with Full Context ---")
    
    # Prepare image data context for the planner
    image_data_context = ""
    if image_extracted_data:
        image_data_context = f"""
**EXTRACTED IMAGE DATA:**
The following data has been extracted from images using LLM analysis:
{json.dumps(image_extracted_data, indent=2)}

**Important:** This image data is already extracted and available as Python variables. DO NOT attempt to reprocess images.
"""

    analysis_planner_prompt = f"""
You are a **professional data scientist** tasked with creating a **final, detailed, and EFFICIENT execution plan**. 
You have been provided with the user's request and the exact structure of available data (both traditional data and image-extracted data).

{image_data_context}

Follow the instructions below **exactly**. The goal is to generate a Python plan that works on any dataset, cleans data correctly, and produces the requested outputs without errors.

---

**Special Rules for Image-Extracted Data:**
- Image data has already been processed and extracted by LLM analysis
- The extracted data is available in the `image_extracted_data` variable as a dictionary
- DO NOT generate code to process images - use the pre-extracted data directly
- Reference image data like: `image_extracted_data['image_name']['extracted_data']['tables'][0]`
- Combine image-extracted data with traditional data sources as needed

## 1. GENERAL RULES
- **Always clean data based on the EXPECTED data types** of the fields required to answer the question(s).
- **DO NOT** skip cleaning for any numeric, date/time, or boolean column — these can break code if left dirty.
- **DO NOT** assume NaN for invalid values; instead, clean them by removing unwanted characters according to their expected type, then convert.
- **DO NOT** use direct conversion functions (`astype`, `pd.to_numeric` without `errors='coerce'`, etc.) on raw data without cleaning first.
- **Work value-by-value** when cleaning: inspect, strip unwanted characters, then convert.

---

## 2. HTML TABLE PREPROCESSING (Dynamic Handling)
When working with HTML-sourced tables:
- **Parse HTML with BeautifulSoup before pandas.read_html()**.
- For each cell:
  - Preserve all **visible text content** from any tag (including `<a>`, `<span>`, `<i>`, etc.).
  - Remove only the HTML markup — not the text — unless the tag contains purely decorative or reference markers (like `<sup>` footnotes).
  - Reference or metadata tags (`<sup>`, citation links, inline notes) should be removed entirely **only if they do not contain meaningful data**.
- If unsure whether a tag's text is important:
  - Keep the text but remove only the tag.
- This ensures text columns like *Title* remain intact while numeric columns are free of reference markers.
- Never concatenate numbers from different elements unless they are part of the same logical number.

---

## 3. DATA CLEANING RULES BY DATATYPE

### 3.1 Numeric & Integer Columns
- Remove **all characters** except digits `0-9` and at most **one decimal point** `.`.
- Remove commas `,` from thousands separators.
- Remove currency symbols (`$`, `₹`, etc.) and any non-numeric characters.
- Examples:
  - `24RK` → `24`
  - `T$2,257,844,554` → `2257844554`
- Convert cleaned values to:
  - `int` if there is no decimal point
  - `float` if there is a decimal point.

### 3.2 Date/Time Columns
- Convert to datetime using `pd.to_datetime(..., errors='coerce')` after stripping whitespace and normalizing separators (`-`, `/`, `.`).
- Handle multiple formats where possible.
- Remove or flag impossible dates (e.g., year < 1900 or > current_year+10).

### 3.3 Boolean/Binary Columns
- Standardize truthy values (`yes`, `y`, `true`, `1`) → `True`
- Standardize falsy values (`no`, `n`, `false`, `0`) → `False`
- Ensure final dtype is boolean.

### 3.4 String/Text Columns
- Keep as-is unless explicitly required by the user request.
- If cleaning is required, only remove leading/trailing spaces and control characters.
- Never strip out meaningful content (e.g., movie titles, names) even if inside tags — unwrap instead.

---

## 4. EXECUTION PLAN STRUCTURE
When generating the plan:
1. **Understand the Goal**
   - Identify the exact outputs (e.g., JSON object, array, charts) from the user request.

2. **Assess the Data**
   - Use extracted image data directly from `image_extracted_data` variable
   - If `ACTUAL Data Structure` is provided for traditional data, use it to determine exact column names and types.
   - Otherwise, infer from the request and sample data.

3. **Select the Right Tools**
   - Use **pandas** for local or scraped data.
   - Use **DuckDB** only if explicitly mentioned, or if data is from SQL/S3 sources.
   - For plots, default to `matplotlib` (and `seaborn` if needed).

4. **Formulate the Plan**
   - Include **data loading** (both traditional and image data), **HTML pre-cleaning** (if scraping), **data cleaning** (following rules above), **analysis**, and **output formatting**.
   - Ensure each cleaning step specifies which columns are affected and how.

5. **Efficiency Mandate**
   - For large remote datasets: push as much filtering and cleaning into the SQL/DuckDB query as possible.
   - Do **NOT** load the entire dataset into pandas before filtering.

---

## 5. DUCKDB-SPECIFIC RULES (if applicable)
- For date parsing: `TRY_STRPTIME(date_column, '%d-%m-%Y')`
- For date differences: cast to `DATE` and subtract directly.
- Avoid slow functions like `julianday`.

---

## 6. OUTPUT RULES
- Your output must contain **ONLY Python code**, no explanations or commentary.
- Do not start with "Here's a script".
- The plan must be clear, step-by-step, and directly executable.

---

**User Request (from questions.txt):**
{question}

**ACTUAL Data Structure (traditional data, if available):**
{actual_data_structure}

**CRITICAL: You MUST use the EXACT column names, data types, and values shown in the ACTUAL Data Structure above. Do not assume or guess - use only what is explicitly shown in the preview data.**

**Your Final, EFFICIENT Execution Plan:**
"""

    final_plan_response = claude_client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=4000,
        messages=[{"role": "user", "content": analysis_planner_prompt}]
    )
    execution_plan = final_plan_response.content[0].text
    print(f"✅ Final Execution Plan Generated:\n{execution_plan}")

    # --- STAGE 4: FINAL EXECUTION & DEBUGGING WITH FALLBACK ---
    print("\n--- STAGE 4: Calling Coder LLM to Generate Final Script ---")
   
    generated_code = ""
    last_error = ""
    
    try:
        for attempt in range(MAX_DEBUG_RETRIES):
            print(f"--- Attempt {attempt + 1} of {MAX_DEBUG_RETRIES} ---")
            
            # Calculate remaining timeout for each attempt - progressively shorter
            base_timeout = DOCKER_EXECUTION_TIMEOUT
            attempt_timeout = max(60, base_timeout - (attempt * 30))  # Reduce timeout by 30s each attempt, minimum 60s
            print(f"⏰ Execution timeout for attempt {attempt + 1}: {attempt_timeout} seconds")
           
            coder_prompt = ""
            if attempt == 0:
                coder_prompt = f"""
You are an expert Python programmer. Write a single, self-contained Python script to strictly follow the provided execution plan.
**CRITICAL RULES:**
1.  Your output MUST be ONLY the raw Python code inside ```python ... ``` tags. Do not include explanations.
2.  The script must be a complete, runnable program that performs all steps from the plan.
3.  When reading local files, you MUST use the absolute path `/data/filename.csv`. For example: `pd.read_csv('/data/sample-sales.csv')`.
4.  The script's final output must be printed to standard output in the exact format requested by the user's original question (e.g., a JSON object or array), containing ONLY the answers, not the questions.
5.  **Your response must contain ONLY Python code, no explanations**
6.  **DO NOT start with "Here's a script" or any explanatory text** 
7.  **While cleaning the data do not assume NaN if the data is not cleaned and have some characters other than the expected data type.**
8.  **CRITICAL: Use ONLY the exact column names, values, and data types shown in the ACTUAL Data Structure. Do not assume or guess country codes, column names, or data formats.**

**Image Data Handling:**
- Image data has been pre-processed and is available in the `image_extracted_data` variable
- DO NOT import image processing libraries or attempt to process images
- Access image data like: `image_extracted_data['image_name']['extracted_data']`
- The image_extracted_data structure is:
{json.dumps(image_extracted_data, indent=2) if image_extracted_data else "No image data available"}

**Final Execution Plan to Follow:**
{execution_plan}

**Your Final Python Script:**
"""
            else:
                print("--- Code failed. Calling LLM in Debug Mode. ---")
                coder_prompt = f"""
You are an expert Python debugger. The previous attempt failed. Analyze the original user request, the execution plan, the faulty code, and the error message. Then, provide a corrected, complete Python script.
Think step-by-step about what went wrong and how to fix it, then provide the full, corrected code inside ```python ... ``` tags.
1.  **Your response must contain ONLY Python code, no explanations**
2.  **DO NOT start with "Here's a script" or any explanatory text** 
3.  **While cleaning the data do not assume NaN if the data is not cleaned and have some characters other than the expected data type.**
4.  ** DO NOT TRY TO DOWNLOAD THE DATA FROM A DATABASE IF THE DATA IS VERY LARGE**
5.  **CRITICAL: Use ONLY the exact column names, values, and data types from the ACTUAL Data Structure. Pay special attention to country codes, date formats, and column names shown in the preview data.**
**CRITICAL RULE FOR FILE PATHS:** If the error is a `FileNotFoundError`, it is because the script is not using the correct absolute path. All data files are in the `/data/` directory inside the Docker container. You MUST correct the code to read files from this path (e.g., `pd.read_csv('/data/sample-sales.csv')`).

**ACTUAL Data Structure Available:**
{actual_data_structure}

**IMPORTANT: The above data structure shows the EXACT format of the data. Use the exact country codes, column names, and data types shown. Do not assume different formats.**

**Original User Request (from questions.txt):**
{question}

**Final Execution Plan:**
{execution_plan}

**Faulty Code:**
```python
{generated_code}
```

**Error Message:**
{last_error}

**Your Corrected, Full Python Script:**
"""
           
            coder_response = claude_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4000,
                messages=[{"role": "user", "content": coder_prompt}]
            )
            generated_code = coder_response.content[0].text.strip().lstrip("```python").rstrip("```")
            
            # Inject the image data at the beginning of the script
            if image_extracted_data:
                image_data_injection = f"""
# Pre-extracted image data (processed by LLM)
import json
image_extracted_data = {json.dumps(image_extracted_data, indent=2)}

"""
                generated_code = image_data_injection + generated_code
           
            print(f"✅ Final Script Generated (Attempt {attempt + 1}):\n--- SCRIPT START ---\n{generated_code}\n--- SCRIPT END ---")

            # Use timeout-enabled execution with progressive timeout reduction
            execution_result = execute_python_code_docker_with_timeout(generated_code, data_dir, attempt_timeout)
           
            if execution_result["output"]:
                print("\n🎉 --- PIPELINE SUCCESSFUL --- 🎉")
                return execution_result["output"]
            else:
                last_error = execution_result["error"]
                if attempt == MAX_DEBUG_RETRIES - 1:
                    print(f"\n❌ --- PIPELINE FAILED AFTER {MAX_DEBUG_RETRIES} ATTEMPTS --- ❌")
                    # Instead of raising exception, generate fallback response
                    print("🔄 Generating fallback response...")
                    fallback_response = generate_fallback_response(question)
                    print(f"✅ Fallback response generated: {fallback_response}")
                    return fallback_response

    except Exception as e:
        # Handle any unexpected errors in the execution pipeline
        print(f"❌ Unexpected error in execution pipeline: {str(e)}")
        print("🔄 Generating fallback response due to unexpected error...")
        fallback_response = generate_fallback_response(question)
        print(f"✅ Fallback response generated: {fallback_response}")
        return fallback_response

    # This should never be reached, but adding as safety net
    print("🔄 Generating fallback response as safety net...")
    return generate_fallback_response(question)

# --- 5. API Server Definition ---
app = FastAPI(title="Data Analyst Agent API")

@app.post("/api/")
async def analyze_data(request: Request):
    """Accepts a task description and files with flexible naming, then performs analysis."""
    print("\n\n--- [START] New Request Received ---")
    
    questions_content = None
    
    try:
        # Parse the multipart form data
        form = await request.form()
        
        # Find the questions.txt file
        other_files = {}
        
        for field_name, file_data in form.items():
            if hasattr(file_data, 'filename') and hasattr(file_data, 'read'):
                # This is a file upload
                filename = file_data.filename
                file_content = await file_data.read()
                
                if filename == "question.txt":
                    questions_content = file_content.decode('utf-8')
                    print(f"✔️ Main Request File '{filename}' processed.")
                else:
                    other_files[filename] = file_content
                    print(f"✔️ Additional file '{filename}' processed.")
        
        # Validate that questions.txt was provided
        if questions_content is None:
            # Generate fallback response even for missing question file
            fallback_response = generate_fallback_response("No question provided")
            return json.loads(fallback_response)
        
        print(f"Question Content:\n{questions_content}")
        
        if other_files:
            print(f"✔️ {len(other_files)} additional file(s) processed: {list(other_files.keys())}")

        # Create temporary directory and write files
        with tempfile.TemporaryDirectory() as tmpdir:
            for name, content in other_files.items():
                full_path = os.path.join(tmpdir, name)
                with open(full_path, 'wb') as f:
                    f.write(content)
           
            print(f"✔️ Data files temporarily written to: {tmpdir}")

            # Run the analysis pipeline
            result_str = await run_analysis_pipeline(questions_content, other_files, tmpdir)
       
        print("✅ Analysis finished successfully.")
        print("--- [END] Sending Final Response ---")
        
        # Try to parse as JSON, otherwise return as string wrapped in JSON
        try:
            return json.loads(result_str)
        except json.JSONDecodeError:
            return {"result": result_str}

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"❌ An error occurred during the request:\n{error_trace}")
        print("--- [END] Generating Fallback Response Due to Server Error ---")
        
        # Generate fallback response for any server-level errors
        fallback_question = questions_content if questions_content else "Server error occurred"
        fallback_response = generate_fallback_response(fallback_question)
        
        try:
            return json.loads(fallback_response)
        except json.JSONDecodeError:
            # Last resort - return basic error structure
            return {"error": "Server error occurred", "result": "Analysis could not be completed"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
