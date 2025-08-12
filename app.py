import os
import io
import base64
import json
import traceback
import re
import sys
import subprocess
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Form
from typing import List, Any, Dict, Optional
from dotenv import load_dotenv
import anthropic

# --- Configuration ---
load_dotenv()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
if not ANTHROPIC_API_KEY:
    raise RuntimeError("ANTHROPIC_API_KEY not found in .env file")

# Use the async client as we are in an async FastAPI environment
anthropic_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# Constants
MAX_DEBUG_RETRIES = 4
DOCKER_EXECUTION_TIMEOUT = 180 # 3 minutes as requested
DOCKER_IMAGE_NAME = "analyst-sandbox"
ANTHROPIC_MODEL = "claude-3-5-sonnet-20241022" # Using a powerful and fast model
# ANTHROPIC_MODEL = "claude-sonnet-4-20250514"
# --- 1. Secure Code Execution using Docker Sandbox ---
def execute_python_code_docker(code: str, data_dir: str) -> dict:
    """Executes Python code in an isolated Docker container."""
    print("\n" + "="*60)
    print("🐍 EXECUTING GENERATED CODE IN A DOCKER SANDBOX 🐍")
    print("-" * 60)
   
    script_filename = ""
    try:
        with tempfile.NamedTemporaryFile(mode='w+', dir=data_dir, delete=False, suffix='.py', encoding='utf-8') as tmp_file:
            tmp_file.write(code)
            script_filename = os.path.basename(tmp_file.name)

        docker_command = ["docker", "run", "--rm", "-v", f"{data_dir}:/data", DOCKER_IMAGE_NAME, "python", f"/data/{script_filename}"]
        print(f"--- Executing Docker command: {' '.join(docker_command)} ---")

        process = subprocess.run(docker_command, capture_output=True, text=True, timeout=DOCKER_EXECUTION_TIMEOUT)

        if process.returncode == 0:
            output = process.stdout.strip()
            print("✅ Docker execution SUCCESSFUL.")
            if not output:
                print("⚠️ WARNING: Script ran successfully but produced NO output.")
                return {"output": None, "error": "Script ran without errors but produced no output. The final print() statement is likely missing."}
           
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
    except Exception as e:
        print(f"❌ Docker launch ERROR: {e}")
        if "No such file or directory" in str(e) or "not found" in str(e):
            return {"output": None, "error": f"Docker command failed. Is Docker installed and running? Have you built the '{DOCKER_IMAGE_NAME}' image?"}
        return {"output": None, "error": str(e)}

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
            preview_parts.append(f"Binary file, Size: {len(content)} bytes")
        preview_parts.append("-" * (len(name) + 10))
    return "\n".join(preview_parts)

# --- 3. Main Analysis Pipeline ---
async def run_analysis_pipeline(question: str, files: Dict[str, bytes], data_dir: str) -> Any:
    """Runs the full, dynamic analysis pipeline using Anthropic models."""
   
    # --- STAGE 1: STRATEGIC PLANNING ---
    print("\n--- STAGE 1: Calling Chief Strategist LLM ---")
    file_previews = create_file_previews(files)
   
    strategist_prompt = f"""
You are a Chief Strategist for a data analysis agent. Your job is to analyze the user's request (`questions.txt`) and decide if a data scouting step is necessary.

**Decision Criteria:**
1.  **Scouting REQUIRED**: If the data source is a web page to be scraped OR an local file (like a CSV, json, pdf, image, etc). The goal is to get column names and a data preview.
2.  **Scouting NOT REQUIRED**: If the `questions.txt` describes a database (like DuckDB on S3) AND provides a clear schema (column names and data types). In this case, we can trust the provided schema and plan the analysis directly.

Your output must be a single, valid JSON object with two keys, and nothing else:
- "scouting_required": boolean (true or false)
- "data_source_type": string ("web", "database", or "local_file")

**User Request (from questions.txt):**
{question}

**Available Data File Previews:**
{file_previews}

**Your JSON Decision:**
"""
   
    strategist_message = await anthropic_client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": strategist_prompt}]
    )
    raw_response_text = strategist_message.content[0].text
    json_match = re.search(r'\{.*\}', raw_response_text, re.DOTALL)
    if not json_match:
        raise ValueError("Strategist LLM did not return a valid JSON object.")
    strategy_decision_text = json_match.group(0)
    strategy = json.loads(strategy_decision_text)
    print(f"✅ Strategy Decided: {strategy}")

    actual_data_structure = "Not available. Plan must be based on the user's provided schema."
    if strategy.get("scouting_required", True):
        # --- STAGE 2 (Conditional): DATA ACQUISITION ---
        print("\n--- STAGE 2: Generating and Executing Data Acquisition Script ---")
        acquisition_prompt = f"""
You are a data acquisition script generator. Based on the user's request, write a Python script to load a SMALL SAMPLE of the data and inspect its structure.

**Instructions:**
- If the data source is a web page, use `pandas.read_html()`.
- If the source is a local file, read it from the `/data/` directory (e.g., `pd.read_csv('/data/edges.csv', nrows=100)`).
- The script MUST print the DataFrame's columns (`print("COLUMNS:", df.columns)`) and the first 5 rows as JSON (`print("HEAD:", df.head().to_json(orient='split'))`).
CRITICAL - Your output must be ONLY the raw Python code inside ```python ... ``` tags, no other explanation text just give the code.
- DO NOT TRY TO CLEAN THE DATA, ONLY GENERATE THE CODES WHICH SHOWS THE PREVIEW OF THE DATA. NO NEED OF DOING ANY STATISTICAL OR VISUAALIZATION ANALYSIS


**User Request (from questions.txt):**
{question}

**Your Data Acquisition Script:**
"""
        acquisition_message = await anthropic_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": acquisition_prompt}]
        )
        acquisition_code = acquisition_message.content[0].text.strip().lstrip("```python").rstrip("```")

        print(acquisition_code)
       
        acquisition_result = execute_python_code_docker(acquisition_code, data_dir)
        if acquisition_result["error"]:
            raise Exception(f"Data acquisition script failed, cannot proceed. Error: {acquisition_result['error']}")
       
        actual_data_structure = acquisition_result["output"]
        print(f"✅ Data Structure Captured:\n{actual_data_structure}")

    # --- STAGE 3: ANALYSIS PLANNING ---
    print("\n--- STAGE 3: Calling Analysis Planner with Full Context ---")
    analysis_planner_prompt = f"""
You are a **professional data scientist** tasked with creating a **final, detailed, and EFFICIENT execution plan**. 
You have been provided with the user's request and, if available, the exact structure of the data.

Follow the instructions below **exactly**. The goal is to generate a Python plan that works on any dataset, cleans data correctly, and produces the requested outputs without errors.

---

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
- If unsure whether a tag’s text is important:
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
   - If `ACTUAL Data Structure` is provided, use it to determine exact column names and types.
   - Otherwise, infer from the request and sample data.

3. **Select the Right Tools**
   - Use **pandas** for local or scraped data.
   - Use **DuckDB** only if explicitly mentioned, or if data is from SQL/S3 sources.
   - For plots, default to `matplotlib` (and `seaborn` if needed).

4. **Formulate the Plan**
   - Include **data loading**, **HTML pre-cleaning** (if scraping), **data cleaning** (following rules above), **analysis**, and **output formatting**.
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

**ACTUAL Data Structure (if available):**
{actual_data_structure}

**Your Final, EFFICIENT Execution Plan:**
"""



    final_plan_message = await anthropic_client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": analysis_planner_prompt}]
    )
    execution_plan = final_plan_message.content[0].text
    print(f"✅ Final Execution Plan Generated:\n{execution_plan}")

    # --- STAGE 4: FINAL EXECUTION & DEBUGGING ---
    print("\n--- STAGE 4: Calling Coder LLM to Generate Final Script ---")
   
    generated_code = ""
    last_error = ""
    for attempt in range(MAX_DEBUG_RETRIES):
        print(f"--- Attempt {attempt + 1} of {MAX_DEBUG_RETRIES} ---")
       
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
4.  ** DO NOT TRY TO DOWNLOAD THE DATA FROM A DATABASE OF THE DATA IS VERY LARGE
**CRITICAL RULE FOR FILE PATHS:** If the error is a `FileNotFoundError`, it is because the script is not using the correct absolute path. All data files are in the `/data/` directory inside the Docker container. You MUST correct the code to read files from this path (e.g., `pd.read_csv('/data/sample-sales.csv')`).

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
       
        coder_message = await anthropic_client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": coder_prompt}]
        )
        generated_code = coder_message.content[0].text.strip().lstrip("```python").rstrip("```")
       
        print(f"✅ Final Script Generated (Attempt {attempt + 1}):\n--- SCRIPT START ---\n{generated_code}\n--- SCRIPT END ---")

        execution_result = execute_python_code_docker(generated_code, data_dir)
       
        if execution_result["output"]:
            print("\n🎉 --- PIPELINE SUCCESSFUL --- 🎉")
            return execution_result["output"]
        else:
            last_error = execution_result["error"]
            if attempt == MAX_DEBUG_RETRIES - 1:
                print(f"\n❌ --- PIPELINE FAILED AFTER {MAX_DEBUG_RETRIES} ATTEMPTS --- ❌")
                raise Exception(f"Failed to generate working code. Last error: {last_error}")

    raise Exception("Pipeline failed to produce a result.")

# --- 5. API Server Definition ---
app = FastAPI(title="Data Analyst Agent API")

@app.post("/api/")
async def analyze_data(request: Request):
    """Accepts a task description and files with flexible naming, then performs analysis."""
    print("\n\n--- [START] New Request Received ---")
    
    try:
        # Parse the multipart form data
        form = await request.form()
        
        # Find the questions.txt file
        questions_content = None
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
            raise HTTPException(status_code=400, detail="question.txt file is required")
        
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
        
        # Try to parse as JSON, otherwise return as string
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
        print("--- [END] Sending Error Response ---")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
