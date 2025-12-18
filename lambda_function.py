import json
import boto3
import urllib.request
import urllib.parse
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.error import URLError, HTTPError

# --- 환경 변수 설정 ---
LAMBDA_BASE_URL = os.environ.get("LAMBDA_BASE_URL")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID")

# [수정됨] DB 접속 정보 (DB_PASS -> DB_PASSWORD)
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
# Lambda 설정에 맞춰 DB_PASSWORD로 변경
DB_PASSWORD = os.environ.get("DB_PASSWORD") 
DB_PORT = os.environ.get("DB_PORT", "5432")

EXTRACTION_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

bedrock = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")

# --- 정적 설정 (Tool Config) ---
TOOL_CONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "search_subway_facility",
                "description": "지하철 역의 편의시설 정보를 검색합니다.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "station": {
                                "type": "string",
                                "description": "지하철 역 이름 (예: 서울역).",
                            },
                            "facility": {
                                "type": "string",
                                "enum": [
                                    "chargers", "elevators", "escalators",
                                    "helpers", "safe-platforms", "sign-phones",
                                    "lifts", "toilets", "movingwalks",
                                ],
                                "description": "특정 편의시설을 묻는 경우 해당 코드",
                            },
                        },
                        "required": [],
                    }
                },
            }
        }
    ]
}

# --- 정적 파일 로드 ---
STATION_MAP = {}

def normalize_station_name(name):
    if not name: return ""
    clean_name = name.strip()
    if clean_name.endswith("역"):
        return clean_name[:-1]
    return clean_name

try:
    with open("station_name_code.json", "r", encoding="utf-8") as f:
        station_list = json.load(f)
        for item in station_list:
            raw_name = item.get("name", "")
            code = item.get("station_cd")
            if raw_name and code:
                clean_key = normalize_station_name(raw_name)
                STATION_MAP[clean_key] = code
    print(f"Station map loaded: {len(STATION_MAP)} stations mapped.")
except Exception as e:
    print(f"Error loading station_name_code.json: {e}")


def get_station_code(name):
    if not name: return None
    target_name = normalize_station_name(name)
    return STATION_MAP.get(target_name, target_name)


def query_db_summary(station_name):
    """DB에서 해당 역의 편의시설 요약 정보(개수)를 조회합니다."""
    
    # [수정됨] 유효성 검사 변수명 변경 (DB_PASS -> DB_PASSWORD)
    if not (DB_HOST and DB_USER and DB_PASSWORD):
        print("ERROR: DB configuration missing.")
        print(f"DEBUG: Current Env Keys: {list(os.environ.keys())}")
        return None

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD, # [수정됨]
            port=DB_PORT,
            connect_timeout=3
        )
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sql = """
                SELECT 
                    station_name,
                    charger_count,
                    elevator_count,
                    escalator_count,
                    lift_count,
                    movingwalk_count,
                    safe_platform_count,
                    sign_phone_count,
                    toilet_count,
                    helper_count,
                    total_facility_count
                FROM subway_facility_total
                WHERE station_name = %s
            """
            cur.execute(sql, (station_name,))
            result = cur.fetchone() 
            
            if result:
                for key, value in result.items():
                    if hasattr(value, 'to_integral_value'):
                        result[key] = int(value)
                return dict(result)
            else:
                return None

    except Exception as e:
        print(f"DB Query Error: {e}")
        return {"error": "DB connection failed"}
    finally:
        if conn:
            conn.close()


def call_subway_api(endpoint):
    if not LAMBDA_BASE_URL: return {"error": "Server config error"}
    base = LAMBDA_BASE_URL.rstrip("/")
    path = endpoint.lstrip("/")
    url = f"{base}/{path}"
    
    try:
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                return json.loads(response.read().decode("utf-8"))
            return {"error": f"Status {response.status}"}
    except Exception as e:
        return {"error": str(e)}


def extract_intent(user_question):
    try:
        response = bedrock.converse(
            modelId=EXTRACTION_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": user_question}]}],
            toolConfig=TOOL_CONFIG,
            inferenceConfig={"temperature": 0},
        )
        output = response["output"]["message"]["content"]
        for content in output:
            if "toolUse" in content:
                return content["toolUse"]["input"]
        return {}
    except Exception:
        return {}


def lambda_handler(event, context):
    try:
        if isinstance(event.get("body"), str):
            body = json.loads(event.get("body", "{}"))
        else:
            body = event.get("body", {})
        user_question = body.get("question", "")

        if not user_question:
            return {"statusCode": 400, "body": json.dumps({"error": "No question"})}

        intent_data = extract_intent(user_question)
        target_station_raw = intent_data.get("station")
        target_facility = intent_data.get("facility")
        
        api_context = {}

        if target_station_raw:
            normalized_name = normalize_station_name(target_station_raw)
            station_code = get_station_code(normalized_name)
            
            print(f"Target: {normalized_name}, Code: {station_code}")

            # DB 조회
            db_summary = query_db_summary(normalized_name)
            if db_summary:
                api_context["facility_counts_summary"] = db_summary
            else:
                api_context["facility_counts_summary"] = "DB 데이터 없음"

            # API 조회
            encoded_code = urllib.parse.quote(str(station_code))
            station_info = call_subway_api(f"/stations/{encoded_code}")
            api_context["station_status_info"] = station_info

            if target_facility:
                facility_info = call_subway_api(f"/{target_facility}/{encoded_code}")
                api_context["target_facility_detail"] = facility_info

        else:
            api_context = {"info": "질문에서 역 이름을 찾을 수 없습니다."}

        system_prompt = """
        <system_prompt>
            <role>
                당신은 'kindMap'의 고객 지원 AI입니다. 
                1. 'facility_counts_summary': 시설 개수 정보 (DB)
                2. 'target_facility_detail': 실시간 상태/고장 정보 (API)
            </role>

            <instructions>
                1. "어떤 시설이 있어?" -> [facility_counts_summary]의 개수를 안내하세요.
                2. 교통약자 도우미의 경우 단위 '명'을 사용하고 반드시 연락처를 함께 안내하세요.
                3. "고장났어?" -> [target_facility_detail]의 'oprtng_situ'를 확인하세요.
                4. 데이터가 없으면 "정보가 없습니다"라고 하세요.
                5. 친절한 해요체로 3~4문장 답변하세요.
            </instructions>
        </system_prompt>
        """

        final_prompt = f"""
        [API Data]
        {json.dumps(api_context, ensure_ascii=False)}

        [User Question]
        {user_question}
        """

        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "system": system_prompt,
            "messages": [{"role": "user", "content": [{"type": "text", "text": final_prompt}]}],
        }

        response = bedrock.invoke_model(modelId=BEDROCK_MODEL_ID, body=json.dumps(payload))
        answer = json.loads(response.get("body").read())["content"][0]["text"]

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"answer": answer}, ensure_ascii=False)
        }

    except Exception as e:
        print(f"Critical Error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}