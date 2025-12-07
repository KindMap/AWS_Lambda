import json
import boto3
import urllib.request
import urllib.parse
import os
import re
from urllib.error import URLError, HTTPError

# --- 환경 변수 및 클라이언트 설정 ---
LAMBDA_BASE_URL = os.environ.get("LAMBDA_BASE_URL")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID")
EXTRACTION_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"

bedrock = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")

# --- [최적화 1] 정적 설정(Tool Config) 전역 변수로 이동 ---
TOOL_CONFIG = {
    "tools": [
        {
            "toolSpec": {
                "name": "search_subway_facility",
                "description": "지하철 역의 편의시설 정보를 검색하기 위해 역 이름과 시설 코드를 추출합니다.",
                "inputSchema": {
                    "json": {
                        "type": "object",
                        "properties": {
                            "station": {
                                "type": "string",
                                "description": "지하철 역 이름 (예: 서울역, 강남역). '역' 접미사 포함.",
                            },
                            "facility": {
                                "type": "string",
                                "enum": [
                                    "chargers",
                                    "elevators",
                                    "escalators",
                                    "helpers",
                                    "safe-platforms",
                                    "sign-phones",
                                    "lifts",
                                    "toilets",
                                    "movingwalks",
                                ],
                                "description": "찾는 편의시설의 코드",
                            },
                        },
                        "required": [],
                    }
                },
            }
        }
    ]
}

# --- 정적 파일 로드 및 구조 변환 ---
STATION_MAP = {}

try:
    with open("station_name_code.json", "r", encoding="utf-8") as f:
        station_list = json.load(f)  # 1. JSON 리스트 로드

        # 2. 리스트를 딕셔너리로 변환 (검색 속도 최적화)
        # 구조: {'제물포': '1810', '서울역': '0150', ...}
        for item in station_list:
            name = item.get("name")
            code = item.get("station_cd")

            if name and code:
                STATION_MAP[name] = code

    print(f"Station map loaded: {len(STATION_MAP)} stations mapped.")

except Exception as e:
    print(f"Error loading station_codes.json: {e}")


def get_station_code(name):
    """
    입력된 이름으로 코드를 찾습니다.
    매핑된 코드가 없으면, 원래 이름을 그대로 반환합니다(Fallback).
    """
    if not name:
        return None

    # 1. 맵에서 직접 검색 (예: "제물포" -> "1810")
    if name in STATION_MAP:
        return STATION_MAP[name]

    # 2. '역'을 떼고 검색 (예: "제물포역" 입력 -> 맵에 "제물포"만 있는 경우 매칭)
    if name.endswith("역"):
        clean_name = name[:-1]  # "역" 제거
        if clean_name in STATION_MAP:
            return STATION_MAP[clean_name]

    # 3. 매핑 정보가 없으면 원래 이름 반환 (fallback)
    return name


def call_subway_api(endpoint):
    """지하철 편의시설 API를 호출하는 헬퍼 함수"""
    base = LAMBDA_BASE_URL.rstrip("/")
    path = endpoint.lstrip("/")
    url = f"{base}/{path}"

    print(f"API Request URL: {url}")

    try:
        req = urllib.request.Request(url)
        # [권장] 명시적으로 JSON을 원한다고 헤더 추가 (필요 시 주석 해제)
        req.add_header("Accept", "application/json")

        with urllib.request.urlopen(req, timeout=20) as response:
            if response.status == 200:
                response_body = response.read().decode("utf-8")
                return json.loads(response_body)
            else:
                return {"error": f"API call failed with status {response.status}"}

    except HTTPError as e:
        return {"error": f"HTTP Error: {e.code}"}
    except URLError as e:
        return {"error": f"URL Error: {e.reason}"}
    except Exception as e:
        return {"error": str(e)}


def extract_intent_with_tool_use(user_question):
    """Bedrock Tool Use를 사용한 의도 추출"""
    try:
        response = bedrock.converse(
            modelId=EXTRACTION_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": user_question}]}],
            toolConfig=TOOL_CONFIG,  # 전역 변수 사용
            # [수정] temperature는 inferenceConfig 안에 넣어야 합니다.
            inferenceConfig={"temperature": 0},
        )

        output_content = response["output"]["message"]["content"]

        for content in output_content:
            if "toolUse" in content:
                tool_input = content["toolUse"]["input"]
                return {
                    "station": tool_input.get("station"),
                    "facility": tool_input.get("facility"),
                }

        # 도구를 사용하지 않은 경우
        return {"station": None, "facility": None}

    except Exception as e:
        print(f"Tool Use Extraction Error: {e}")
        return {"station": None, "facility": None}


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        user_question = body.get("question", "")

        if not user_question:
            return {"statusCode": 400, "body": json.dumps({"error": "No question"})}

        # 1. 의도 추출
        intent_data = extract_intent_with_tool_use(user_question)
        print(f"Extracted Intent: {intent_data}")

        target_station_name = intent_data.get("station")
        target_facility = intent_data.get("facility")

        api_context = {}

        if target_station_name:
            # 2. 역 코드 변환
            station_identifier = get_station_code(target_station_name)

            # URL 인코딩
            encoded_identifier = urllib.parse.quote(station_identifier)

            print(f"Station: {target_station_name} -> ID: {station_identifier}")

            # 3. 기본 역 정보 호출
            station_info = call_subway_api(f"/stations/{encoded_identifier}")
            api_context["station_info"] = station_info

            # 4. 특정 시설 정보 호출 (변수명 수정됨!)
            if target_facility:
                # [수정됨] encoded_station -> encoded_identifier
                facility_info = call_subway_api(
                    f"/{target_facility}/{encoded_identifier}"
                )
                api_context[target_facility] = facility_info

        else:
            api_context = {"info": "질문에서 역 이름을 찾을 수 없습니다."}

        # 5. 최종 답변 생성 프롬프트
        system_prompt = """
            <system_prompt>
                <role>
                    당신은 'kindMap' 지도 서비스의 친절한 고객 지원 AI 봇입니다.
                    사용자의 질문에 간결하고 정확하게 한국어로 답변해야 합니다.
                    대한민국 서울특별시에서 제공하는 교통약자를 위한 서비스에 대한 정보를 제공해야 합니다.
                    사용자의 이해를 최우선으로 해야 합니다.
                </role>

                <data_handling_rules>
                    제공된 [API Data]는 JSON 형식이며, 각 시설별 테이블 구조에 기반합니다.
                    
                    1. **공통 원칙**:
                        - `oprtng_situ` (운행 현황) 정보를 최우선으로 안내하시오. (NULL이면 언급 X)
                        - `dtl_pstn` (상세 위치)를 사용하여 위치를 설명하시오.
                    
                    2. **시설별 핵심 정보**:
                        - **도우미**: `helper_tel` (전화번호) 필수.
                        - **화장실**: `gate_inout` (개찰구 내/외) 필수.
                        - **엘리베이터/리프트**: 고장 여부(`oprtng_situ`) 필수 체크.
                        - **충전기**: `dtl_pstn` 위치 상세 설명.
                </data_handling_rules>

                <context>
                    사용자의 질문에 따라 필요할 경우, 제공된 [API Data]를 바탕으로 답변하시오.
                    데이터가 비어있거나 NULL인 경우 "죄송하지만 해당 정보는 제공되지 않습니다"라고 명확히 말하시오.
                </context>

                <instructions>
                    1. 핵심만 간결하게 답변하세요 (5문장 이내).
                    2. 아주 다정하고 친절한 톤을 유지하세요.
                    3. 답변 끝에 "추가로 궁금한 점이 있으신가요?"를 덧붙이세요.
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
            "messages": [
                {"role": "user", "content": [{"type": "text", "text": final_prompt}]}
            ],
        }

        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID, body=json.dumps(payload)
        )

        final_response_body = json.loads(response.get("body").read())
        answer = final_response_body["content"][0]["text"]

        return {
            "statusCode": 200,
            "body": json.dumps({"answer": answer}, ensure_ascii=False),
        }

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
