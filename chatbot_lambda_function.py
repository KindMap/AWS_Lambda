import json
import boto3
import urllib.request
import urllib.parse
import os
import re
from urllib.error import URLError, HTTPError


LAMBDA_BASE_URL = os.environ.get("LAMBDA_BASE_URL")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID")

# Bedrock client
bedrock = boto3.client(service_name="bedrock-runtime", region_name="us-west-2")


def call_subway_api(endpoint):
    """지하철 편의시설 API를 호출하는 헬퍼 함수"""
    url = f"{LAMBDA_BASE_URL}{endpoint}"
    print(f"API response: {url}")  # 확인용 로그

    try:
        req = urllib.request.Request(url)
        # req.add_header('Content-Type', 'application/json') => 확인하고 추가하기

        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                # API 응답 -> JSON 파싱
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


# 응답에 사용할 헬퍼 함수
def extract_intent_with_bedrock(user_question):
    """
    Bedrock을 사용하여 사용자의 자연어 질문에서 '지하철 역 이름'과 '편의시설(시설 코드)'을 추출
    결과는 반드시 JSON 포맷이어야 함
    """

    # 키워드 추출을 위한 시스템 프롬프트
    # API 명세서 작성해 둔 것 활용하여 작성
    extraction_system_prompt = """
    <system_prompt>
        <role>
            당신은 사용자의 자연어 질문을 처리하는 챗봇 AI 내부 자연어 처리 분석기이다.
            사용자의 질문에서 다음 두 가지 정보를 추출하여 JSON 형식으로 반환해야 한다.
        </role>
        <context>
            1. station: 지하철 역 이름 (예: "서울역", "강남역"). '역' 글자를 반드시 포함할 것. 없으면 null.
            2. facility: 사용자가 찾는 편의시설에 해당하는 API 코드. 아래 목록 중 하나를 선택. 없으면 null.
            - chargers (전동휠체어 충전기, 충전)
            - elevators (엘리베이터, 승강기)
            - escalators (에스컬레이터)
            - helpers (도우미)
            - safe-platforms (안전 발판)
            - sign-phones (수어 통역 화상 전화기, 수어 통역)
            - lifts (휠체어 리프트)
            - toilets (화장실)
            - movingwalks (무빙워크)
        </context>
        <instructions>
            사용자의 질문에서 위의 두 가지 정보를 추출하여 JSON 형식으로 반환하시오.
        </instructions>
        <constraints>
            오직 JSON 데이터만 출력하시오. 설명이나 마크다운(```json)을 붙이지 마시오.
        </constraints>
    </system_prompt>
    """

    user_message = f"질문: {user_question}"

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 200,  # 추출은 짧게 끝나므로 토큰 수를 줄임
        "system": extraction_system_prompt,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": user_message}]}
        ],
        "temperature": 0.0,  # 정확한 추출을 위해 0으로 설정
    }

    try:
        response = bedrock.invoke_model(
            modelId=BEDROCK_MODEL_ID, body=json.dumps(payload)
        )
        result_body = json.loads(response.get("body").read())
        extracted_text = result_body["content"][0]["text"]

        # LLM이 마크다운을 붙여 출력할 경우 대비
        cleaned_text = re.sub(r"```json\s*|\s*```", "", extracted_text).strip()

        return json.loads(cleaned_text)

    except Exception as e:
        print(f"Extraction Error: {e}")
        # 실패 시 기본값 반환
        return {"station": None, "facility": None}


def lambda_handler(event, context):
    try:
        # 입력 파싱
        body = json.loads(event.get("body", "{}"))
        user_question = body.get("question", "")

        if not user_question:
            return {"statusCode": 400, "body": json.dumps({"error": "No question"})}

        intent_data = extract_intent_with_bedrock(user_question)
        print(f"Extracted Intent: {intent_data}")  # 로그 확인용 (CloudWatch)

        target_station = intent_data.get("station")
        target_facility = intent_data.get("facility")

        api_context = {}

        if target_station:
            encoded_station = urllib.parse.quote(target_station)

            # 역 기본 정보는 항상 조회 -> 컨텍스트 확보
            station_info = call_subway_api(f"/stations/{encoded_station}")
            api_context["station_info"] = station_info

            # 특정 시설을 물어봤을 경우 -> 해당 시설 API 추가 조회
            if target_facility:
                facility_info = call_subway_api(f"/{target_facility}/{encoded_station}")
                api_context[target_facility] = facility_info
            else:
                # 시설을 특정하지 않았을 경우 -> pass
                pass

        else:
            api_context = {"info": "질문에서 역 이름을 찾을 수 없습니다."}

        system_prompt = """
            <system_prompt>
                <role>
                    당신은 'kindMap' 지도 서비스의 친절한 고객 지원 AI 봇입니다.
                    사용자의 질문에 간결하고 정확하게 한국어로 답변해야 합니다.
                    대한민국 서울특별시에서 제공하는 교통약자를 위한 서비스에 대한 정보를 제공해야 합니다.
                    사용자의 이해를 최우선으로 해야 합니다.
                </role>

                <context>
                    경로 찾기 서비스에서 제공하는 교통약자 유형은 총 4가지이다. {휠체어 사용자, 시각장애인을 포함한 저시력자, 청각장애인, 고령자}
                    경로 찾기의 고려 대상이 되는 핵심 기준은 총 5가지이다. {소요시간, 환승 횟수, 환승 난이도, 편의도, 혼잡도}
                    교통약자 유형별 우선순위는 다음과 같다.
                    - **PHY (휠체어)**: 환승 > 난이도 > 편의성 > 혼잡도 > 시간
                    - **VIS (시각장애)**: 편의성 > 난이도 > 환승 > 혼잡도 > 시간
                    - **AUD (청각장애)**: 편의성 > 시간 > 난이도 > 환승 > 혼잡도
                    - **ELD (고령자)**: 혼잡도 > 난이도 > 환승 > 편의성 > 시간
                    로그인한 사용자의 경우 해당 교통약자 유형별 정보를 제공해야 한다.
                    로그인하지 않은 사용자의 경우, 최초 질문 시 교통약자 유형을 물어보고 다음 답변부터 해당 사항을 적용해야 한다.
                    
                    사용자의 질문에 따라 필요할 경우, 제공된 [API Data]를 바탕으로 사용자의 질문에 답변하시오.
                    - 역의 편의시설 위치 정보나, 도우미의 전화번호 정보가 있다면 함께 안내하시오.
                    - 편의시설(엘리베이터 등)의 위치나 상태 정보를 상세히 설명하시오.
                    - 데이터가 없다면 정중하게 정보가 없음을 알리시오.
                </context>

                <instructions>
                    1. 사용자의 의도를 파악하고 핵심만 답변하세요.
                    2. 기술적인 용어는 쉽게 풀어서 설명하세요.
                    3. 답변 끝에는 항상 "추가로 궁금한 점이 있으신가요?"라고 물어보세요.
                    4. 실시간 정보를 제공하시오.
                    5. 아주 다정하고 친절한 태도로 답변하시오.
                </instructions>

                <constraints>
                    - 정치적이거나 민감한 주제에는 답변하지 마세요.
                    - 모르는 정보는 "죄송하지만 해당 정보는 가지고 있지 않습니다"라고 솔직히 말하세요. (지어내지 마세요)
                    - 답변은 5문장 이내로 작성하세요. (음성 답변을 고려하여 짧게)
                </constraints>
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
