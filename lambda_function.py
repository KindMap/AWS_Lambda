# 공공데이터 포털 API ETL 파이프라인
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor  # dictionary cusor용
import requests
import json
import os
import hashlib

RDS_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
API_KEY = os.environ.get("API_KEY")
BASE_URL = os.environ.get("BASE_URL")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 공공데이터포털의 API endpoint별 JSON 응답 키 매핑을 위한 설정
# endpoint -> 호출할 API 주소의 뒷부분
# table -> 데이터를 저장할 RDS의 테이블명
# pk -> DB 테이블의 Primary Key column명
# mapping -> {"DB column명": "API_JSON키"}

# 'https://apis.data.go.kr/B553766/wksn/{endpoint}?serviceKey={API_KEY}&dataType=JSON'
# dataType -> JSON으로 고정

API_CONFIG = [
    {
        # 엘리베이터 조회
        "endpoint": "getWksnElvtr",
        "table": "subway_elevator",
        "pk": "mng_no",
        "mapping": {
            "mng_no": "mngNo",  # 관리번호
            "stn_cd": "stnCd",  # 역코드
            "stn_nm": "stnNm",  # 역명
            "line_nm": "lineNm",  # 호선명
            "fclt_nm": "fcltNm",  # 시설명
            "oprtng_situ": "oprtngSitu",  # 가동현황
            "dtl_pstn": "dtlPstn",  # 상세위치
            "pscp_nope": "pscpNope",  # 정원인원 수
            "crtr_ymd": "crtrYmd",  # 기준일자
        },
    },
    {
        # 장애인화장실 조회
        "endpoint": "getWksnRstrm",
        "table": "subway_toilet",
        "pk": "mng_no",
        "mapping": {
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "whlchr_acs_yn": "whlchrAcsPsbltyYn",  # 휠체어 접근 가능 여부
            "gate_inout": "gateInoutSe",  # 게이트 내외 구분(외부/내부)
            "rstrm_info": "rstrmInfo",  # 화장실 구분 상세
            "stn_flr": "stnFlr",  # 위치층
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        # 휠체어리프트 조회
        "endpoint": "getWksnWhcllift",
        "table": "subway_lift",
        "pk": "mng_no",
        "mapping": {
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "oprtng_situ": "oprtngSitu",
            "limit_wht": "limitWht",
            "bgng_flr_dtl": "bgngFlrDtlPstn",  # 시작층 상세위치
            "end_flr_dtl": "endFlrDtlPstn",  # 종료층 상세위치
            "crtr_ymd": "crtrYmd",
            "vcnt_entrc_no": "vcntEntrcNo",  # 근접 출입구 위치 => db 추가하기
        },
    },
    {
        # 무빙워크 조회 데이터
        "endpoint": "getWksnMvnwlk",
        "table": "subway_movingwalk",
        "pk": "mng_no",
        "mapping": {
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "oprtng_situ": "oprtngSitu",
            "crtr_ymd": "crtrYmd",
            "vcnt_entrc_no": "vcntEntrcNo",  # 근접 출입구 위치 => db 추가하기
        },
    },
    {
        # 휠체어 급속 충전기 조회
        "endpoint": "getWksnWhclCharge",
        "table": "subway_charger",
        "pk": "mng_no",
        "mapping": {
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "dtl_pstn": "dtlPstn",
            "cnnctr_se": "cnnctrSe",  # 커넥터 구분
            "elctc_fac_cnt": "elctcFacCnt",  # 충전설비수
            "utztn_crg": "utztnCrg",  # 이용요금
            "oper_tel": "operInstTelno",  # 운영기관 전화번호
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        # 수어 영상 전화기 조회
        "endpoint": "getWksnSlng",
        "table": "subway_sign_phone",
        "pk": "generated_id",  # 생성된 ID를 PK로 사용
        "pk_gen_keys": ["stnCd", "lineNm", "dtlPstn"],  # 유니크 조합 키
        "mapping": {
            "generated_id": "generated_id",  # 매핑에도 추가
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "dtl_pstn": "dtlPstn",
            "vcnt_entrc_no": "vcntEntrcNo",  # 근접 출입구 위치
            "stn_flr": "stnFlr",
            "utztn_hr": "utztnHr",  # 이용 시간
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        # 안전발판 보유현황 조회
        "endpoint": "getWksnSafePlfm",
        "table": "subway_safe_platform",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm"],  # 역+노선 기준으로 하나씩 있다고 가정
        "mapping": {
            "generated_id": "generated_id",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "sfty_scf_yn": "sftyScfIdEn",  # 안전발판유무
            "mngr_tel": "mngrTelno",  # 관리자 전화번호
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        # 교통약자 도우미 현황 조회
        "endpoint": "getWksnHelper",
        "table": "subway_helper",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm"],
        "mapping": {
            "generated_id": "generated_id",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "helper_tel": "trffcWksnHlprTelno",  # 해당 교통약자도우미 전화번호
            "crtr_ymd": "crtrYmd",
        },
    },
]


def get_connection():
    try:
        return psycopg2.connect(
            host=RDS_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            connection_timeout=30,
        )
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise e


def generate_unique_id(item, keys):
    """
    item data와 key 목록을 조합하여 해시 생성 => unique ID
    """
    try:
        unique_str = "".join([str(item.get(k, "")) for k in keys])
        # MD5 hash 생성
        return hashlib.md5(unique_str.encode("utf-8")).hexdigest()
    except Exception as e:
        logger.error(f"ID 생성 실패: {e}")
        return None


def parse_api_response(json_data):
    """공공데이터포털 API 응답 정규화 Dict -> List, List로 통일"""
    try:
        body = json_data.get("body")
        if not body:
            return []
        items = body.get("items")
        if not items:
            return []
        item_data = items.get("item")
        if not item_data:
            return []

        if isinstance(item_data, dict):
            return [item_data]
        elif isinstance(item_data, list):
            return item_data  # list면 그냥 return
        return []
    except Exception as e:
        logger.error(f"parsing error: {e}")
        return []


def save_to_db_dynamic(
    conn, table_name, pk_column, mapping, data_list, pk_gen_keys=None
):
    """동적 쿼리 저장 + ID 생성 로직"""
    if not data_list:
        return 0

    db_columns = list(mapping.keys())
    placeholders = ["%s"] * len(db_columns)

    # Update -> pk는 업데이트하지 않음
    update_clauses = [
        f"{col} = EXCLUDED.{col}" for col in db_columns if col != pk_column
    ]

    sql = f"""
        INSERT INTO {table_name} ({', '.join(db_columns)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT ({pk_column})
        DO UPDATE SET
            {', '.join(update_clauses)},
            updated_at = NOW()
    """

    values_batch = []
    for item in data_list:
        # generate pk
        if pk_gen_keys:
            generated_id = generate_unique_id(item, pk_gen_keys)
            item["generated_id"] = generated_id  # item dictionary에 추가

        # check pk
        json_pk_key = mapping[pk_column]
        if not item.get(json_pk_key):
            continue

        row_values = []
        for db_col in db_columns:
            json_key = mapping[db_col]
            row_values.append(item.get(json_key, None))

        values_batch.append(tuple(row_values))

    if not values_batch:
        return 0

    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, values_batch)
        conn.commit()
        return len(values_batch)
    except Exception as e:
        conn.rollback()
        logger.error(f"[{table_name}] DB 저장 실패: {e}")
        return 0


def lambda_handler(event, context):
    conn = None
    total_processed_all = 0

    try:
        conn = get_connection()

        for config in API_CONFIG:
            endpoint = config["endpoint"]
            table_name = config["table"]
            mapping = config["mapping"]
            pk_column = config["pk"]
            pk_gen_keys = config.get("pk_gen_keys")  # ID 생성 키 목록 else None

            page_no = 1
            num_of_rows = 1000
            total_count = 0

            logger.info(f"[{endpoint}] 시작")

            while True:
                # API 호출 URL
                url = f"{BASE_URL}/{endpoint}?serviceKey={API_KEY}&dataType=JSON&pageNo={page_no}&numOfRows={num_of_rows}"

                try:
                    response = requests.get(url, timeout=10)
                    if response.status_code != 200:
                        logger.error(f"HTTP error: {response.status_code}")
                        break

                    try:
                        raw_data = response.json()
                    except:
                        break

                    # check Total Count
                    if page_no == 1:
                        body = raw_data.get("body", {})
                        total_count = body.get("totalCount", 0)
                        if total_count == 0:
                            break

                    rows = parse_api_response(raw_data)
                    if not rows:
                        break

                    # DB 저장
                    count = save_to_db_dynamic(
                        conn, table_name, pk_column, mapping, rows, pk_gen_keys
                    )
                    total_processed_all += count

                    logger.info(f"-> {table_name}: {count}건 ({page_no}p)")

                    if (page_no * num_of_rows) >= total_count:
                        break
                    page_no += 1

                except Exception as e:
                    logger.error(f"loop error: {e}")
                    break

    except Exception as e:
        logger.info(f"치명적 오류: {e}")
    finally:
        if conn:
            conn.close()

    return {
        "statusCode": 200,
        "body": json.dumps(f"작업 완료: 총 {total_processed_all}"),
    }
