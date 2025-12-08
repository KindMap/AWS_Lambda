import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor  # dictionary cusor용
import requests
import json
import os
import hashlib
from dotenv import load_dotenv

load_dotenv()

RDS_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_NAME = os.environ.get("DB_NAME")
API_KEY = os.environ.get("API_KEY")
BASE_URL = os.environ.get("BASE_URL")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# API 설정 (기존과 동일)
API_CONFIG = [
    {
        "endpoint": "getWksnElvtr",
        "table": "subway_elevator",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "oprtng_situ": "oprtngSitu",
            "dtl_pstn": "dtlPstn",
            "pscp_nope": "pscpNope",
            "crtr_ymd": "crtrYmd",
            "elvtr_sn": "elvtrSn",
        },
    },
    {
        "endpoint": "getWksnRstrm",
        "table": "subway_toilet",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "whlchr_acs_yn": "whlchrAcsPsbltyYn",
            "gate_inout": "gateInoutSe",
            "rstrm_info": "rstrmInfo",
            "stn_flr": "stnFlr",
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        "endpoint": "getWksnWhcllift",
        "table": "subway_lift",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "oprtng_situ": "oprtngSitu",
            "limit_wht": "limitWht",
            "bgng_flr_dtl": "bgngFlrDtlPstn",
            "end_flr_dtl": "endFlrDtlPstn",
            "crtr_ymd": "crtrYmd",
            "vcnt_entrc_no": "vcntEntrcNo",
        },
    },
    {
        "endpoint": "getWksnMvnwlk",
        "table": "subway_movingwalk",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "oprtng_situ": "oprtngSitu",
            "crtr_ymd": "crtrYmd",
            "vcnt_entrc_no": "vcntEntrcNo",
        },
    },
    {
        "endpoint": "getWksnWhclCharge",
        "table": "subway_charger",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "dtl_pstn": "dtlPstn",
            "cnnctr_se": "cnnctrSe",
            "elctc_fac_cnt": "elctcFacCnt",
            "utztn_crg": "utztnCrg",
            "oper_tel": "operInstTelno",
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        "endpoint": "getWksnSlng",
        "table": "subway_sign_phone",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "dtl_pstn": "dtlPstn",
            "vcnt_entrc_no": "vcntEntrcNo",
            "stn_flr": "stnFlr",
            "utztn_hr": "utztnHr",
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        "endpoint": "getWksnSafePlfm",
        "table": "subway_safe_platform",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "sfty_scf_yn": "sftyScfldEn",
            "mngr_tel": "mngrTelno",
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        "endpoint": "getWksnHelper",
        "table": "subway_helper",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "stnNm"],
        "mapping": {
            "generated_id": "generated_id",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "helper_tel": "trffcWksnHlprTelno",
            "crtr_ymd": "crtrYmd",
        },
    },
    {
        "endpoint": "getWksnEsctr",
        "table": "subway_escalator",
        "pk": "generated_id",
        "pk_gen_keys": ["stnCd", "lineNm", "fcltNm"],
        "mapping": {
            "generated_id": "generated_id",
            "mng_no": "mngNo",
            "stn_cd": "stnCd",
            "stn_nm": "stnNm",
            "line_nm": "lineNm",
            "fclt_nm": "fcltNm",
            "oprtng_situ": "oprtngSitu",
            "upbdnb_se": "upbdnbSe",
            "bgng_flr_dtl": "bgngFlrDtlPstn",
            "end_flr_dtl": "endFlrDtlPstn",
            "vcnt_entrc_no": "vcntEntrcNo",
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
            connect_timeout=30,
        )
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        raise e


def generate_unique_id(item, keys):
    try:
        unique_str = "".join([str(item.get(k, "")) for k in keys])
        return hashlib.md5(unique_str.encode("utf-8")).hexdigest()
    except Exception as e:
        logger.error(f"ID 생성 실패: {e}")
        return None


def parse_api_response(json_data):
    try:
        body = json_data.get("body")
        if not body:
            body = json_data.get("response", {}).get("body")

        items = body.get("items")
        if not items:
            return []
        item_data = items.get("item")
        if not item_data:
            return []

        if isinstance(item_data, dict):
            return [item_data]
        elif isinstance(item_data, list):
            return item_data
        return []
    except Exception as e:
        logger.error(f"parsing error: {e}")
        return []


def save_to_db_dynamic(
    conn, table_name, pk_column, mapping, data_list, pk_gen_keys=None
):
    if not data_list:
        return 0

    db_columns = list(mapping.keys())
    placeholders = ["%s"] * len(db_columns)

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
        if pk_gen_keys:
            generated_id = generate_unique_id(item, pk_gen_keys)
            item["generated_id"] = generated_id

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


def update_summary_table(conn):
    """
    모든 편의시설 업데이트 후, subway_facility_total 테이블을 재집계하는 함수
    - '역' 이름을 기준으로 그룹화 (REPLACE(TRIM(name), '역', '') 사용)
    - oprtng_situ 컬럼이 있는 경우, 'S'(중단) 상태는 카운트에서 제외
    """
    try:
        logger.info("subway_facility_total 테이블 업데이트 시작...")
        with conn.cursor() as cursor:
            # 1. 기존 데이터 초기화 (전체 재집계)
            cursor.execute("TRUNCATE TABLE subway_facility_total;")

            # 2. 집계 및 삽입 쿼리
            # 가동현황(oprtng_situ)이 존재하는 테이블은 'S'가 아닌 것만 카운트
            sql = """
            INSERT INTO subway_facility_total (
                station_name, station_cd_list, 
                charger_count, elevator_count, escalator_count,
                lift_count, movingwalk_count, safe_platform_count, 
                sign_phone_count, toilet_count, helper_count, 
                total_facility_count
            )
            WITH 
            -- 가동현황 체크 필요한 테이블 ('S' 제외)
            cnt_elevator AS ( 
                SELECT stn_nm, COUNT(*) as cnt FROM subway_elevator 
                WHERE oprtng_situ IS DISTINCT FROM 'S' GROUP BY stn_nm 
            ),
            cnt_escalator AS ( 
                SELECT stn_nm, COUNT(*) as cnt FROM subway_escalator 
                WHERE oprtng_situ IS DISTINCT FROM 'S' GROUP BY stn_nm 
            ),
            cnt_lift AS ( 
                SELECT stn_nm, COUNT(*) as cnt FROM subway_lift 
                WHERE oprtng_situ IS DISTINCT FROM 'S' GROUP BY stn_nm 
            ),
            cnt_movingwalk AS ( 
                SELECT stn_nm, COUNT(*) as cnt FROM subway_movingwalk 
                WHERE oprtng_situ IS DISTINCT FROM 'S' GROUP BY stn_nm 
            ),
            
            -- 가동현황이 없거나 체크 불필요한 테이블
            cnt_charger AS ( SELECT stn_nm, COUNT(*) as cnt FROM subway_charger GROUP BY stn_nm ),
            cnt_safe_platform AS ( SELECT stn_nm, COUNT(*) as cnt FROM subway_safe_platform GROUP BY stn_nm ),
            cnt_sign_phone AS ( SELECT stn_nm, COUNT(*) as cnt FROM subway_sign_phone GROUP BY stn_nm ),
            cnt_toilet AS ( SELECT stn_nm, COUNT(*) as cnt FROM subway_toilet GROUP BY stn_nm ),
            cnt_helper AS ( SELECT stn_nm, COUNT(*) as cnt FROM subway_helper GROUP BY stn_nm )

            SELECT 
                s.name AS station_name,
                ARRAY_AGG(DISTINCT s.station_cd) AS station_cd_list,
                
                COALESCE(SUM(c.cnt), 0) AS charger_count,
                COALESCE(SUM(e.cnt), 0) AS elevator_count,
                COALESCE(SUM(esc.cnt), 0) AS escalator_count,
                COALESCE(SUM(l.cnt), 0) AS lift_count,
                COALESCE(SUM(m.cnt), 0) AS movingwalk_count,
                COALESCE(SUM(sp.cnt), 0) AS safe_platform_count,
                COALESCE(SUM(sig.cnt), 0) AS sign_phone_count,
                COALESCE(SUM(t.cnt), 0) AS toilet_count,
                COALESCE(SUM(h.cnt), 0) AS helper_count,
                
                (
                    COALESCE(SUM(c.cnt), 0) + COALESCE(SUM(e.cnt), 0) + COALESCE(SUM(esc.cnt), 0) +
                    COALESCE(SUM(l.cnt), 0) + COALESCE(SUM(m.cnt), 0) + COALESCE(SUM(sp.cnt), 0) +
                    COALESCE(SUM(sig.cnt), 0) + COALESCE(SUM(t.cnt), 0) + COALESCE(SUM(h.cnt), 0)
                ) AS total_facility_count

            FROM subway_station s
                -- '역' 접미사 제거 후 이름 매칭
                LEFT JOIN cnt_charger c ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(c.stn_nm), '역', '')
                LEFT JOIN cnt_elevator e ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(e.stn_nm), '역', '')
                LEFT JOIN cnt_escalator esc ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(esc.stn_nm), '역', '')
                LEFT JOIN cnt_lift l ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(l.stn_nm), '역', '')
                LEFT JOIN cnt_movingwalk m ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(m.stn_nm), '역', '')
                LEFT JOIN cnt_safe_platform sp ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(sp.stn_nm), '역', '')
                LEFT JOIN cnt_sign_phone sig ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(sig.stn_nm), '역', '')
                LEFT JOIN cnt_toilet t ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(t.stn_nm), '역', '')
                LEFT JOIN cnt_helper h ON REPLACE(TRIM(s.name), '역', '') = REPLACE(TRIM(h.stn_nm), '역', '')

            GROUP BY s.name;
            """
            cursor.execute(sql)
            conn.commit()
            logger.info("subway_facility_total 업데이트 완료.")

    except Exception as e:
        conn.rollback()
        logger.error(f"subway_facility_total 업데이트 실패: {e}")


def lambda_handler(event, context):
    conn = None
    total_processed_all = 0

    try:
        conn = get_connection()

        # 1. 개별 API 테이블 업데이트
        for config in API_CONFIG:
            endpoint = config["endpoint"]
            table_name = config["table"]
            mapping = config["mapping"]
            pk_column = config["pk"]
            pk_gen_keys = config.get("pk_gen_keys")

            page_no = 1
            num_of_rows = 1000

            logger.info(f"[{endpoint}] 시작")

            while True:
                params = {
                    "serviceKey": API_KEY,
                    "dataType": "JSON",
                    "pageNo": page_no,
                    "numOfRows": num_of_rows,
                }

                request_url = f"{BASE_URL.rstrip('/')}/{endpoint}"

                try:
                    response = requests.get(request_url, params=params, timeout=30)
                    if response.status_code != 200:
                        logger.error(f"HTTP error: {response.status_code}")
                        break

                    try:
                        raw_data = response.json()
                    except json.JSONDecodeError:
                        logger.error(f"응답이 JSON이 아님: {response.text[:100]}")
                        break

                    header = raw_data.get("header")
                    if not header:
                        header = raw_data.get("response", {}).get("header", {})

                    if header.get("resultCode") != "00":
                        msg = header.get("resultMsg", f"알 수 없는 에러")
                        logger.error(f"API Error: {msg}")
                        break

                    rows = parse_api_response(raw_data)
                    if not rows:
                        break

                    count = save_to_db_dynamic(
                        conn, table_name, pk_column, mapping, rows, pk_gen_keys
                    )
                    total_processed_all += count

                    logger.info(f"-> {table_name}: {count}건 ({page_no}p)")

                    if len(rows) < num_of_rows:
                        logger.info(f"-> 마지막 페이지 도달. 종료.")
                        break
                    page_no += 1

                except Exception as e:
                    logger.error(f"loop error: {e}")
                    break

        # 2. 요약 테이블(subway_facility_total) 업데이트 실행
        if conn:
            update_summary_table(conn)

    except Exception as e:
        logger.info(f"치명적 오류: {e}")
    finally:
        if conn:
            conn.close()

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"작업 완료: 총 {total_processed_all}건 업데이트 및 요약 테이블 갱신"
        ),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    print("local test started")
    test_event = {}
    test_context = None
    result = lambda_handler(test_event, test_context)
    print(f"test completed. result: {result}")
