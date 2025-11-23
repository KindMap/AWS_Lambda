# 공공데이터 포털 API ETL 파이프라인
import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor  # dictionary cusor용
import requests
import json
import os

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
