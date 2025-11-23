import json
import os
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import logging
from urllib.parse import unquote
import re  # 정규 표현식 모듈
from math import radians, sin, cos, sqrt, atan2  # math 모듈 최상단 이동

# 로깅 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_CONFIG = {
    "host": os.environ["DB_HOST"],
    "database": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "port": int(os.environ.get("DB_PORT", 5432)),
}

# 지하철 교통약자를 위한 편의시설 정보 조회를 위한
# 엔드포인트 경로와 실제 DB 테이블명 매핑
TABLE_MAPPING = {
    "/chargers": "subway_charger",  # 전동휠체어 급속충전기
    "/elevators": "subway_elevator",  # 엘리베이터
    "/escalators": "subway_escalator",  # 에스컬레이터
    "/helpers": "subway_helper",  # 교통약자 도우미
    "/lifts": "subway_lift",  # 휠체어 리프트
    "/movingwalks": "subway_movingwalk",  # 무빙워크
    "/safe-platforms": "subway_safe_platform",  # 안전발판
    "/sign-phones": "subway_sign_phone",  # 수어통역 화상전화기
    "/toilets": "subway_toilet",  # 장애인 화장실
}


@contextmanager
def get_db_cursor():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def lambda_handler(event, context):
    # 디버깅 로깅
    logger.info(f"Received event: {json.dumps(event)}")

    http_method = event.get("httpMethod", "")
    path = event.get("path", "")
    path_params = event.get("pathParameters") or {}
    query_params = event.get("queryStringParameters") or {}

    logger.info(f"Method: {http_method}, Path: {path}")
    logger.info(f"PathParameters: {path_params}")
    logger.info(f"QueryParameters: {query_params}")

    try:
        # 경로 파라미터 추출 헬퍼 함수
        def extract_path_param(path_key: str, path_prefix: str) -> Optional[str]:
            """pathParameters에서 또는 경로에서 직접 파라미터 추출"""
            if path_key in path_params and path_params[path_key]:
                return path_params[path_key]

            # pathParameters가 없으면 경로에서 직접 추출
            if path.startswith(path_prefix) and len(path) > len(path_prefix):
                return path[len(path_prefix) :].strip("/")

            return None

        # ------------------------------------------------------------------
        # 1. 역 정보 조회 (/stations, /stations/{identifier})
        # ------------------------------------------------------------------

        # /stations/{identifier} - 역 코드 또는 역 이름으로 특정 역 정보 조회
        if (
            path.startswith("/stations/")
            and path != "/stations"
            and http_method == "GET"
        ):
            raw_param = extract_path_param("station_cd", "/stations/")

            if raw_param:
                decoded_param = unquote(raw_param)

                # 한글 유무로 판별
                if re.search("[가-힣]", decoded_param):
                    logger.info(f"Fetching station by Name (Hangul): {decoded_param}")
                    return handle_get_station_by_name(decoded_param)
                else:
                    logger.info(
                        f"Fetching station by Code (Alphanumeric): {decoded_param}"
                    )
                    return handle_get_station_by_code(decoded_param)
            else:
                return response(400, {"error": "역 코드 또는 역 이름이 필요합니다"})

        # /stations - 전체 또는 호선별 역 조회
        elif path == "/stations" and http_method == "GET":
            return handle_get_stations(query_params)

        # ------------------------------------------------------------------
        # 2. 구간 정보 및 거리 계산
        # ------------------------------------------------------------------

        # /sections - 전체 또는 호선별 구간 조회
        elif path == "/sections" and http_method == "GET":
            return handle_get_sections(query_params)

        # /route/distance - 경로 거리 계산
        elif path == "/route/distance" and http_method == "POST":
            body = json.loads(event.get("body", "{}"))
            return handle_calculate_route_distance(body)

        # /nearby-stations - 주변 역 검색
        elif path == "/nearby-stations" and http_method == "GET":
            return handle_get_nearby_stations(query_params)

        # ------------------------------------------------------------------
        # 3. 환승역 편의성 (/transfer-convenience)
        # ------------------------------------------------------------------

        # /transfer-convenience/{station_cd} - 특정 환승역 편의성
        elif (
            path.startswith("/transfer-convenience/")
            and path != "/transfer-convenience"
            and http_method == "GET"
        ):
            station_cd = extract_path_param("station_cd", "/transfer-convenience/")
            if station_cd:
                logger.info(f"Fetching transfer convenience for station: {station_cd}")
                return handle_get_transfer_conv_by_code(station_cd)
            else:
                return response(400, {"error": "역 코드(station_cd)가 필요합니다"})

        # /transfer-convenience - 전체 환승역 편의성
        elif path == "/transfer-convenience" and http_method == "GET":
            return handle_get_all_transfer_conv()

        # ------------------------------------------------------------------
        # 4. 공통 편의시설 조회 (Dynamic Routing)
        #    /toilets (전체/쿼리), /toilets/서울역 (경로파라미터) 모두 지원
        # ------------------------------------------------------------------

        # [수정됨] 경로 파라미터까지 지원하도록 로직 개선
        elif http_method == "GET":
            # 1) 정확히 일치하는 경우 (예: /toilets) -> 전체 조회 or 쿼리파라미터 조회
            if path in TABLE_MAPPING:
                table_name = TABLE_MAPPING[path]
                logger.info(f"Fetching common data from table: {table_name}")
                return handle_get_common_data(table_name, query_params)

            # 2) 경로 파라미터가 포함된 경우 (예: /toilets/서울역) -> 특정 역 조회
            # TABLE_MAPPING의 키들 중 현재 path의 앞부분과 일치하는 것이 있는지 확인
            matched_route = next(
                (route for route in TABLE_MAPPING if path.startswith(route + "/")), None
            )

            if matched_route:
                table_name = TABLE_MAPPING[matched_route]
                raw_param = path[len(matched_route) :].strip(
                    "/"
                )  # 경로에서 파라미터 추출
                decoded_param = unquote(raw_param)

                logger.info(f"Fetching {table_name} with path param: {decoded_param}")

                # 경로 파라미터를 쿼리 파라미터 딕셔너리에 병합하여 전달
                # 한글이면 stn_nm, 아니면 stn_cd로 처리
                dynamic_params = query_params.copy()
                if re.search("[가-힣]", decoded_param):
                    dynamic_params["stn_nm"] = decoded_param
                else:
                    dynamic_params["stn_cd"] = decoded_param

                return handle_get_common_data(table_name, dynamic_params)

            # 매칭되는 경로가 없으면 404
            logger.warning(f"Endpoint not found - Path: {path}")
            return response(
                404, {"error": "엔드포인트를 찾을 수 없습니다", "path": path}
            )

        else:
            return response(
                405, {"error": "허용되지 않는 메소드입니다", "method": http_method}
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return response(400, {"error": "JSON 파싱 오류", "details": str(e)})

    except Exception as e:
        logger.error(f"Internal server error: {str(e)}", exc_info=True)
        return response(500, {"error": "서버 오류가 발생했습니다", "details": str(e)})


def handle_get_stations(params: Dict) -> Dict:
    """전체 또는 호선별 역 조회"""
    line = params.get("line")

    try:
        if line:
            query = """
            SELECT station_id, line, name, lat, lng, station_cd
            FROM subway_station
            WHERE line = %(line)s
            ORDER BY station_id
            """
            query_params = {"line": line}
        else:
            query = """
            SELECT station_id, line, name, lat, lng, station_cd
            FROM subway_station
            ORDER BY station_id
            """
            query_params = None

        with get_db_cursor() as cursor:
            cursor.execute(query, query_params)
            results = cursor.fetchall()

        return response(200, {"data": results, "count": len(results)})

    except Exception as e:
        logger.error(f"Error in handle_get_stations: {str(e)}")
        raise


def handle_get_station_by_code(station_cd: str) -> Dict:
    """특정 역 조회"""
    try:
        query = """
        SELECT station_id, line, name, lat, lng, station_cd
        FROM subway_station
        WHERE station_cd = %(station_cd)s
        """

        with get_db_cursor() as cursor:
            cursor.execute(query, {"station_cd": station_cd})
            result = cursor.fetchone()

        if not result:
            return response(
                404, {"error": "역을 찾을 수 없습니다", "station_cd": station_cd}
            )

        return response(200, {"data": result})

    except Exception as e:
        logger.error(f"Error in handle_get_station_by_code: {str(e)}")
        raise


def handle_get_station_by_name(station_name: str) -> Dict:
    """
    역 이름으로 역 정보 조회
    1. 정확 일치 검색 우선
    2. 결과가 없을 경우 부분 일치(포함) 검색
    """
    try:
        with get_db_cursor() as cursor:
            # 1. 정확 일치 조회 (Exact Match)
            query_exact = """
            SELECT station_id, line, name, lat, lng, station_cd
            FROM subway_station
            WHERE name = %(name)s
            ORDER BY line
            """
            cursor.execute(query_exact, {"name": station_name})
            results = cursor.fetchall()
            match_type = "exact"

            # 2. 정확 일치 결과가 없으면 부분 일치 조회 (Partial Match)
            if not results:
                logger.info(
                    f"No exact match found for '{station_name}', trying partial search..."
                )

                query_partial = """
                SELECT station_id, line, name, lat, lng, station_cd
                FROM subway_station
                WHERE name LIKE %(name_pattern)s
                ORDER BY name, line
                """
                # 검색어 앞뒤로 % 추가하여 포함 검색
                cursor.execute(query_partial, {"name_pattern": f"%{station_name}%"})
                results = cursor.fetchall()
                match_type = "partial"

        # 3. 두 검색 모두 결과가 없는 경우
        if not results:
            return response(
                404,
                {
                    "error": "해당 이름(또는 이름을 포함하는) 역을 찾을 수 없습니다",
                    "query_name": station_name,
                },
            )

        # 4. 결과 반환
        return response(
            200,
            {
                "match_type": match_type,  # 'exact'(정확) 또는 'partial'(유사) 구분값 전달
                "query": station_name,
                "count": len(results),
                "data": results,
            },
        )

    except Exception as e:
        logger.error(f"Error in handle_get_station_by_name: {str(e)}")
        raise


def handle_get_sections(params: Dict) -> Dict:
    """전체 또는 호선별 구간 조회"""
    line = params.get("line")

    try:
        if line:
            query = """
            SELECT section_id, line, up_station_name, down_station_name, 
                   section_order, via_coordinates
            FROM subway_section
            WHERE line = %(line)s
            ORDER BY section_order
            """
            query_params = {"line": line}
        else:
            query = """
            SELECT section_id, line, up_station_name, down_station_name, 
                   section_order, via_coordinates
            FROM subway_section
            ORDER BY line, section_order
            """
            query_params = None

        with get_db_cursor() as cursor:
            cursor.execute(query, query_params)
            results = cursor.fetchall()

        return response(200, {"data": results, "count": len(results)})

    except Exception as e:
        logger.error(f"Error in handle_get_sections: {str(e)}")
        raise


def handle_get_all_transfer_conv() -> Dict:
    """전체 환승역 편의성 조회"""
    try:
        query = """
        SELECT * FROM transfer_station_convenience
        ORDER BY station_cd
        """

        with get_db_cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()

        return response(200, {"data": results, "count": len(results)})

    except Exception as e:
        logger.error(f"Error in handle_get_all_transfer_conv: {str(e)}")
        raise


def handle_get_transfer_conv_by_code(station_cd: str) -> Dict:
    """특정 환승역 편의성 조회"""
    try:
        query = """
        SELECT * FROM transfer_station_convenience
        WHERE station_cd = %(station_cd)s
        """

        with get_db_cursor() as cursor:
            cursor.execute(query, {"station_cd": station_cd})
            result = cursor.fetchone()

        if not result:
            return response(
                404,
                {"error": "환승역 정보를 찾을 수 없습니다", "station_cd": station_cd},
            )

        return response(200, {"data": result})

    except Exception as e:
        logger.error(f"Error in handle_get_transfer_conv_by_code: {str(e)}")
        raise


def handle_calculate_route_distance(body: Dict) -> Dict:
    """경로 거리 계산"""
    route_station_cds = body.get("route_station_cds", [])

    if not route_station_cds or len(route_station_cds) < 2:
        return response(
            400, {"error": "최소 2개 이상의 역이 필요합니다", "total_distance_m": 0.0}
        )

    try:
        query = """
        SELECT 
            station_cd,
            name as station_name,
            lat,
            lng,
            array_position(%(station_cds)s, station_cd) as route_order
        FROM subway_station
        WHERE station_cd = ANY(%(station_cds)s)
        ORDER BY route_order
        """

        with get_db_cursor() as cursor:
            cursor.execute(query, {"station_cds": route_station_cds})
            stations = cursor.fetchall()

        if len(stations) != len(route_station_cds):
            missing = set(route_station_cds) - {s["station_cd"] for s in stations}
            return response(400, {"error": f"존재하지 않는 역: {list(missing)}"})

        segment_distances = []
        total_distance = 0.0

        for i in range(len(stations) - 1):
            s1 = stations[i]
            s2 = stations[i + 1]

            distance = haversine_distance(
                float(s1["lat"]), float(s1["lng"]), float(s2["lat"]), float(s2["lng"])
            )

            segment_distances.append(
                {
                    "from_cd": s1["station_cd"],
                    "from_name": s1["station_name"],
                    "to_cd": s2["station_cd"],
                    "to_name": s2["station_name"],
                    "distance_m": round(distance, 2),
                    "distance_km": round(distance / 1000, 2),
                }
            )

            total_distance += distance

        return response(
            200,
            {
                "total_distance_m": round(total_distance, 2),
                "total_distance_km": round(total_distance / 1000, 2),
                "segment_distances": segment_distances,
                "station_count": len(stations),
                "segment_count": len(segment_distances),
            },
        )

    except Exception as e:
        logger.error(f"Error in handle_calculate_route_distance: {str(e)}")
        raise


def handle_get_nearby_stations(params: Dict) -> Dict:
    """주변 역 검색"""
    try:
        lat = float(params.get("lat"))
        lng = float(params.get("lng"))
        radius_km = float(params.get("radius", 1.0))
    except (TypeError, ValueError, AttributeError):
        return response(400, {"error": "lat, lng 파라미터가 필요합니다 (숫자 형식)"})

    try:
        query = """
        SELECT 
            station_cd,
            name as station_name,
            line,
            lat,
            lng,
            ST_Distance(
                ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
                ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography
            ) / 1000 as distance_km
        FROM subway_station
        WHERE ST_DWithin(
            ST_SetSRID(ST_MakePoint(lng, lat), 4326)::geography,
            ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography,
            %(radius_m)s
        )
        ORDER BY distance_km
        LIMIT 20
        """

        with get_db_cursor() as cursor:
            cursor.execute(
                query, {"lat": lat, "lng": lng, "radius_m": radius_km * 1000}
            )
            results = cursor.fetchall()

        return response(
            200,
            {
                "search_location": {"lat": lat, "lng": lng},
                "radius_km": radius_km,
                "data": results,
                "count": len(results),
            },
        )

    except Exception as e:
        logger.error(f"Error in handle_get_nearby_stations: {str(e)}")
        raise


def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Haversine 공식을 사용한 두 지점 간 거리 계산 (미터 단위)"""
    R = 6371000  # 지구 반지름 (미터)

    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    delta_lat = radians(lat2 - lat1)
    delta_lng = radians(lng2 - lng1)

    a = (
        sin(delta_lat / 2) ** 2
        + cos(lat1_rad) * cos(lat2_rad) * sin(delta_lng / 2) ** 2
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def response(status_code: int, body: Dict) -> Dict:
    """API Gateway 응답 형식 생성"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json; charset=utf-8",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Api-Key",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        },
        "body": json.dumps(body, ensure_ascii=False, default=str),
    }


def handle_get_common_data(table_name: str, params: Dict) -> Dict:
    """
    편의시설 테이블 조회 핸들러
    - table_name : 조회할 테이블 이름
    - params : 쿼리 파라미터 stn_cd, stn_nm 등...
    """
    # 쿼리 파라미터 유연성 허용
    # 역 코드, 역명, 호선명
    stn_cd = params.get("stn_cd") or params.get("station_cd")
    stn_nm = params.get("stn_nm") or params.get("station_name")
    line_nm = params.get("line_nm") or params.get("line")  # [수정] line 키도 허용

    try:
        query = f"SELECT * FROM {table_name}"
        query_params = {}
        conditions = []

        if stn_cd:
            conditions.append("station_cd = %(stn_cd)s")
            query_params["stn_cd"] = stn_cd

        if stn_nm:
            conditions.append("stn_nm = %(stn_nm)s")
            query_params["stn_nm"] = stn_nm

        if line_nm:
            # [수정] DB 컬럼명이 line_nm이므로 조건절과 파라미터 키를 모두 line_nm으로 통일
            conditions.append("line_nm = %(line_nm)s")
            query_params["line_nm"] = line_nm

        # 조건 적용
        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " LIMIT 100"  # 대량 조회 방지용 리밋

        with get_db_cursor() as cursor:
            cursor.execute(query, query_params)
            results = cursor.fetchall()

        return response(
            200, {"resource": table_name, "count": len(results), "data": results}
        )

    except Exception as e:
        logger.error(f"Error in handle_get_common_data for {table_name}: {str(e)}")
        return response(500, {"error": f"데이터 조회 중 오류 발생: {str(e)}"})
