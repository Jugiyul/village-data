import pandas as pd
import psycopg2

# ------------------------------
# 1. DB 연결 (PostgreSQL)
# ------------------------------
conn = psycopg2.connect(
    host="localhost",
    dbname="", # db명
    user="", # 유저명
    password="", # 비밀번호
    port=5432
)
cur = conn.cursor()

# ------------------------------
# 2. 엑셀 읽기
# ------------------------------
file_path = r"행정동코드_매핑정보_20241218.xlsx" ## 위치를 적어주길
df_dong = pd.read_excel(file_path, sheet_name="행정동코드", header=1)
df_gu = pd.read_excel(file_path, sheet_name="유입지코드")

# 컬럼명 정리
df_dong.columns = df_dong.columns.str.strip()
df_gu.columns = df_gu.columns.str.strip()

# NaN → None, 빈 문자열 → None
df_dong = df_dong.where(pd.notnull(df_dong), None).replace("", None)
df_gu = df_gu.where(pd.notnull(df_gu), None).replace("", None)

# 컬럼명 표준화
df_dong = df_dong.rename(columns={
    "H_SDNG_CD": "stat_dong_code",
    "H_DNG_CD": "mois_dong_code",
    "DO_NM": "sido_name",
    "CT_NM": "sigungu_name",
    "H_DNG_NM": "dong_name"
})
df_gu = df_gu.rename(columns={
    "RESD_CD": "gu_code",
    "RESD_DO_NM": "sido_name",
    "RESC_CT_NM": "sigungu_name"
})

# ------------------------------
# 3. STG 적재
# ------------------------------
records_dong = df_dong[["stat_dong_code", "mois_dong_code", "sido_name", "sigungu_name", "dong_name"]].values.tolist()
cur.executemany("""
    INSERT INTO stg.raw_region_dong (stat_dong_code, mois_dong_code, sido_name, sigungu_name, dong_name)
    VALUES (%s, %s, %s, %s, %s)
""", records_dong)

records_gu = df_gu[["gu_code", "sido_name", "sigungu_name"]].values.tolist()
cur.executemany("""
    INSERT INTO stg.raw_region_gu (gu_code, sido_name, sigungu_name)
    VALUES (%s, %s, %s)
""", records_gu)

conn.commit()

# ------------------------------
# 4. DW 적재 (구 단위)
# ------------------------------
cur.execute("""
    INSERT INTO dw.dim_region_upper (gu_code, sido_name, sigungu_name)
    SELECT gu_code, MAX(sido_name), MAX(sigungu_name)
    FROM stg.raw_region_gu
    WHERE gu_code IS NOT NULL
    GROUP BY gu_code
    ON CONFLICT (gu_code) DO NOTHING;
""")

# 로그: gu_code NULL
cur.execute("""
    INSERT INTO meta.unmatched_region (source_table, gu_code, sido_name, sigungu_name, error_reason)
    SELECT 'raw_region_gu', gu_code, sido_name, sigungu_name, 'NULL gu_code'
    FROM stg.raw_region_gu
    WHERE gu_code IS NULL;
""")

# ------------------------------
# 5. DW 적재 (동 단위)
# ------------------------------
cur.execute("""
    INSERT INTO dw.dim_region (dong_code, stat_dong_code, gu_code, sido_name, sigungu_name, dong_name)
    SELECT DISTINCT mois_dong_code, stat_dong_code, NULL, sido_name, sigungu_name, dong_name
    FROM stg.raw_region_dong
    WHERE mois_dong_code IS NOT NULL
    ON CONFLICT (dong_code) DO NOTHING;
""")

# 로그: dong_code NULL
cur.execute("""
    INSERT INTO meta.unmatched_region (source_table, dong_code, sido_name, sigungu_name, dong_name, error_reason)
    SELECT 'raw_region_dong', mois_dong_code, sido_name, sigungu_name, dong_name, 'NULL dong_code'
    FROM stg.raw_region_dong
    WHERE mois_dong_code IS NULL;
""")

conn.commit()

# ------------------------------
# 6. 동 ↔ 구 매핑 (gu_code 채우기)
# ------------------------------
cur.execute("""
    UPDATE dw.dim_region r
    SET gu_code = u.gu_code
    FROM dw.dim_region_upper u
    WHERE r.sigungu_name = u.sigungu_name
      AND r.sido_name = u.sido_name
      AND r.gu_code IS NULL;
""")

conn.commit()

# ------------------------------
# 7. (선택) sigungu_name NULL 보정
# ------------------------------
cur.execute("""
    UPDATE dw.dim_region_upper
    SET sigungu_name = sido_name || ' 전체'
    WHERE sigungu_name IS NULL;
""")

conn.commit()

print("✅ STG → DW 이관 및 매핑 완료")

cur.close()
conn.close()
