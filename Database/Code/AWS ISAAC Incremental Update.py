# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 15:20:31 2024

@author: kohm
"""

from datetime import datetime
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
import os
user = os.getenv('USERPROFILE')
code_path = os.path.join(user,
                         'OneDrive - HP Inc/Projects/Database/Code')
os.chdir(code_path)
import credentials  # nopep8
pd.set_option('display.max.columns', None)

# Set data path
feather_path = os.path.join(user,
                            'OneDrive - HP Inc\Projects\Database\Data',
                            'Feather')

# Set today for updated date
today = datetime.today()
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

current_week = today.isocalendar()[1]
current_week = today.strftime('%Y-W' + f'{current_week:02}')
# %% Connection to Database
engine = credentials.aws_connection()
i_engine = credentials.isaac_connection()
# %% Function


def db_insertdata(df, table, engine, col_dtype, if_exist):
    try:
        df.to_sql(table,
                  engine,
                  if_exists=if_exist,
                  chunksize=None,
                  index=False,
                  dtype=col_dtype)
        print('Insertion of data to Database table {} SUCCEEDED!'.format(table))
    except:
        print('Insertion of data to Database table {} FAILED!'.format(table))


def update_build(mpa_df, cycle_query, tbl, update_query):
    if len(mpa_df) == 1:
        mpa = mpa_df['ISAAC_MPA_NAME'].item()
        mpa = """('{}')""".format(mpa)
    else:
        mpa = tuple(mpa_df['ISAAC_MPA_NAME'])
    cycle_query = cycle_query.format(mpa)

    isaac_cycle_df = pd.read_sql(cycle_query, con=i_engine)

    db_cycle_query = """SELECT DISTINCT CYCLE_WK_NM,
    LOC_FROM_NM FROM FactoryOps.{} WHERE LOC_FROM_NM
    IN {} ORDER BY CYCLE_WK_NM
    """.format(tbl, mpa)

    db_cycle_df = pd.read_sql(db_cycle_query, con=engine)

    missing_cycle = isaac_cycle_df.merge(db_cycle_df, how='outer',
                                         left_on=['DB_CYCLE',
                                                  'DB_LOC_FROM_NM'],
                                         right_on=['CYCLE_WK_NM',
                                                   'LOC_FROM_NM'])

    missing_cycle = missing_cycle.loc[
        (missing_cycle['CYCLE_WK_NM'].isnull()) & (
            missing_cycle['CYCLE_WK_NM'].isnull())].copy()

    if (today_weekday < 4):
        missing_cycle = missing_cycle.loc[
            missing_cycle['DB_CYCLE'] != current_week].copy()

    for cycle, mpa in zip(missing_cycle['DB_CYCLE'],
                          missing_cycle['DB_LOC_FROM_NM']):
        update_query_u = update_query.format(cycle, mpa)
        missing_df = pd.read_sql(update_query_u, con=i_engine)
        missing_df = missing_df.loc[missing_df['QTY'] != 0].copy()
        db_insertdata(missing_df, tbl, engine,
                      {
                          'CYCLE_WK_NM': sa.types.VARCHAR(length=10),
                          'LOC_TO_CD': sa.types.VARCHAR(length=8),
                          'LOC_FROM_NM': sa.types.VARCHAR(length=30),
                          'PLTFRM_NM': sa.types.VARCHAR(length=50),
                          'BUS_UNIT_NM': sa.types.VARCHAR(length=30),
                          'PART_NR': sa.types.VARCHAR(length=30),
                          'CAL_WK_DT': sa.types.Date,
                          'QTY': sa.types.FLOAT()
                      }, 'append')
        print('{} for {} is inserted in {}'.format(cycle, mpa, tbl))
    # Update Table Update
    update_query = \
        """UPDATE FactoryOps.table_update
    SET UPDATED = '{}'
    WHERE table_update.`TABLE` = '{}'""".format(today_str, tbl)
    try:
        with engine.connect() as conn:
            conn.execute(text(update_query))
            conn.execute(text("COMMIT"))
            print('{} has been updated in AWS Database'.format(tbl))
    except:
        print('Table update for {} has FAILED!'.format(tbl))


def update_canon_build(cycle_query, tbl, update_query, fac_name, fac, prod):
    cycle_query_u = cycle_query.format(prod, fac)
    isaac_cycle_df = pd.read_sql(cycle_query_u, con=i_engine)

    db_cycle_query = """SELECT DISTINCT CYCLE_WK_NM 
    FROM FactoryOps.{} WHERE FACTORY = '{}' 
    ORDER BY CYCLE_WK_NM
    """.format(tbl, fac_name)

    db_cycle_df = pd.read_sql(db_cycle_query, con=engine)

    missing_cycle = isaac_cycle_df.merge(db_cycle_df, how='outer',
                                         left_on=['DB_CYCLE'],
                                         right_on=['CYCLE_WK_NM'])

    missing_cycle = missing_cycle.loc[
        (missing_cycle['CYCLE_WK_NM'].isnull()) & (
            missing_cycle['CYCLE_WK_NM'].isnull())].copy()

    if (today_weekday < 4):
        missing_cycle = missing_cycle.loc[
            missing_cycle['DB_CYCLE'] != current_week].copy()

    for cycle in missing_cycle['DB_CYCLE']:
        update_query_u = update_query.format(fac_name, prod, fac, cycle)
        missing_df = pd.read_sql(update_query_u, con=i_engine)
        missing_df = missing_df.loc[missing_df['QTY'] != 0].copy()
        db_insertdata(missing_df, tbl, engine,
                      {
                          'CYCLE_WK_NM': sa.types.VARCHAR(length=10),
                          'LOC_TO_CD': sa.types.VARCHAR(length=8),
                          'LOC_FROM_NM': sa.types.VARCHAR(length=30),
                          'PLTFRM_NM': sa.types.VARCHAR(length=50),
                          'BUS_UNIT_NM': sa.types.VARCHAR(length=30),
                          'PART_NR': sa.types.VARCHAR(length=30),
                          'CAL_WK_DT': sa.types.Date,
                          'QTY': sa.types.FLOAT()
                      }, 'append')
        print('{} for {} is inserted in {}'.format(cycle, fac_name, tbl))
    # Update Table Update
    update_query = \
        """UPDATE FactoryOps.table_update
    SET UPDATED = '{}'
    WHERE table_update.`TABLE` = '{}'""".format(today_str, tbl)
    try:
        with engine.connect() as conn:
            conn.execute(text(update_query))
            conn.execute(text("COMMIT"))
            print('{} has been updated in AWS Database'.format(tbl))
    except:
        print('Table update for {} has FAILED!'.format(tbl))


def update_skus(tbl, i_skus_query, mpa_df):
    if len(mpa_df) == 1:
        mpa = mpa_df['ISAAC_MPA_NAME'].item()
        mpa = """('{}')""".format(mpa)
    else:
        mpa = tuple(mpa_df['ISAAC_MPA_NAME'])

    i_skus_query = i_skus_query.format(mpa)

    i_skus = pd.read_sql(i_skus_query, i_engine)

    skus_query = """SELECT * FROM FactoryOps.{}""".format(tbl)

    db_df = pd.read_sql(skus_query, engine)

    db_df = db_df.merge(i_skus, how='outer',
                        on=['LOC_FROM_NM', 'PART_NR'], indicator=True)

    fields = ['PLTFRM_NM', 'BUS_UNIT_NM', 'PROD_LINE_CD', 'PROD_TYPE_CD',
              'BUS_GRP_NM', 'CAT_NM', 'CAT_SUB', 'PROD_GRP', 'BUS_UNIT_L3']

    for field in fields:
        main_field = field + '_x'
        replace_field = field + '_y'
        update = db_df.loc[
            (db_df['_merge'] == 'both') &
            (db_df[main_field] != db_df[replace_field]) &
            (db_df[replace_field].notnull())].copy()

        if (len(update) != 0):
            for x, y, z in zip(update['LOC_FROM_NM'],
                               update[replace_field],
                               update['PART_NR']):
                if pd.isnull(y):
                    update_query = """UPDATE FactoryOps.{} SET {} = NULL
                    WHERE PART_NR = '{}' AND LOC_FROM_NM = '{}'""".format(
                        tbl, field, z, x)
                else:
                    update_query = """UPDATE FactoryOps.{} SET {} = '{}'
                    WHERE PART_NR = '{}' AND LOC_FROM_NM = '{}'""".format(
                        tbl, field, y, z, x)
                try:
                    with engine.connect() as conn:
                        conn.execute(text(update_query))
                        conn.execute(text("COMMIT"))
                        print('{} for SKU {} updated for {}'.format(field, z, x))
                except:
                    print('SKU Update has failed for table {}'.format(tbl))

        # NEW SKUS
        db_df.loc[db_df['_merge'] == 'right_only',
                  main_field] = db_df[replace_field]

    new_skus = db_df.loc[db_df['_merge'] == 'right_only'].copy()
    new_skus.columns = new_skus.columns.str.rstrip('_x')

    new_skus = new_skus.loc[:, ~new_skus.columns.str.endswith('_y')]
    new_skus = new_skus.drop(columns=['_merge'])
    db_insertdata(new_skus, tbl, engine,
                  {
                      'LOC_FROM_NM': sa.types.VARCHAR(length=30),
                      'PLTFRM_NM': sa.types.VARCHAR(length=50),
                      'BUS_UNIT_NM': sa.types.VARCHAR(length=30),
                      'PART_NR': sa.types.VARCHAR(length=20),
                      'PROD_LINE_CD': sa.types.VARCHAR(length=5),
                      'PROD_TYPE_CD': sa.types.VARCHAR(length=15),
                      'BUS_GRP_NM': sa.types.VARCHAR(length=15),
                      'CAT_NM': sa.types.VARCHAR(length=40),
                      'CAT_SUB': sa.types.VARCHAR(length=40),
                      'PROD_GRP': sa.types.VARCHAR(length=40),
                      'BUS_UNIT_L3': sa.types.VARCHAR(length=30)
                  }, 'append')
    # Update Table Update
    update_query = \
        """UPDATE FactoryOps.table_update
    SET UPDATED = '{}'
    WHERE table_update.`TABLE` = '{}'""".format(today_str, tbl)
    try:
        with engine.connect() as conn:
            conn.execute(text(update_query))
            conn.execute(text("COMMIT"))
            print('{} has been updated in AWS Database'.format(tbl))
    except:
        print('Table update for {} has FAILED!'.format(tbl))


def update_canon_skus(tbl, i_skus_query, fac_name, prod, fac):
    i_skus_query = i_skus_query.format(fac_name, prod, fac)

    i_skus = pd.read_sql(i_skus_query, i_engine)

    skus_query = """SELECT * FROM FactoryOps.{} WHERE FACTORY = '{}'""".format(
        tbl, fac_name)

    db_df = pd.read_sql(skus_query, engine)

    db_df = db_df.merge(i_skus, how='outer',
                        on=['FACTORY', 'PART_NR'], indicator=True)

    fields = ['PLTFRM_NM', 'BUS_UNIT_NM', 'PROD_LINE_CD', 'PROD_TYPE_CD',
              'BUS_GRP_NM', 'CAT_NM', 'CAT_SUB', 'PROD_GRP', 'BUS_UNIT_L3']

    for field in fields:
        main_field = field + '_x'
        replace_field = field + '_y'
        update = db_df.loc[
            (db_df['_merge'] == 'both') &
            (db_df[main_field] != db_df[replace_field]) &
            (db_df[replace_field].notnull())].copy()

        if (len(update) != 0):
            for x, y in zip(update[replace_field],
                            update['PART_NR']):
                if pd.isnull(y):
                    update_query = """UPDATE FactoryOps.{} SET {} = NULL
                    WHERE PART_NR = '{}' AND FACTORY = '{}'""".format(
                        tbl, field, y, fac_name)
                else:
                    update_query = """UPDATE FactoryOps.{} SET {} = '{}'
                    WHERE PART_NR = '{}' AND FACTORY = '{}'""".format(
                        tbl, field, x, y, fac_name)
                try:
                    with engine.connect() as conn:
                        conn.execute(text(update_query))
                        conn.execute(text("COMMIT"))
                        print('{} for SKU {} updated for {}'.format(
                            field, y, fac_name))
                except:
                    print('{} SKU Update has failed for table {}'.format(
                        fac_name, tbl))
        # NEW SKUS
        db_df.loc[db_df['_merge'] == 'right_only',
                  main_field] = db_df[replace_field]

    new_skus = db_df.loc[db_df['_merge'] == 'right_only'].copy()
    new_skus.columns = new_skus.columns.str.rstrip('_x')

    new_skus = new_skus.loc[:, ~new_skus.columns.str.endswith('_y')]
    new_skus = new_skus.drop(columns=['_merge'])
    db_insertdata(new_skus, tbl, engine,
                  {
                      'PLTFRM_NM': sa.types.VARCHAR(length=50),
                      'BUS_UNIT_NM': sa.types.VARCHAR(length=30),
                      'PART_NR': sa.types.VARCHAR(length=20),
                      'PROD_LINE_CD': sa.types.VARCHAR(length=5),
                      'PROD_TYPE_CD': sa.types.VARCHAR(length=15),
                      'BUS_GRP_NM': sa.types.VARCHAR(length=15),
                      'CAT_NM': sa.types.VARCHAR(length=40),
                      'CAT_SUB': sa.types.VARCHAR(length=40),
                      'PROD_GRP': sa.types.VARCHAR(length=40),
                      'BUS_UNIT_L3': sa.types.VARCHAR(length=30),
                      'FACTORY': sa.types.VARCHAR(length=50)
                  }, 'append')
    # Update Table Update
    update_query = \
        """UPDATE FactoryOps.table_update
    SET UPDATED = '{}'
    WHERE table_update.`TABLE` = '{}'""".format(today_str, tbl)
    try:
        with engine.connect() as conn:
            conn.execute(text(update_query))
            conn.execute(text("COMMIT"))
            print('{} has been updated in AWS Database'.format(tbl))
    except:
        print('Table update for {} has FAILED!'.format(tbl))


# %% QUERIES
# =============================================================================
# INKJET
# =============================================================================
ink_mpas_query = """SELECT DISTINCT ISAAC_MPA_NAME FROM FactoryOps.vendor 
WHERE BU = 'Inkjet' AND Active = 'Y'"""

ink_issac_cycle_query = """SELECT DISTINCT CYCLE_WK_NM AS DB_CYCLE,
LOC_FROM_NM AS DB_LOC_FROM_NM FROM ISAAC.dbo.tbl_Output 
WHERE LOC_FROM_NM IN {} AND CYCLE_WK_NM != '#' AND BUS_UNIT_IBP_NM = 'IPSHW'
AND MOT != '#' AND DATA_ELEMENT = 'PLAN'
ORDER BY CYCLE_WK_NM"""

ink_update_query = """
SELECT CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, 
PART_NR, CAL_WK_DT, SUM(BUILD_PLAN_QT) AS QTY
FROM ISAAC.dbo.tbl_Output
WHERE BUS_UNIT_IBP_NM = 'IPSHW' AND DATA_ELEMENT = 'PLAN' AND 
CYCLE_WK_NM = '{}' AND LOC_FROM_NM = '{}' AND CYCLE_WK_NM != '#'
AND MOT != '#'
GROUP BY CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, 
PLTFRM_NM, BUS_UNIT_NM, PART_NR, CAL_WK_DT
"""

ink_skus_query = """
SELECT DISTINCT LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, PART_NR, PROD_LINE_CD,
PROD_TYPE_CD, BUS_GRP_NM, CAT_NM, CAT_SUB, PROD_GRP, BUS_UNIT_L3
FROM ISAAC.dbo.tbl_Output
WHERE BUS_UNIT_IBP_NM = 'IPSHW' AND DATA_ELEMENT = 'PLAN' AND 
CYCLE_WK_NM != '#' AND LOC_FROM_NM IN {}
AND MOT != '#'
"""

# =============================================================================
# LASER
# =============================================================================
laser_mpas_query = """SELECT DISTINCT ISAAC_MPA_NAME FROM FactoryOps.vendor 
WHERE BU = 'Laser' AND Active = 'Y'"""

jcuu_jwh_issac_cycle_query = """SELECT DISTINCT CYCLE_WK_NM AS DB_CYCLE,
LOC_FROM_NM AS DB_LOC_FROM_NM FROM ISAAC.dbo.tbl_Output 
WHERE LOC_FROM_NM IN {} AND CYCLE_WK_NM != '#' 
AND BUS_UNIT_IBP_NM IN ('LESHW', 'LESSCANNER', 'HPPSHW')
AND MOT != '#' AND DATA_ELEMENT = 'PLAN' AND REGION != 'MPA-WW'
ORDER BY CYCLE_WK_NM"""

jcuu_jwh_update_query = """
SELECT CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, 
PART_NR, CAL_WK_DT, SUM(BUILD_PLAN_QT) AS QTY
FROM ISAAC.dbo.tbl_Output 
WHERE BUS_UNIT_IBP_NM IN ('LESHW', 'LESSCANNER', 'HPPSHW') 
AND DATA_ELEMENT = 'PLAN' AND REGION != 'MPA-WW' AND
CYCLE_WK_NM = '{}' AND LOC_FROM_NM = '{}' AND CYCLE_WK_NM != '#'
AND MOT != '#'
GROUP BY CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, 
PLTFRM_NM, BUS_UNIT_NM, PART_NR, CAL_WK_DT
"""

jcuu_jwh_skus_query = """
SELECT DISTINCT LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, PART_NR, PROD_LINE_CD,
PROD_TYPE_CD, BUS_GRP_NM, CAT_NM, CAT_SUB, PROD_GRP, BUS_UNIT_L3
FROM ISAAC.dbo.tbl_Output
WHERE BUS_UNIT_IBP_NM IN ('LESHW', 'LESSCANNER', 'HPPSHW') 
AND DATA_ELEMENT = 'PLAN' AND REGION != 'MPA-WW'
AND CYCLE_WK_NM != '#' AND MOT != '#' AND LOC_FROM_NM IN {}
"""

fxncz_issac_cycle_query = """SELECT DISTINCT CYCLE_WK_NM AS DB_CYCLE,
LOC_FROM_NM AS DB_LOC_FROM_NM FROM ISAAC.dbo.tbl_Output 
WHERE LOC_FROM_NM IN {} AND CYCLE_WK_NM != '#' 
AND BUS_UNIT_IBP_NM IN ('LESHW', 'HPPSHW')
AND MOT != '#' AND DATA_ELEMENT = 'PLAN' AND REGION != 'MPA-WW'
ORDER BY CYCLE_WK_NM"""

fxncz_update_query = """
SELECT CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, 
PART_NR, CAL_WK_DT, SUM(BUILD_PLAN_QT) AS QTY
FROM ISAAC.dbo.tbl_Output 
WHERE BUS_UNIT_IBP_NM IN ('LESHW', 'HPPSHW') 
AND CYCLE_WK_NM != '#'
AND DATA_ELEMENT = 'PLAN' AND REGION != 'MPA-WW' AND
CYCLE_WK_NM = '{}' AND LOC_FROM_NM = '{}'
AND MOT != '#'
GROUP BY CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, 
PLTFRM_NM, BUS_UNIT_NM, PART_NR, CAL_WK_DT
"""

fxncz_skus_query = """
SELECT DISTINCT LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, PART_NR, PROD_LINE_CD,
PROD_TYPE_CD, BUS_GRP_NM, CAT_NM, CAT_SUB, PROD_GRP, BUS_UNIT_L3
FROM ISAAC.dbo.tbl_Output
WHERE BUS_UNIT_IBP_NM IN ('LESHW', 'HPPSHW') 
AND CYCLE_WK_NM != '#'
AND DATA_ELEMENT = 'PLAN' AND REGION != 'MPA-WW' AND
MOT != '#' AND LOC_FROM_NM IN {}
"""


fxnwhl_isaac_cycle_query = """SELECT DISTINCT CYCLE_WK_NM AS DB_CYCLE,
LOC_FROM_NM AS DB_LOC_FROM_NM FROM ISAAC.dbo.tbl_Output 
WHERE LOC_FROM_NM IN {} AND CYCLE_WK_NM != '#' 
AND MOT != '#' AND DATA_ELEMENT = 'PLAN' 
AND PROD_LINE_CD IN ('E0','E4','G8','GW','IT','LX','GQ','GR','GX','HF','IG',
                     'MC','E5','G0','GL','GY','LZ')
ORDER BY CYCLE_WK_NM"""

fxnwhl_update_query = """
SELECT CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, 
PART_NR, CAL_WK_DT, SUM(BUILD_PLAN_QT) AS QTY
FROM ISAAC.dbo.tbl_Output 
WHERE PROD_LINE_CD IN ('E0','E4','G8','GW','IT','LX','GQ','GR','GX','HF','IG',
                     'MC','E5','G0','GL','GY','LZ')
AND CYCLE_WK_NM != '#'
AND DATA_ELEMENT = 'PLAN' AND
CYCLE_WK_NM = '{}' AND LOC_FROM_NM = '{}'
AND MOT != '#'
GROUP BY CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, 
PLTFRM_NM, BUS_UNIT_NM, PART_NR, CAL_WK_DT
"""

fxnwhl_skus_query = """
SELECT DISTINCT LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, PART_NR, PROD_LINE_CD,
PROD_TYPE_CD, BUS_GRP_NM, CAT_NM, CAT_SUB, PROD_GRP, BUS_UNIT_L3
FROM ISAAC.dbo.tbl_Output
WHERE PROD_LINE_CD IN ('E0','E4','G8','GW','IT','LX','GQ','GR','GX','HF','IG',
                     'MC','E5','G0','GL','GY','LZ')
AND CYCLE_WK_NM != '#'
AND DATA_ELEMENT = 'PLAN' AND MOT != '#' AND LOC_FROM_NM IN {}
"""
# =============================================================================
# LASER CANON
# =============================================================================
canon_isaac_cycle_query = """SELECT DISTINCT CYCLE_WK_NM AS DB_CYCLE
FROM ISAAC.dbo.tbl_Output 
WHERE CYCLE_WK_NM != '#' 
AND BUS_UNIT_IBP_NM = 'LESHW'
AND MOT != '#' AND DATA_ELEMENT = 'PLAN' AND PROD_TYPE_CD IN ({})
AND FACTORY_DN IN ({}) AND BUS_UNIT_NM IN ('VOLUME LASER PRINTERS', 
                                           'VALUE LASER PRINTERS')
ORDER BY CYCLE_WK_NM
"""

canon_update_query = """
SELECT CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, PLTFRM_NM, BUS_UNIT_NM, 
PART_NR, CAL_WK_DT, SUM(BUILD_PLAN_QT) AS QTY, '{}' AS FACTORY
FROM ISAAC.dbo.tbl_Output 
WHERE CYCLE_WK_NM != '#' 
AND BUS_UNIT_IBP_NM = 'LESHW'
AND MOT != '#' AND DATA_ELEMENT = 'PLAN' AND PROD_TYPE_CD IN ({})
AND FACTORY_DN IN ({}) AND BUS_UNIT_NM IN ('VOLUME LASER PRINTERS', 
                                           'VALUE LASER PRINTERS')
AND CYCLE_WK_NM = '{}'
GROUP BY CYCLE_WK_NM, LOC_TO_CD, LOC_FROM_NM, 
PLTFRM_NM, BUS_UNIT_NM, PART_NR, CAL_WK_DT
"""

canon_skus_query = """
SELECT DISTINCT PLTFRM_NM, BUS_UNIT_NM, PART_NR, PROD_LINE_CD,
PROD_TYPE_CD, BUS_GRP_NM, CAT_NM, CAT_SUB, PROD_GRP, BUS_UNIT_L3,
'{}' AS FACTORY
FROM ISAAC.dbo.tbl_Output
WHERE CYCLE_WK_NM != '#' 
AND BUS_UNIT_IBP_NM = 'LESHW'
AND MOT != '#' AND DATA_ELEMENT = 'PLAN' AND PROD_TYPE_CD IN ({})
AND FACTORY_DN IN ({}) AND BUS_UNIT_NM IN ('VOLUME LASER PRINTERS', 
                                           'VALUE LASER PRINTERS')
"""

# %% DATA
# =============================================================================
# INKJET POR BUILD PLAN
# =============================================================================
ink_mpa_df = pd.read_sql(ink_mpas_query, engine)

update_build(ink_mpa_df, ink_issac_cycle_query, 'ink_por_build_plan',
             ink_update_query)

# =============================================================================
# LASER POR BUILD PLAN
# =============================================================================
laser_mpa_df = pd.read_sql(laser_mpas_query, engine)
jcuu_jwh_mpa_df = laser_mpa_df.loc[
    laser_mpa_df['ISAAC_MPA_NAME'].str.contains('JABIL CHIHUAHUA|JABIL WEIHAI')].copy()

fxncz_mpa_df = laser_mpa_df.loc[
    laser_mpa_df['ISAAC_MPA_NAME'] == 'HP FOXCONN PARDUBICE MFG(SG31)'].copy()

fxnwhl_mpa_df = laser_mpa_df.loc[
    laser_mpa_df['ISAAC_MPA_NAME'] == 'FOXCONN WEIHAI'].copy()

update_build(jcuu_jwh_mpa_df, jcuu_jwh_issac_cycle_query,
             'laser_por_build_plan', jcuu_jwh_update_query)

update_build(fxncz_mpa_df, fxncz_issac_cycle_query,
             'laser_por_build_plan', fxncz_update_query)

update_build(fxnwhl_mpa_df, fxnwhl_isaac_cycle_query,
             'laser_por_build_plan', fxnwhl_update_query)

# =============================================================================
# LASER CANON POR BUILD PLAN
# =============================================================================
update_canon_build(canon_isaac_cycle_query, 'canon_ffgi_por_build_plan',
                   canon_update_query, 'Canon VN', """'CANONCVN', 'CANONCVNO'""",
                   """'FFGI', 'FGI'""")

update_canon_build(canon_isaac_cycle_query, 'canon_ffgi_por_build_plan',
                   canon_update_query, 'Canon PH', """'CANONCBMP', 'CANONCBMPO'""",
                   """'FFGI', 'FGI'""")

update_canon_build(canon_isaac_cycle_query, 'canon_ffgi_por_build_plan',
                   canon_update_query, 'Canon JP', """'CANONAKG'""",
                   """'FFGI', 'FGI'""")

update_canon_build(canon_isaac_cycle_query, 'canon_ffgi_por_build_plan',
                   canon_update_query, 'Canon CN, Zhongshan', """'CANONZNS', 'CANONZNSO'""",
                   """'FFGI', 'FGI'""")

update_canon_build(canon_isaac_cycle_query, 'canon_eng_por_build_plan',
                   canon_update_query, 'Canon VN', """'CANONCVN', 'CANONCVNO'""",
                   """'PRE'""")

update_canon_build(canon_isaac_cycle_query, 'canon_eng_por_build_plan',
                   canon_update_query, 'Canon PH', """'CANONCBMP', 'CANONCBMPO'""",
                   """'PRE'""")

update_canon_build(canon_isaac_cycle_query, 'canon_eng_por_build_plan',
                   canon_update_query, 'Canon JP', """'CANONAKG', 'CANONNAG'""",
                   """'PRE'""")

update_canon_build(canon_isaac_cycle_query, 'canon_eng_por_build_plan',
                   canon_update_query, 'Canon CN, Zhongshan', """'CANONZNS', 'CANONZNSO'""",
                   """'PRE'""")

# =============================================================================
# INKJET SKUS
# =============================================================================
update_skus('ink_skus', ink_skus_query, ink_mpa_df)

# =============================================================================
# LASER SKUS
# =============================================================================
update_skus('laser_skus', jcuu_jwh_skus_query, jcuu_jwh_mpa_df)

update_skus('laser_skus', fxncz_skus_query, fxncz_mpa_df)

update_skus('laser_skus', fxnwhl_skus_query, fxnwhl_mpa_df)

# =============================================================================
# CANON SKUS
# =============================================================================
update_canon_skus('canon_ffgi_skus', canon_skus_query, 'Canon VN', """'FFGI', 'FGI'""",
                  """'CANONCVN', 'CANONCVNO'""")

update_canon_skus('canon_ffgi_skus', canon_skus_query, 'Canon PH', """'FFGI', 'FGI'""",
                  """'CANONCBMP', 'CANONCBMPO'""")

update_canon_skus('canon_ffgi_skus', canon_skus_query, 'Canon JP', """'FFGI', 'FGI'""",
                  """'CANONAKG'""")

update_canon_skus('canon_ffgi_skus', canon_skus_query, 'Canon CN, Zhongshan', """'FFGI', 'FGI'""",
                  """'CANONZNS', 'CANONZNSO'""")

update_canon_skus('canon_eng_skus', canon_skus_query, 'Canon VN', """'PRE'""",
                  """'CANONCVN', 'CANONCVNO'""")

update_canon_skus('canon_eng_skus', canon_skus_query, 'Canon PH', """'PRE'""",
                  """'CANONCBMP', 'CANONCBMPO'""")

update_canon_skus('canon_eng_skus', canon_skus_query, 'Canon JP', """'PRE'""",
                  """'CANONAKG', 'CANONNAG'""")

update_canon_skus('canon_eng_skus', canon_skus_query, 'Canon CN, Zhongshan', """'PRE'""",
                  """'CANONZNS', 'CANONZNSO'""")