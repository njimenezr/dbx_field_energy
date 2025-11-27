import uuid

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from utils import get_targeted_env, get_user


# LAKEBASE CODE
def get_lakebase_auth_token():
    """
    Function to get the lakebase auth token"""
    w = WorkspaceClient()
    creds = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[get_targeted_env("LAKEBASE_INSTANCE_NAME")],
    )
    return creds.token


conn = None


def get_lakebase_connection():
    global conn
    if conn is not None and not conn.closed:
        return conn

    # Check if Lakebase is configured before attempting connection
    lakebase_host = get_targeted_env("LAKEBASE_HOST", default=None)
    if not lakebase_host:
        return None

    try:
        import psycopg2

        """Function to get a connection to the lakebase database"""
        conn = psycopg2.connect(
            host=lakebase_host,
            port=get_targeted_env("LAKEBASE_PORT", 5432),
            user=get_user().user_name,
            password=get_lakebase_auth_token(),
            database=get_targeted_env("LAKEBASE_DATABASE", default="databricks_postgres"),
            sslmode="require",
        )
        return conn
    except Exception as e:
        # Return None if connection fails
        return None


# Don't connect automatically - let functions handle connection when needed
# conn = get_lakebase_connection()

conn_sync = None


def get_lakebase_connection_sync():
    global conn_sync
    if conn_sync is not None and not conn_sync.closed:
        return conn_sync

    # Check if Lakebase is configured before attempting connection
    lakebase_host = get_targeted_env("LAKEBASE_HOST", default=None)
    if not lakebase_host:
        return None

    try:
        import psycopg2

        """Function to get a connection to the lakebase database"""
        conn = psycopg2.connect(
            host=lakebase_host,
            port=get_targeted_env("LAKEBASE_PORT", 5432),
            user=get_user().user_name,
            password=get_lakebase_auth_token(),
            database=get_targeted_env("LAKEBASE_SYNC_DATABASE", default="databricks_postgres"),
            sslmode="require",
        )
        return conn
    except Exception as e:
        # Return None if connection fails
        return None


# Don't connect automatically - let functions handle connection when needed
# conn_sync = get_lakebase_connection_sync()


def get_catalog_schema():
    return f"{{.catalog}}.{{.schema}}"


def sql_query(query: str) -> pd.DataFrame:
    """Function to accept a query and return pandas dataframe"""
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{get_targeted_env('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


catalog_schema = get_catalog_schema()


def syched_job_data():
    conn = get_lakebase_connection_sync()
    query = f"""
        SELECT
            *
        FROM (
            SELECT
                *,
                dense_rank() over (PARTITION BY "PRODUCING_FORMATION" order by "SPUD_DATE" desc) as "SPUD_DATE_RANK"
            FROM
                "{get_targeted_env('LAKEBASE_SYNC_DATABASE')}"."{get_targeted_env('LAKEBASE_SYNC_SCHEMA')}"."{get_targeted_env('LAKEBASE_SYNC_JOB_DATA_TABLE')}"
            )
        WHERE "SPUD_DATE_RANK" < 10
        ORDER BY
        "PRODUCING_FORMATION", "SPUD_DATE" desc;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0].upper() for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)


print("Done with query")

# PULL IN JOB PHASE QUERY FOR DAYS VS DEPTH CHART


def syched_job_phase_data():
    return sql_query(
        f"""
            SELECT
            *
            FROM (
            SELECT
                *,
                dense_rank() over (PARTITION BY PRODUCING_FORMATION order by SPUD_DATE desc) as SPUD_DATE_RANK,
                SUM(DURATION_DAYS) over (PARTITION BY API_NUMBER order by START_TIME) as CUMULATIVE_DAYS,
                END_DEPTH*-1 as PLOTTING_DEPTH
            FROM
                {catalog_schema}.drilling_job_phase_silver) WHERE SPUD_DATE_RANK <= 10
            ORDER BY
            PRODUCING_FORMATION, SPUD_DATE, API_NUMBER, START_TIME ASC;
        """
    )


def filtered_job_data(formation_):
    job_data = syched_job_data()
    "Filters the job data to the particular formation of interest"
    _df = job_data[job_data["PRODUCING_FORMATION"] == formation_]
    return _df


def filtered_jobphase_data(formation_):
    job_phase_data = syched_job_phase_data()
    print("Getting filtered jobphase data")
    "Filters the job data to the particular formation of interest"
    _df = job_phase_data[job_phase_data["PRODUCING_FORMATION"] == formation_]
    return _df


def load_initial_cost_dataframe(cost_accts):
    """Initial creation of empty dataframe to fill the cost prediction accordian"""
    df = pd.DataFrame(
        {
            "PRODUCING_FORMATION": None,
            "GEO_RISK_INDEX": None,
            "TOTAL_DEPTH": None,
            "COST_DESC": None,
            "PREDICTED_COST": None,
        },
        index=range(len(cost_accts)),
    )
    return df


def update_cost_table(
    choice, geo_risk_index, surface_length, inter_length, production_length
):
    """Function to update the cost table with the appropriate paramters that will be used in the model.  T
    This function needs to be modified toc all the API and return the predictions."""
    w = WorkspaceClient()

    df = pd.DataFrame(
        {
            "PRODUCING_FORMATION": choice,
            "GEO_RISK_INDEX": geo_risk_index,
            "DAYS_FROM_SPUD": 0,
            "TOTAL_DEPTH": surface_length + inter_length + production_length,
            "COST_DESC": cost_accts,
        }
    )

    df = df[
        [
            "PRODUCING_FORMATION",
            "GEO_RISK_INDEX",
            "DAYS_FROM_SPUD",
            "TOTAL_DEPTH",
            "COST_DESC",
        ]
    ].to_dict(orient="records")
    response = w.serving_endpoints.query(
        name="drilling-cost-endpoint", dataframe_records=df
    )
    depth1 = surface_length + inter_length + production_length

    predictions = response.predictions
    df = pd.DataFrame(
        {
            "PRODUCING_FORMATION": choice,
            "GEO_RISK_INDEX": geo_risk_index,
            "TOTAL_DEPTH": f"{depth1:,.2f}",
            "COST_DESC": cost_accts,
            "PREDICTED_COST": [f"${value:,.2f}" for value in predictions],
        }
    )
    cost = sum(predictions)
    return df, cost


cost_accts = [
    "Drilling Rig and Crew",
    "Casing and Tubing",
    "Drilling Fluids and Chemicals",
    "Directional Drilling Services",
    "Logging and Formation Evaluation",
    "Cementing Services",
    "Site Preparation and Roads",
    "Water Supply and Disposal",
    "Rental Equipment",
    "Transportation",
    "Supervision and Engineering",
    "Land and Legal",
]

cost_df = load_initial_cost_dataframe(cost_accts)

#################### JOB PHASE TIME FUNCTIONALITY
job_phases = [
    "RIG MOVE",
    "SURFACE",
    "SURFACE",
    "SURFACE",
    "INTERMEDIATE",
    "INTERMEDIATE",
    "INTERMEDIATE",
    "PRODUCTION",
    "PRODUCTION",
    "PRODUCTION",
]


job_sub_phases = [
    "RIG MOVE",
    "DRILLING",
    "CASING",
    "CEMENT",
    "DRILLING",
    "CASING",
    "CEMENT",
    "DRILLING",
    "CASING",
    "CEMENT",
]


def load_initial_time_dataframe(job_phases_, job_sub_phases_):
    """Initial creation of empty dataframe to fill the time prediction accordian"""
    df = pd.DataFrame(
        {
            "PRODUCING_FORMATION": None,
            "GEO_RISK_INDEX": None,
            "JOB_PHASE": job_phases_,
            "JOB_SUB_PHASE": job_sub_phases_,
            "JOB_PHASE_DEPTH": None,
            "DOL_PREDICTIONS": None,
        },
        index=range(len(job_phases)),
    )
    return df


def update_time_table(
    choice, geo_risk_index, surface_length, inter_length, production_length
):
    """Function to update the cotimest table with the appropriate paramters that will be used in the model.
    This function needs to be modified to call the API and return the predictions."""

    w = WorkspaceClient()
    df = pd.DataFrame(
        {
            "PRODUCING_FORMATION": choice,
            "GEO_RISK_INDEX": geo_risk_index,
            "JOB_PHASE": job_phases,
            "JOB_SUB_PHASE": job_sub_phases,
            "JOB_PHASE_DEPTH": [
                0,
                surface_length,
                surface_length,
                surface_length,
                inter_length,
                inter_length,
                inter_length,
                production_length,
                production_length,
                production_length,
            ],
        }
    )
    df = df[
        [
            "PRODUCING_FORMATION",
            "GEO_RISK_INDEX",
            "JOB_PHASE",
            "JOB_SUB_PHASE",
            "JOB_PHASE_DEPTH",
        ]
    ].to_dict(orient="records")
    response = w.serving_endpoints.query(
        name="drilling-job-time-endpoint", dataframe_records=df
    )
    predictions = response.predictions

    df = pd.DataFrame(
        {
            "PRODUCING_FORMATION": choice,
            "GEO_RISK_INDEX": geo_risk_index,
            "JOB_PHASE": job_phases,
            "JOB_SUB_PHASE": job_sub_phases,
            "JOB_PHASE_DEPTH": [
                f"{value:,}"
                for value in [
                    0,
                    surface_length,
                    surface_length,
                    surface_length,
                    inter_length,
                    inter_length,
                    inter_length,
                    production_length,
                    production_length,
                    production_length,
                ]
            ],
            "DOL_PREDICTIONS": [f"{value:,.2f}" for value in predictions],
        }
    )
    dol = sum(predictions)
    return df, dol


time_df = load_initial_time_dataframe(
    job_phases_=job_phases, job_sub_phases_=job_sub_phases
)


def drop_lakebase_table():
    """Function to drop the table in the lakebase database"""
    conn = get_lakebase_connection()

    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS estimations;")
        conn.commit()


def create_lakebase_table():
    """Function to create a table in the lakebase database"""
    conn = get_lakebase_connection()

    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS estimations (
                ID SERIAL PRIMARY KEY,
                API_NUMBER VARCHAR(50),
                FORMATION VARCHAR(50),
                SURFACE_LENGTH INT,
                INTER_LENGTH INT,
                PRODUCTION_LENGTH INT,
                GEO_RISK_INDEX DECIMAL(5,2),
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CREATED_BY VARCHAR(50),
                UPDATED_BY VARCHAR(50),
                REVIEW_STAMP TEXT,
                COST_ESTIMATION JSONB,
                DAYS_ON_LOCATION JSONB,
                TOTAL_COST_ESTIMATION DECIMAL(10,2),
                TOTAL_DAYS_ON_LOCATION DECIMAL(10,2)
            )
        """
        )
        conn.commit()


def get_lakebase_data(query):
    """Function to get data from the lakebase database"""
    conn = get_lakebase_connection()

    with conn.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0].upper() for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)


def save_estimations(
    api_number,
    formation,
    surface_length,
    inter_length,
    production_length,
    geo_risk_index,
    user_name,
    total_cost_estimation,
    total_days_on_location,
    cost_estimation,
    days_on_location,
):
    """Function to save the estimations to the lakebase database"""
    conn = get_lakebase_connection()

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO estimations (api_number, formation, surface_length, inter_length, production_length, geo_risk_index, created_by, total_cost_estimation, total_days_on_location, cost_estimation, days_on_location)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """,
            (
                api_number,
                formation,
                surface_length,
                inter_length,
                production_length,
                geo_risk_index,
                user_name,
                total_cost_estimation,
                total_days_on_location,
                cost_estimation,
                days_on_location,
            ),
        )
        conn.commit()
        est_id = cursor.fetchone()[0]
    # Return the ID of the newly inserted row
    return est_id


def update_stamp_ai(estimation_id, summary, user_name):
    """Function to save the summary of the estimations to the lakebase database"""
    conn = get_lakebase_connection()

    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE estimations
            SET review_stamp = %s, updated_by = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """,
            (summary, user_name, estimation_id),
        )
        conn.commit()


def update_estimations() -> int:
    """Function to update the estimations in the lakebase database"""

    # the edited rows format is dictionary containing only changed properties, not all; For instance, {0: {'API_NUMBER': '1223'}}
    edited_rows = pd.DataFrame.from_dict(
        st.session_state.get("estimations_df", {}).get("edited_rows", {}),
        orient="index",
    )
    added_rows = pd.DataFrame(
        st.session_state.get("estimations_df", {}).get("added_rows", [])
    )
    deleted_rows = pd.DataFrame(
        st.session_state.get("estimations_df", {}).get("deleted_rows", [])
    )

    state = st.session_state.get("estimations_df_state", {})

    conn = get_lakebase_connection()

    with conn.cursor() as cursor:
        # Update existing rows
        for index, row in edited_rows.iterrows():
            # the edited rows format is dictionary containing only changed properties, not all; For instance, {0: {'API_NUMBER': '1223'}}
            update_values = ", ".join(
                [f"{col} = %s" for col in row.index if col != "id"]
            )
            update_params = [row[col] for col in row.index if col != "id"]
            id_ = int(state.loc[index, "ID"])

            cursor.execute(
                f"""
                UPDATE estimations
                SET {update_values}, updated_by = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (*update_params, get_user().user_name, id_),
            )

        # Insert new rows
        for index, row in added_rows.iterrows():
            insert_values = ", ".join(["%s"] * len(row))
            insert_params = [row[col] for col in row.index]
            cursor.execute(
                f"""
                INSERT INTO estimations ({', '.join(row.index)})
                VALUES ({insert_values})
            """,
                insert_params,
            )

        # Delete rows
        for index, row in deleted_rows.iterrows():
            id_ = int(state.loc[index, "ID"])
            cursor.execute(
                """
                DELETE FROM estimations
                WHERE id = %s
            """,
                (id_,),
            )

        # Commit the changes
        conn.commit()

    return len(edited_rows) + len(added_rows) + len(deleted_rows)


def get_api_numbers():
    """Function to get the API numbers from the lakebase database"""
    conn = get_lakebase_connection()

    with conn.cursor() as cursor:
        cursor.execute("SELECT DISTINCT api_number FROM estimations;")
        rows = cursor.fetchall()
        return [row[0] for row in rows]
