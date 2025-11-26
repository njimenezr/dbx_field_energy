import pyspark
import dbldatagen as dg
from typing import Dict, List
from pyspark.sql import functions as F, Row, SparkSession
from pyspark.sql.functions import col, lit, when, date_format, to_date
from datetime import datetime, timedelta, time
import pandas as pd
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
import random
from math import floor


def generate_well_header_data(
    spark_session: pyspark.sql.SparkSession,
    num_assets: int = 1000,
) -> dg.DataGenerator:
    """
    Generate synthetic well header data for a specified number of assets.

    Args:
        spark_session (pyspark.sql.SparkSession): Spark session object.
        num_assets (int): Number of assets to generate data for.

    Returns:
        dg.DataGenerator: Data generator object for creating synthetic well header data.
    """

    # Define the number of rows and partitions for the synthetic dataset
    row_count = num_assets
    partitions_requested = 4

    # Define the data generator specification
    data_spec = (
        dg.DataGenerator(
            sparkSession=spark_session,
            name="well_header_data",
            rows=row_count,
            partitions=partitions_requested,
        )
        .withColumn(
            "API_NUMBER",
            IntegerType(),
            minValue=4200000,
            maxValue=4299999,
            random=True,
        )
        .withColumn("WELL_NAME", StringType(), template="Well_A-Za-z", random=True)
        .withColumn(
            "FIELD_NAME",
            StringType(),
            values=["Field_1", "Field_2", "Field_3", "Field_4", "Field_5"],
            random=True,
        )
        .withColumn(
            "LATITUDE",
            FloatType(),
            minValue=31.00,
            maxValue=32.50,
            step=1e-6,
            random=True,
        )
        .withColumn(
            "LONGITUDE",
            FloatType(),
            minValue=-104.00,
            maxValue=-101.00,
            step=1e-6,
            random=True,
        )
        .withColumn(
            "COUNTY",
            StringType(),
            values=["Reeves", "Midland", "Ector", "Loving", "Ward"],
            random=True,
        )
        .withColumn("STATE", StringType(), values=["Texas"])
        .withColumn("COUNTRY", StringType(), values=["USA"])
        .withColumn(
            "WELL_TYPE",
            StringType(),
            values=["Oil"],
            random=True,
        )
        .withColumn(
            "WELL_ORIENTATION",
            StringType(),
            values=["Horizontal"],
            random=True,
        )
        .withColumn(
            "PRODUCING_FORMATION",
            StringType(),
            values=["Wolfcamp", "Spraberry", "Bone Spring", "Delaware", "Avalon"],
            random=True,
        )
        .withColumn(
            "CURRENT_STATUS",
            StringType(),
            values=["Producing", "Shut-in", "Plugged and Abandoned", "Planned"],
            random=True,
            weights=[80, 10, 5, 5],
        )
        .withColumn(
            "TOTAL_DEPTH", IntegerType(), minValue=12000, maxValue=20000, random=True
        )
        .withColumn(
            "SPUD_DATE", DateType(), begin="2020-01-01", end="2025-02-14", random=True
        )
        .withColumn(
            "COMPLETION_DATE",
            DateType(),
            begin="2020-01-01",
            end="2025-02-14",
            random=True,
        )
        .withColumn(
            "SURFACE_CASING_DEPTH",
            IntegerType(),
            minValue=500,
            maxValue=800,
            random=True,
        )
        .withColumn("OPERATOR_NAME", StringType(), values=["OPERATOR_XYZ"])
        .withColumn(
            "PERMIT_DATE", DateType(), begin="2019-01-01", end="2025-02-14", random=True
        )
        .withColumn("GEO_RISK_INDEX", FloatType(), values=[0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1], random=True)
    )

    return data_spec


def generate_start_depth(section, subsection, previous_end):
    """
    Determine the start depth based on the section and subsection.

    Args:
    - section (str): The section of the operation.
    - subsection (str): The subsection of the operation.
    - previous_end (int): The previous end depth.

    Returns:
    - int: The start depth based on the input parameters.
    """
    if section == "RIG_MOVE":  # If the section is RIG_MOVE, start depth is 0
        return 0
    elif subsection == "DRILLING":  # If the subsection is DRILLING, start depth is the previous end depth
        return previous_end
    else:  # For all other cases, start depth is also the previous end depth
        return previous_end


def generate_end_depth(section, surface_depth, inter_depth, depth):
    """
    Determine the end depth based on the section provided.

    Args:
    section (str): The section of the well (RIG_MOVE, SURFACE, INTERMEDIATE, PRODUCTION).
    surface_depth (int): The depth at the surface.
    inter_depth (int): The intermediate depth.
    depth (int): The production depth.

    Returns:
    int: The end depth based on the section provided.
    """
    if section == "RIG_MOVE":  # If section is RIG_MOVE
        return 0
    elif section == "SURFACE":  # If section is SURFACE
        return surface_depth
    elif section == "INTERMEDIATE":  # If section is INTERMEDIATE
        return inter_depth
    elif section == "PRODUCTION":  # If section is PRODUCTION
        return depth
    else:  # For any other section
        return 0


def generate_start_time(section, subsection, spud_date, previous_end_time):
    """
    Determine the start time based on the section and subsection.

    Args:
    - section (str): The section of the operation.
    - subsection (str): The subsection of the operation.
    - spud_date (datetime.date): The spud date of the operation.
    - previous_end_time (datetime): The end time of the previous operation.

    Returns:
    - datetime: The calculated start time based on the input parameters.
    """
    if section == "RIG_MOVE":
        return datetime.combine(spud_date, time.min) - timedelta(
            minutes=3600
        )  ## FLAT RATE RIG MOVE< CAN MODIFY
    elif section == "SURFACE" and subsection == "DRILLING":
        return datetime.combine(spud_date, time.min)
    else:
        return previous_end_time


def generate_end_time(
    section,  # Input: Current section of the well
    subsection,  # Input: Current subsection of the well
    spud_date,  # Input: Date when drilling started
    start_time,  # Input: Time when drilling started
    start_depth,  # Input: Depth at the start of the operation
    end_depth,  # Input: Depth at the end of the operation
    previous_start_depth,  # Input: Depth at the start of the previous operation
    previous_end_depth,  # Input: Depth at the end of the previous operation
    formation,
    geoRisk
):
    """
    Generate the end time for a specific operation based on the provided parameters.

    Parameters:
    section (str): Current section of the well
    subsection (str): Current subsection of the well
    spud_date (datetime.date): Date when drilling started
    start_time (datetime.time): Time when drilling started
    start_depth (float): Depth at the start of the operation
    end_depth (float): Depth at the end of the operation
    previous_start_depth (float): Depth at the start of the previous operation
    previous_end_depth (float): Depth at the end of the previous operation

    Returns:
    datetime.datetime: Calculated end time for the operation
    """
    def generate_formation_randomness(formation):
        if formation == "Wolfcamp":
            return random.uniform(0.55, 0.7)
        elif formation == "Spraberry":
            return random.uniform(0.7, 0.85)
        elif formation == "Bone Spring":
            return random.uniform(0.85, 1.0)
        elif formation == "Delaware":
            return random.uniform(1, 1.15)
        elif formation == "Avalon":
            return random.uniform(1.15, 1.3)

    def generate_geoRisk_randomness(geoRisk):
        if geoRisk <= 0.2:
            return random.uniform(0.55, 0.7)
        elif geoRisk <= 0.4:
            return random.uniform(0.7, 0.85)
        elif geoRisk <= 0.6:
            return random.uniform(0.85, 1.0)
        elif geoRisk <= 0.8:
            return random.uniform(1, 1.15)
        elif geoRisk <= 1:
            return random.uniform(1.15, 1.3)

    # Define rate of penetration benchmarks for different sections
    rop_benchmarks_daily = {
        "SURFACE": 3000,
        "INTERMEDIATE": 2000,
        "PRODUCTION": 1000,
    }  ## CAN MODIFY FOR SLOWER OR FASTER WELLS

    # Determine the end time based on the section and subsection
    if section == "RIG_MOVE":
        return datetime.combine(spud_date, time.min)
    elif subsection == "DRILLING":
        return start_time + timedelta(
            minutes=round(
                (end_depth - start_depth)
                / (rop_benchmarks_daily[section] / 1440 * generate_formation_randomness(formation)*generate_geoRisk_randomness(geoRisk))
                / 15
            )
            * 15
        )
    elif subsection == "CASING":
        return start_time + timedelta(
            minutes=round(
                (previous_end_depth - previous_start_depth)
                / (
                    rop_benchmarks_daily[section]
                    * 1.5
                    / 1440
                    * generate_formation_randomness(formation)
                )
                / 15
            )
            * 15
        )
    elif subsection == "CEMENTING":
        return start_time + timedelta(
            minutes=round(1440 * random.uniform(0.75, 1.25) / 15)
            * 15  ## CAN MODIFY FOR BIGGER CEMENTING JOBS
        )
    else:
        return 0


def generate_job_phase_data(wellTable):
    """
    Generate job phase data based on well information.

    Args:
    wellTable (DataFrame): DataFrame containing well information including TOTAL_DEPTH, SPUD_DATE, and SURFACE_CASING_DEPTH.

    Returns:
    DataFrame: DataFrame containing job phase data with columns API_NUMBER, JOB_TYPE, JOB_PHASE, JOB_SUB_PHASE, START_DEPTH, END_DEPTH, START_TIME, END_TIME.
    """

    # Initialize a dictionary to store wellbore section data
    wellboreSection = {}

    for row in wellTable:
        # Extract well data
        depth = row["TOTAL_DEPTH"]
        spud_date = row["SPUD_DATE"]
        surface_casing_depth = row["SURFACE_CASING_DEPTH"]
        formation=row["PRODUCING_FORMATION"]
        geoRisk=row["GEO_RISK_INDEX"]
        surface = surface_casing_depth
        intermediate = random.randint(6000, 9000)  ## CAN MODIFY FOR BIGGER VARIANCE

        # Define nested dictionary for job phases
        nested_dict = {
            "RIG_MOVE": {"RIG_MOVE": {"DESCRIPTION": "RIG MOVE PHASE"}},
            "SURFACE": {
                "DRILLING": {"DESCRIPTION": "DRILLNG OF SURFACE HOLE"},
                "CASING": {"DESCRIPTION": "RUNNING SURFACE CASING"},
                "CEMENTING": {"DESCRIPTION": "SURFACE CEMENT JOB"},
            },
            "INTERMEDIATE": {
                "DRILLING": {"DESCRIPTION": "DRILLING OF INTERMEDIATE HOLE"},
                "CASING": {"DESCRIPTION": "RUNNING INTERMEDIATE CASING"},
                "CEMENTING": {"DESCRIPTION": "INTERMEDIATE CEMENT JOB"},
            },
            "PRODUCTION": {
                "DRILLING": {"DESCRIPTION": "DRILLING OF PRODUCTION HOLE"},
                "CASING": {"DESCRIPTION": "RUNNING PRODUCTION CASING"},
                "CEMENTING": {"DESCRIPTION": "PRODUCTION CEMENT JOB"},
            },
        }
        tempDict = {}
        # Initialize placeholders for depth and time
        start_depth = 0
        end_depth = 0
        end_time = datetime.combine(spud_date, time.min)
        start_time = datetime.combine(spud_date, time.min)

        for section, section2 in nested_dict.items():
            # CREATE EMPTY DICTIONARY FOR EACH SECTION,  THIS WILL BE FILLED WITH MULTIPLE SUB SECTIONS
            tempDict[section] = {}

            for subsection, subsection2 in section2.items():
                # Calculate depths
                previous_end_depth = end_depth
                previous_start_depth = start_depth
                end_depth = generate_end_depth(section, surface, intermediate, depth)
                start_depth = generate_start_depth(
                    section, subsection, previous_end_depth
                )

                # Calculate times
                last_end_time = end_time
                last_start_time = start_time

                start_time = generate_start_time(
                    section, subsection, spud_date, last_end_time
                )
                end_time = generate_end_time(
                    section,
                    subsection,
                    spud_date,
                    start_time,
                    start_depth,
                    end_depth,
                    previous_start_depth,
                    previous_end_depth,
                    formation,
                    geoRisk
                )

                # Store calculated values in tempDict
                tempDict[section][subsection] = {
                    "start_depth": start_depth,
                    "end_depth": end_depth,
                    "start_time": start_time.strftime("%Y-%m-%d %H:%M"),
                    "end_time": end_time.strftime("%Y-%m-%d %H:%M"),
                }

        # Store tempDict in wellboreSection
        wellboreSection[row["API_NUMBER"]] = tempDict

    # Convert wellboreSection to a list of Rows
    rows_data = []
    for api_number, sections in wellboreSection.items():
        for section, subsections in sections.items():
            for subsection, bounds in subsections.items():
                rows_data.append(
                    Row(
                        API_NUMBER=api_number,
                        JOB_TYPE="DRILLING",
                        JOB_PHASE=section,
                        JOB_SUB_PHASE=subsection,
                        START_DEPTH=bounds["start_depth"],
                        END_DEPTH=bounds["end_depth"],
                        START_TIME=bounds["start_time"],
                        END_TIME=bounds["end_time"]
                    )
                )

    # Create a DataFrame from rows_data
    spark = SparkSession.builder.appName("DrillNPTData").getOrCreate()

    job_phase_df = spark.createDataFrame(rows_data)

    return job_phase_df
def generate_npt_data(spark_session: pyspark.sql.SparkSession, wellTable):
    """
    Generate Non-Productive Time (NPT) drilling data based on the provided wellTable.

    Args:
    - spark_session: Spark session object for interacting with Spark.
    - wellTable: Table containing well information.

    Returns:
    - npt_drill_df: Spark DataFrame containing the generated NPT drilling data.
    """
    # Define a list of NPT codes and their descriptions
    key_list = [
        "SP", "LC", "WC", "FO", "CF", "HI", "EFS", "EFD", "WW", "WE",
        "RR", "BW", "TT", "TI/TO", "ML", "FD", "DD", "CRI", "BOPF",
        "DFC", "SC", "WOC", "FED", "WCI"
    ]
    npt_dict = {
        "SP": "Stuck Pipe", "LC": "Lost Circulation", "WC": "Well Control Issues",
        "FO": "Fishing Operations", "CF": "Cementing Failures", "HI": "Hole Instability",
        "EFS": "Equipment Failure (Surface)", "EFD": "Equipment Failure (Downhole)",
        "WW": "Waiting on Weather", "WE": "Waiting on Equipment", "RR": "Rig Repairs",
        "BW": "Bit Wear/Failure", "TT": "Tool Transportation Delays", "TI/TO": "Tripping In/Out",
        "ML": "Mud Losses", "FD": "Formation Damage", "DD": "Directional Drilling Challenges",
        "CRI": "Casing Running Issues", "BOPF": "Blowout Preventer (BOP) Failure",
        "DFC": "Drilling Fluid Contamination", "SC": "Stuck Casing", "WOC": "Waiting on Cement",
        "FED": "Formation Evaluation Delays", "WCI": "Wellbore Cleaning Issues"
    }

    # Initialize an empty DataFrame to combine generated data
    combined_df = pd.DataFrame()
    records=[]
    # Iterate over each row in wellTable to generate synthetic NPT data
    for row in wellTable:
        # Define the number of rows and partitions for the synthetic dataset
        row_count = random.randint(10, 20)
        partitions_requested = 4

        for i in range(row_count):
            tempList=[row["API_NUMBER"],
                      'DRILLING',
                       random.randint(1000, row["TOTAL_DEPTH"]),
                       random.choice(key_list),
                       random.uniform(0,1)*100*15]
            records.append(tempList)
    
    
    combined_df = pd.DataFrame(records, columns=['API_NUMBER', 'JOB_TYPE', 'DEPTH','NPT_CODE','NPT_DURATION_MIN'])
    combined_df["NPT_ESTIMATED_COST"]=(combined_df["NPT_DURATION_MIN"]/60)*(2000*random.uniform(0.85, 1.15)*0.5 + 1.5)
    # Map NPT codes to their descriptions
    combined_df["NPT_DESC"] = combined_df["NPT_CODE"].map(npt_dict)

    # Create a Spark session
    spark = SparkSession.builder.appName("DrillNPTData").getOrCreate()

    # Create a Spark DataFrame from the combined Pandas DataFrame
    npt_drill_df = spark.createDataFrame(combined_df)

    return npt_drill_df


def generate_cost_account_data(well_dict):
    """
    Generate cost account data for each well in the provided well dictionary.

    Args:
    well_dict (list of dict): A list of dictionaries where each dictionary represents a well with keys like "API_NUMBER" and "DEPTH_PERCENTILE_RANK".

    Returns:
    DataFrame: A Spark DataFrame containing cost records with columns like "API_NUMBER", "AFE_NUMBER", "AFE_JOB_TYPE", "COST_DESC", "COST_ACCT_CODE", and "ACTUAL_COST".
    """
    def generate_formation_randomness(formation):
        if formation == "Wolfcamp":
            return random.uniform(0.55, 0.7)
        elif formation == "Spraberry":
            return random.uniform(0.7, 0.85)
        elif formation == "Bone Spring":
            return random.uniform(0.85, 1.0)
        elif formation == "Delaware":
            return random.uniform(1, 1.15)
        elif formation == "Avalon":
            return random.uniform(1.15, 1.3)
    def generate_geoRisk_randomness(geoRisk):
        if geoRisk <= 0.2:
            return random.uniform(0.55, 0.7)
        elif geoRisk <= 0.4:
            return random.uniform(0.7, 0.85)
        elif geoRisk <= 0.6:
            return random.uniform(0.85, 1.0)
        elif geoRisk <= 0.8:
            return random.uniform(1, 1.15)
        elif geoRisk <= 1:
            return random.uniform(1.15, 1.3)
    spark = SparkSession.builder.appName("CostAcctData").getOrCreate()

    result = []  # Initialize an empty list to store the cost records

    # Define a dictionary containing cost items and their details
    afe_costs = {
        "Drilling Rig and Crew": {
            "range": [1500000, 2500000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),  # Generate a random account code
        },
        "Casing and Tubing": {
            "range": [800000, 1500000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Drilling Fluids and Chemicals": {
            "range": [400000, 800000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Directional Drilling Services": {
            "range": [300000, 600000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Logging and Formation Evaluation": {
            "range": [200000, 400000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Cementing Services": {
            "range": [150000, 300000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Site Preparation and Roads": {
            "range": [200000, 400000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Water Supply and Disposal": {
            "range": [300000, 600000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Rental Equipment": {
            "range": [200000, 400000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Transportation": {
            "range": [100000, 200000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Supervision and Engineering": {
            "range": [150000, 300000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
        "Land and Legal": {
            "range": [100000, 200000],
            "type": "variable",
            "acct_code": random.randint(10000, 99999),
        },
    }

    # Iterate over each well in the provided well dictionary
    for well in well_dict:
        api_number = well["API_NUMBER"]  # Extract the API number of the well
        percentile_rank = well[
            "DEPTH_PERCENTILE_RANK"
        ]  # Extract the percentile rank of the well
        formation=well["PRODUCING_FORMATION"]
        geoRisk=well["GEO_RISK_INDEX"]
        AFE_NUMBER = random.randint(7777777, 9999999)
        # Iterate over each cost item in the AFE costs dictionary
        for cost_item, cost_details in afe_costs.items():
            cost_range = cost_details[
                "range"
            ]  # Get the cost range for the current cost item
            (
                min_cost,
                max_cost,
            ) = cost_range  # Unpack the minimum and maximum cost values
            cost = (
                min_cost + (max_cost - min_cost) * percentile_rank
            )  # Calculate the cost based on the percentile rank
            cost = round(
                cost * generate_formation_randomness(formation)*generate_geoRisk_randomness(geoRisk), 2
            )  # Apply a random multiplier to the cost

            # Create a dictionary representing the cost record
            cost_record = {
                "API_NUMBER": api_number,
                "AFE_NUMBER": AFE_NUMBER,
                "AFE_JOB_TYPE": "DRILLING",
                "COST_DESC": cost_item,
                "COST_ACCT_CODE": cost_details["acct_code"],
                "ACTUAL_COST": cost,
            }
            result.append(cost_record)  # Append the cost record to the result list

    # Convert the result list of dictionaries to a Spark DataFrame and return it
    return spark.createDataFrame([Row(**record) for record in result])