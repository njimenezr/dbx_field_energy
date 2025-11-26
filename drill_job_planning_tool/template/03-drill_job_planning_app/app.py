import altair as alt
import streamlit as st
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    HumanMessage,
    SystemMessage,
    trim_messages,
)
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import START, MessagesState, StateGraph
from streamlit_float import *

# initialize float feature/capability
float_init(theme=True, include_unstable_primary=False)

import agent
import data
from utils import get_context_username, get_targeted_env

title = get_targeted_env("APP_TITLE", default="CORP XYZ AUTODRILL")
st.set_page_config(layout="wide", menu_items=None, page_icon="gear", page_title=title)
ss = st.session_state

hide_default_format = """ 
       <style>
            #MainMenu {visibility: hidden; }
            .stAppViewBlockContainer {padding-top: 1rem !important;}
            .stMainBlockContainer {padding-top: 1rem !important;}
            .stHeading {text-align: right;}
            .stHeading h1 {padding: 0px;}
       </style>
    """
st.markdown(hide_default_format, unsafe_allow_html=True)


st.title(f"**{title}**", anchor=False)
st.markdown(
    '<p style="text-align: right; padding: 5px; color: #1B3139">Powered by <img src="./app/static/images/logo.png" width="150" height="24"/></p>',
    unsafe_allow_html=True,
)


st.html(
    """
    <style>
        h1 strong {
            text-transform: uppercase;
        }
        .stPopoverBody{
            width: 460px;
            height: 600px;
        }
        .stPopover {
            position: fixed;
            bottom: -8px;
            right: 0px;
            width: 200px;
            height: 40px;
            border-color: #ff5f46;
        }
        .stPopover button {
            border-bottom-color: #ff5f46 !important;
            color: #ff5f46 !important;
        }
        .stChatInput {
            bottom: 0px;
            width: 611px;
            height: 40px;
            margin: 10px;
        }
        div[data-testid="stPopoverBody"] {
            width: 670px;
            height: 600px;
        }

        div[data-testid="stPopoverBody"] > div {
            display: flex;
            flex-direction: column-reverse;
            height: 500px;
            overflow: auto;
        }
    </style>
    """
)

with st.popover("AI Assistant"):
    if "chat_history" not in st.session_state:
        init_msg = "Hello, I am an AI assistant. How can I help you?"
        st.session_state.chat_history = [
            AIMessage(content=init_msg),
        ]
        with st.chat_message("ai"):
            st.write(init_msg)

    if "memory" not in st.session_state:
        st.session_state.memory = MemorySaver()

    model = agent.get_db_chat_client(get_targeted_env("ASSISTANT_MODEL"))

    prompt_template = ChatPromptTemplate(
        [
            # SystemMessage(content='Multi-Agent Supervisor answers the questions about well drilling plan time and cost estimations as well as provides domain-related information, e.g., answers questions like "How to select geologic risk?" or "What are the best practices in Oil and Gas industry for selecting intermediate casing length?."'),
            MessagesPlaceholder(variable_name="messages")
        ]
    )

    # Messages Trimmer
    trimmer = trim_messages(
        strategy="last", max_tokens=50, token_counter=len, include_system=True
    )

    # define a new graph
    workflow = StateGraph(state_schema=MessagesState)

    def call_model(state: MessagesState):
        trimmed_messages = trimmer.invoke(state["messages"])
        chat_template_messages = prompt_template.invoke({"messages": trimmed_messages})
        completion = model.invoke(input=chat_template_messages)
        return {"messages": completion}

    # Create a new Node
    workflow.add_edge(START, "model")
    workflow.add_node("model", call_model)

    # Runnable Graph
    runnable_graph = workflow.compile(checkpointer=st.session_state.memory)

    # Graph config
    config = {"configurable": {"thread_id": 1}}

    # Accept user input
    container = st.container()
    with container:
        button_b_pos = "0rem"
        button_css = float_css_helper(width="2.2rem", bottom=button_b_pos, transition=0)
        float_parent(css=button_css)
        prompt = st.chat_input()

    if prompt:
        # Previous Messages
        for message in st.session_state.chat_history:
            with st.chat_message(
                "human" if isinstance(message, HumanMessage) else "ai"
            ):
                st.write(message.content)

        # Appends user prompt to chat history
        st.session_state.chat_history.append(HumanMessage(content=prompt))

        # New Message
        with st.chat_message("human"):
            st.write(prompt)

        # New AI Message
        with st.chat_message("assistant"):
            placeholder = st.empty()
            streamed_text = ""

        try:
            for message_chunk in runnable_graph.stream(
                {"messages": HumanMessage(content=prompt)},
                config,
                stream_mode="updates",
            ):
                if "model" in message_chunk and message_chunk["model"].get("messages"):
                    messages = message_chunk["model"].get("messages")
                    if isinstance(messages, AIMessage):
                        for part in messages.content:
                            if isinstance(part, dict) and part.get("type") == "text":
                                streamed_text += part["text"]
                    elif isinstance(messages, AIMessage) and isinstance(
                        messages.content, str
                    ):
                        streamed_text += messages.content
                    else:
                        streamed_text += str(messages)
                    placeholder.markdown(streamed_text, unsafe_allow_html=True)

                # TODO can be used if the 'second'-query failure is fixed.
                # if hasattr(message_chunk, "content"):
                #     if isinstance(message_chunk.content, list):
                #         for part in message_chunk.content:
                #             if isinstance(part, dict) and part.get("type") == "text":
                #                 streamed_text += part["text"]
                #     elif isinstance(message_chunk.content, str):
                #         streamed_text += message_chunk.content

                #     # Update markdown in real-time
                #     placeholder.markdown(streamed_text, unsafe_allow_html=True)

            st.session_state.chat_history.append(AIMessage(content=streamed_text))
        except Exception as e:
            error_msg = f"Error generating response: {e}"

# data.drop_lakebase_table()
data.create_lakebase_table()

left, right = st.columns([0.15, 0.85])
with left:
    # SIDEBAR SECTION WITH INPUTS and SAVE
    st.subheader("Drill Planning Inputs", anchor=False)

    api_number = st.text_input(
        label="API Number", value="", help="Please enter API number for the well."
    )

    formation = st.selectbox(
        label="Target Formation",
        options=["Wolfcamp", "Spraberry", "Bone Spring", "Delaware", "Avalon"],
        help="Target Formation for Lateral",
        key="formation-sbox",
    )

    surface_length = st.slider(
        label="Surface Casing Length",
        min_value=300,
        max_value=800,
        value=570,
        help="Please select total length of surface casing.",
    )

    inter_length = st.slider(
        label="Intermediate Casing Length",
        min_value=5000,
        max_value=9000,
        value=5612,
        help="Please select total length of intermediate casing.",
    )

    production_length = st.slider(
        label="Production Casing Length",
        min_value=6000,
        max_value=10000,
        value=7764,
        help="Please select total length of production casing",
    )

    geo_risk_index = st.slider(
        "Geologic Risk Index:",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.1,
        help="Please select geologic risk index",
    )

    do_summary = st.checkbox(
        label="Put a Review Stamp",
        value=True,
        help="Check this box to ask the SME Agent to review the inputs and estimation regarding outliers and anomalies.",
    )

    # add button to save inputs
    if st.button("Save"):
        progress_text = "Operation in progress. Please wait."
        percent_complete = 0

        my_bar = st.progress(percent_complete, text=progress_text)
        if not api_number:
            st.error("API Number is required.")
            my_bar.empty()
        else:
            try:
                percent_complete += 10
                my_bar.progress(percent_complete, text="Estimating costs...")
                cost_table, total_cost = data.update_cost_table(
                    formation,
                    geo_risk_index,
                    surface_length,
                    inter_length,
                    production_length,
                )

                percent_complete += 30
                my_bar.progress(percent_complete, text="Estimating time...")
                time_table, dol = data.update_time_table(
                    formation,
                    geo_risk_index,
                    surface_length,
                    inter_length,
                    production_length,
                )

                percent_complete += 20
                my_bar.progress(percent_complete, text="Saving inputs...")
                estimation_id = data.save_estimations(
                    api_number,
                    formation,
                    surface_length,
                    inter_length,
                    production_length,
                    geo_risk_index,
                    get_context_username(),
                    total_cost,
                    dol,
                    cost_table.to_json(),
                    time_table.to_json(),
                )

                percent_complete += 40
                if do_summary:
                    try:
                        my_bar.progress(
                            percent_complete,
                            text="Agent working on the review stamp evaluation...",
                        )
                        stamp = agent.ai_ask_estimation_stamp(
                            {
                                "PRODUCING_FORMATION": formation,
                                "TOTAL_DEPTH": surface_length + inter_length + production_length,
                                "GEO_RISK_INDEX": geo_risk_index,
                                "TOTAL_COST_PREDICTED": total_cost,
                                "TOTAL_DAYS_ON_LOCATION_PREDICTED": dol,
                            }
                        )
                        data.update_stamp_ai(estimation_id, stamp, "agent_bot")
                    except Exception as e:
                        st.warning(f"Summary generation failed: {e}")

                my_bar.empty()
                st.success("Inputs saved successfully!")
                st.write("Estimation ID:", estimation_id)

            except Exception as e:
                my_bar.empty()
                st.error(f"Failed to save estimation: {e}")
                st.error("Please check your inputs and try again.")

with right:
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "Well Locations Map",
            "Days vs Depth",
            "Job Cost Estimation",
            "Job Time Estimation",
            "Saved Estimates",
        ]
    )

    with tab1:
        # PREPARE DATA
        try:
            filtered_data = data.filtered_job_data(formation)
            filtered_data["LATITUDE"] = filtered_data["LATITUDE"].astype(float)
            filtered_data["LONGITUDE"] = filtered_data["LONGITUDE"].astype(float)

            jobphase_data = data.filtered_jobphase_data(formation)
            jobphase_data["CUMULATIVE_DAYS"] = jobphase_data["CUMULATIVE_DAYS"].astype(
                float
            )

            # MAP SECTION
            if (
                not filtered_data.empty
                and "LATITUDE" in filtered_data.columns
                and "LONGITUDE" in filtered_data.columns
            ):
                st.map(filtered_data[["LATITUDE", "LONGITUDE"]])
            else:
                st.warning("No location data available for the selected formation.")

        except Exception as e:
            st.error(f"Failed to load map data: {e}")
            st.warning("Map section is temporarily unavailable.")

    with tab2:
        try:
            # Get data if not already loaded
            if "filtered_data" not in locals() or "jobphase_data" not in locals():
                filtered_data = data.filtered_job_data(formation)
                jobphase_data = data.filtered_jobphase_data(formation)
                jobphase_data["CUMULATIVE_DAYS"] = jobphase_data[
                    "CUMULATIVE_DAYS"
                ].astype(float)

            # DVD CURVE SECTION
            if not jobphase_data.empty:
                chart = (
                    alt.Chart(jobphase_data)
                    .mark_line()
                    .encode(x="CUMULATIVE_DAYS", y="PLOTTING_DEPTH", color="WELL_NAME")
                )
                st.altair_chart(chart, use_container_width=True)
            else:
                st.warning("No data available for Days vs Depth chart.")

            # NPT JOB SECTIONS
            st.write("JOB NPT")
            c1, c2 = st.columns(2)

            if not filtered_data.empty:
                with c1:
                    try:
                        st.bar_chart(
                            filtered_data,
                            x="WELL_NAME",
                            y="TOTAL_NPT_COST",
                            color="WELL_NAME",
                        )
                    except Exception as e:
                        st.error(f"Failed to create NPT Cost chart: {e}")

                with c2:
                    try:
                        st.bar_chart(
                            filtered_data,
                            x="WELL_NAME",
                            y="TOTAL_NPT_MIN",
                            color="WELL_NAME",
                        )
                    except Exception as e:
                        st.error(f"Failed to create NPT Time chart: {e}")

                st.write("JOB COST")
                c1, c2 = st.columns(2)
                with c1:
                    try:
                        st.bar_chart(
                            filtered_data,
                            x="WELL_NAME",
                            y="TOTAL_ACTUAL_COST",
                            color="WELL_NAME",
                        )
                    except Exception as e:
                        st.error(f"Failed to create Actual Cost chart: {e}")

                with c2:
                    try:
                        st.bar_chart(
                            filtered_data,
                            x="WELL_NAME",
                            y="COST_PER_FOOT",
                            color="WELL_NAME",
                        )
                    except Exception as e:
                        st.error(f"Failed to create Cost Per Foot chart: {e}")
            else:
                st.warning("No data available for charts.")

        except Exception as e:
            st.error(f"Failed to load chart data: {e}")
            st.warning("Chart section is temporarily unavailable.")

    with tab3:
        try:
            cost_table, total_cost = data.update_cost_table(
                formation,
                geo_risk_index,
                surface_length,
                inter_length,
                production_length,
            )

            if cost_table is not None and not cost_table.empty:
                st.dataframe(cost_table)
                st.divider()
                st.markdown(f"**Total Well Cost:** {total_cost:,.2f}")
            else:
                st.warning("No cost data available.")

        except Exception as e:
            st.error(f"Failed to load cost estimation: {e}")
            st.warning("Cost estimation is temporarily unavailable.")

    with tab4:
        st.markdown("**Job Time Estimation**")
        try:
            time_table, dol = data.update_time_table(
                formation,
                geo_risk_index,
                surface_length,
                inter_length,
                production_length,
            )

            if time_table is not None and not time_table.empty:
                st.dataframe(time_table)
                st.divider()
                st.markdown(f"**Total Days on Location:** {dol:,.2f}")
            else:
                st.warning("No time estimation data available.")

        except Exception as e:
            st.error(f"Failed to load time estimation: {e}")
            st.warning("Time estimation is temporarily unavailable.")

    with tab5:
        try:
            # st.selectbox("API", options=data.get_api_numbers(), key="api_number", help="Select API number to filter saved estimates")
            data_db = data.get_lakebase_data(
                """
                SELECT
                    ID,
                    API_NUMBER,
                    FORMATION,
                    SURFACE_LENGTH,
                    INTER_LENGTH,
                    PRODUCTION_LENGTH,
                    GEO_RISK_INDEX,
                    CREATED_AT,
                    UPDATED_AT,
                    CASE 
                        WHEN CREATED_BY = 'agent_bot' THEN CREATED_BY
                        WHEN LENGTH(CREATED_BY) >= 7 THEN 
                            SUBSTR(CREATED_BY, 1, 3) || '*****' || SUBSTR(CREATED_BY, LENGTH(CREATED_BY) - 3, 4)
                        ELSE CREATED_BY
                    END AS CREATED_BY,
                    CASE 
                        WHEN UPDATED_BY = 'agent_bot' THEN UPDATED_BY
                        WHEN LENGTH(UPDATED_BY) >= 7 THEN 
                            SUBSTR(UPDATED_BY, 1, 3) || '*****' || SUBSTR(UPDATED_BY, LENGTH(UPDATED_BY) - 3, 4)
                        ELSE UPDATED_BY
                    END AS UPDATED_BY,
                    REVIEW_STAMP,
                    COST_ESTIMATION,
                    DAYS_ON_LOCATION,
                    TOTAL_COST_ESTIMATION,
                    TOTAL_DAYS_ON_LOCATION
                FROM estimations
                ORDER BY created_at DESC;
            """
            )
            st.session_state["estimations_df_state"] = data_db

            if data_db is not None and not data_db.empty:
                st.data_editor(
                    data_db,
                    hide_index=True,
                    column_order=[
                        each for each in data_db.columns.tolist() if each != "ID"
                    ],
                    key="estimations_df",
                    num_rows="dynamic",
                )
                if st.button("Save Changes"):
                    if "estimations_df" in st.session_state:
                        affected_rows_num = data.update_estimations()
                        st.success(
                            f"Changes saved successfully! {affected_rows_num} rows affected."
                        )
            else:
                st.info("No saved estimates found.")

        except Exception as e:
            st.error(f"Failed to load saved estimates: {e}")
            st.warning("Saved estimates section is temporarily unavailable.")
