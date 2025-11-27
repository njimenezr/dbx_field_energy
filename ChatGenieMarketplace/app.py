import altair as alt
import streamlit as st
import pandas as pd
import os
import sys
import base64
from dotenv import load_dotenv

# Import Genie functionality
import sys
sys.path.append(os.path.dirname(__file__))
from genie_room import genie_query

# Import data module and utils from AppFrontEnd
try:
    # Add AppFrontEnd to path
    app_frontend_path = os.path.join(os.path.dirname(__file__), "AppFrontEnd")
    if app_frontend_path not in sys.path:
        sys.path.insert(0, app_frontend_path)
    
    # Import modules - catch both ImportError and AssertionError
    try:
        import data
        from utils import get_context_username, get_targeted_env
    except (ImportError, AssertionError) as e:
        # If data module fails due to missing Lakebase config, set to None
        data = None
        from utils import get_context_username, get_targeted_env
except (ImportError, AssertionError) as e:
    st.warning(f"Could not import some AppFrontEnd modules: {e}. Some features may not work.")
    # Define fallback functions
    def get_context_username():
        return "user"
    def get_targeted_env(key, default=None):
        return os.environ.get(key, default)
    data = None

# Load environment variables
load_dotenv()

# Initialize float feature for chat popover
try:
    from streamlit_float import *
    float_init(theme=True, include_unstable_primary=False)
    FLOAT_AVAILABLE = True
except ImportError:
    FLOAT_AVAILABLE = False
    st.warning("streamlit_float not available, chat popover may not work correctly")

title = get_targeted_env("APP_TITLE", default="App de Riesgos")
st.set_page_config(layout="wide", menu_items=None, page_icon="gear", page_title=title)
ss = st.session_state

# HOCOL Corporate Style
hocol_style = """ 
       <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
            
            * {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
            }
            
            #MainMenu {visibility: hidden; }
            .stAppViewBlockContainer {padding-top: 1rem !important;}
            .stMainBlockContainer {padding-top: 1rem !important;}
            .stHeading {text-align: right;}
            .stHeading h1 {
                padding: 0px;
                color: #003366 !important;
                font-weight: 700 !important;
                font-size: 2rem !important;
                letter-spacing: -0.02em;
            }
            
            /* HOCOL Corporate Colors */
            :root {
                --hocol-blue: #003366;
                --hocol-blue-light: #004d99;
                --hocol-gray: #4a5568;
                --hocol-gray-light: #e2e8f0;
                --hocol-accent: #0066cc;
            }
            
            /* Button styles */
            .stButton > button {
                background-color: #003366 !important;
                color: white !important;
                border-radius: 6px !important;
                border: none !important;
                font-weight: 500 !important;
                padding: 0.5rem 1.5rem !important;
                transition: all 0.3s ease !important;
            }
            
            .stButton > button:hover {
                background-color: #004d99 !important;
                transform: translateY(-1px);
                box-shadow: 0 4px 8px rgba(0, 51, 102, 0.2) !important;
            }
            
            /* Input styles */
            .stTextInput > div > div > input,
            .stSelectbox > div > div > select {
                border: 1px solid #cbd5e0 !important;
                border-radius: 6px !important;
                padding: 0.5rem !important;
                font-size: 0.95rem !important;
            }
            
            .stTextInput > div > div > input:focus,
            .stSelectbox > div > div > select:focus {
                border-color: #003366 !important;
                box-shadow: 0 0 0 3px rgba(0, 51, 102, 0.1) !important;
            }
            
            /* Slider styles */
            .stSlider {
                color: #003366 !important;
            }
            
            /* Subheader styles */
            .stSubheader {
                color: #003366 !important;
                font-weight: 600 !important;
                font-size: 1.25rem !important;
            }
            
            /* Checkbox styles */
            .stCheckbox label {
                color: #4a5568 !important;
                font-weight: 500 !important;
            }
            
            /* Tab styles */
            .stTabs [data-baseweb="tab-list"] {
                gap: 8px;
            }
            
            .stTabs [data-baseweb="tab"] {
                color: #4a5568 !important;
                font-weight: 500 !important;
                padding: 0.75rem 1.5rem !important;
            }
            
            .stTabs [aria-selected="true"] {
                color: #003366 !important;
                border-bottom: 3px solid #003366 !important;
            }
       </style>
    """
st.markdown(hocol_style, unsafe_allow_html=True)

st.title(f"**{title}**", anchor=False)
# Load HOCOL logo
try:
    logo_path = os.path.join(os.path.dirname(__file__), "AppFrontEnd", "static", "images", "hocollogo.png")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as img_file:
            img_data = base64.b64encode(img_file.read()).decode()
            st.markdown(
                f'<p style="text-align: right; padding: 8px 0px; margin: 0px;"><span style="color: #4a5568; font-size: 0.875rem; font-weight: 500; margin-right: 10px; vertical-align: middle;">Powered by</span><span style="background-color: #ffffff; padding: 6px 10px; border-radius: 4px; display: inline-block; vertical-align: middle;"><img src="data:image/png;base64,{img_data}" style="display: block; height: 40px; width: auto; max-width: 180px; object-fit: contain;"/></span></p>',
                unsafe_allow_html=True,
            )
    else:
        # Fallback: use direct path
        st.markdown(
            '<p style="text-align: right; padding: 8px 0px; margin: 0px;"><span style="color: #4a5568; font-size: 0.875rem; font-weight: 500; margin-right: 10px; vertical-align: middle;">Powered by</span><span style="background-color: #ffffff; padding: 6px 10px; border-radius: 4px; display: inline-block; vertical-align: middle;"><img src="AppFrontEnd/static/images/hocollogo.png" style="display: block; height: 40px; width: auto; max-width: 180px; object-fit: contain;"/></span></p>',
            unsafe_allow_html=True,
        )
except Exception:
    # Fallback: use direct path
    st.markdown(
        '<p style="text-align: right; padding: 8px 0px; margin: 0px;"><span style="color: #4a5568; font-size: 0.875rem; font-weight: 500; margin-right: 10px; vertical-align: middle;">Powered by</span><span style="background-color: #ffffff; padding: 6px 10px; border-radius: 4px; display: inline-block; vertical-align: middle;"><img src="AppFrontEnd/static/images/hocollogo.png" style="display: block; height: 40px; width: auto; max-width: 180px; object-fit: contain;"/></span></p>',
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
            border-color: #003366;
        }
        .stPopover button {
            border-bottom-color: #003366 !important;
            color: #003366 !important;
            font-weight: 500 !important;
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

# Genie Chat Assistant
with st.popover("AI Assistant"):
    # Initialize chat history
    if "genie_chat_history" not in st.session_state:
        init_msg = "Hello, I am your Genie AI assistant. How can I help you with your data?"
        st.session_state.genie_chat_history = [{"role": "assistant", "content": init_msg}]
        with st.chat_message("assistant"):
            st.write(init_msg)

    # Display chat history
    for message in st.session_state.genie_chat_history:
        with st.chat_message(message["role"]):
            if isinstance(message["content"], pd.DataFrame):
                st.dataframe(message["content"])
            else:
                st.write(message["content"])

    # Get Genie configuration
    genie_space_id = os.environ.get("GENIE_SPACE")
    service_token = os.environ.get("DATABRICKS_SERVICE_TOKEN")
    
    if not genie_space_id or not service_token:
        st.error("Genie Space ID or Service Token not configured. Please check your environment variables.")
    else:
        # Accept user input
        container = st.container()
        with container:
            if FLOAT_AVAILABLE:
                button_b_pos = "0rem"
                button_css = float_css_helper(width="2.2rem", bottom=button_b_pos, transition=0)
                float_parent(css=button_css)
            prompt = st.chat_input("Ask your question...")

        if prompt:
            # Add user message to history
            st.session_state.genie_chat_history.append({"role": "user", "content": prompt})
            
            # Display user message
            with st.chat_message("user"):
                st.write(prompt)

            # Get response from Genie
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        response, query_text = genie_query(prompt, service_token, genie_space_id)
                        
                        # Display response
                        if isinstance(response, pd.DataFrame):
                            st.dataframe(response)
                            st.session_state.genie_chat_history.append({"role": "assistant", "content": response})
                            
                            # Show SQL query if available
                            if query_text:
                                with st.expander("View SQL Query"):
                                    st.code(query_text, language="sql")
                        else:
                            st.write(response)
                            st.session_state.genie_chat_history.append({"role": "assistant", "content": response})
                            
                            # Show SQL query if available
                            if query_text:
                                with st.expander("View SQL Query"):
                                    st.code(query_text, language="sql")
                    except Exception as e:
                        error_msg = f"Error: {str(e)}"
                        st.error(error_msg)
                        st.session_state.genie_chat_history.append({"role": "assistant", "content": error_msg})

# Initialize data tables
try:
    data.create_lakebase_table()
except Exception as e:
    st.warning(f"Could not initialize data tables: {e}")

# Main content area
left, right = st.columns([0.15, 0.85])

with left:
    # SIDEBAR SECTION WITH INPUTS and SAVE
    st.subheader("Drill Planning Inputs", anchor=False)

    api_number = st.selectbox(
        label="Campo",
        options=["", "Campo-001", "Campo-002", "Campo-003", "Campo-004", "Campo-005"],
        help="Seleccione el campo.",
        index=0
    )

    formation = st.selectbox(
        label="Wellbore",
        options=["Wolfcamp", "Spraberry", "Bone Spring", "Delaware", "Avalon"],
        help="Wellbore for Lateral",
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
        if not data:
            st.error("Data module not available. Please check AppFrontEnd/data.py.tmpl")
        else:
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
        # Well Locations Map - Add your visualization code here
        st.info("Add your map visualization code here")
        # Example:
        # st.map(data)
        pass

    with tab2:
        # Days vs Depth - Add your visualization code here
        st.info("Add your Days vs Depth visualization code here")
        # Example:
        # chart = alt.Chart(data).mark_line().encode(...)
        # st.altair_chart(chart, use_container_width=True)
        pass

    with tab3:
        # Job Cost Estimation - Add your visualization code here
        st.info("Add your cost estimation visualization code here")
        # Example:
        # st.dataframe(cost_data)
        # st.markdown(f"**Total Well Cost:** {total_cost:,.2f}")
        pass

    with tab4:
        st.markdown("**Job Time Estimation**")
        # Job Time Estimation - Add your visualization code here
        st.info("Add your time estimation visualization code here")
        # Example:
        # st.dataframe(time_data)
        # st.markdown(f"**Total Days on Location:** {dol:,.2f}")
        pass

    with tab5:
        # Saved Estimates - Add your visualization code here
        st.info("Add your saved estimates visualization code here")
        # Example:
        # st.data_editor(data)
        # if st.button("Save Changes"):
        #     # Save logic here
        pass

