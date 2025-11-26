# Streamed response emulator
import random
import time

import streamlit as st

from agent import get_langchain_chat_open_ai_client
from utils import get_targeted_env

SYSTEM_PROMPT = """
You are an AI assistant specialized in Well Planning for the Oil and Gas industry. You assist with tasks related to drilling time estimation, cost forecasting, offset well analysis, and planning optimization workflows.

You understand and can reference drilling engineering concepts, industry terms, and project planning methods (e.g., AFE, NPT, rig scheduling, cost breakdowns).
You must answer only questions strictly related to the topic of well planning time and cost analysis in oil and gas.
If a user asks something outside this domain (e.g., geology, politics, machine learning theory, or general tech), respond with the following system message:
"This assistant only supports questions related to well planning time and cost analysis in oil and gas. Please rephrase your question within that scope."

Be professional, concise, and focused. If the user’s question is unclear but may be related to the domain, politely ask for clarification with a planning-specific interpretation.
Provide only the information needed to answer the question, avoiding unnecessary details or tangents.
If it possible, provide the response in table format.
"""

def response_generator():
    # for dev purpose ONLY
    response = random.choice(
        [
            "Hello there! How can I assist you today?",
            "Hi, human! Is there anything I can help you with?",
            "Do you need help?",
        ]
    )
    for word in response.split():
        yield word + " "
        time.sleep(0.05)


def assistant_chat_stream():
    stream = ge().responses.create(
            model=get_targeted_env("ASSISTANT_MODEL", default="databricks-meta-llama-3-1-8b-instruct"),
            input=[
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state.messages
            ],
            stream=True,
        )

    for event in stream:
        if event.type == "response.output_text.delta":
            yield event.delta
        elif event.type == "output_text":
            for each in event.content:
                yield each