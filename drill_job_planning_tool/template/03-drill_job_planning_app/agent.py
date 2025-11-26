import streamlit as st
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI

from utils import get_targeted_env, get_workspace_client


def ai_ask_estimation_stamp(estimations: dict) -> str:
    model = get_db_chat_client(get_targeted_env("ASSISTANT_MODEL"))
    sys = "Recall everything you know about well drilling cost and time estimation, NPT, as well as actively use the Genie rooms as your assistants to process the input from a user, find all historical median, mean, and anomalies and respond with one of stamps: $OK$ if the values in +/- 15%, $REVIEW$ if the values in +/- 50%, $ANOMALY$ if the values in +/- 60% depending on the comparison you did agaisnt the input user provided. Important! Do not provide any explanation, any other supportingtext, respond ONLY with the one word containing a stamp ($OK$, $REVIEW$ or $ANOMALY$). Never provide your answer longer than 1 world."
    prompt_template = ChatPromptTemplate(
        [
            # SystemMessage(content=sys),
            HumanMessage(content=f"{sys}. Inputs: {estimations}")
        ]
    )

    chat_template_messages = prompt_template.invoke({})

    completion = model.invoke(input=chat_template_messages)
    stamp_txt = completion.text()

    if "$ANOMALY$" in stamp_txt:
        return "‼️"
    elif "$REVIEW$" in stamp_txt:
        return "⚠️"
    elif "$OK$" in stamp_txt:
        return "✅"
    else:
        return "❓"


def get_agent_client():
    w = get_workspace_client()
    return w.serving_endpoints.get_open_ai_client()


def get_langchain_chat_open_ai_client(model: str) -> ChatOpenAI:
    w = get_workspace_client()
    return w.serving_endpoints.get_langchain_chat_open_ai_client(model=model)


def get_db_chat_client(model: str):
    from databricks_langchain import ChatDatabricks

    return ChatDatabricks(endpoint=model, use_responses_api=True)
