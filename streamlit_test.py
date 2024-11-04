import json
import os
import time
from uuid import uuid4

import pandas as pd
import streamlit as st
import streamlit_analytics2
import streamlit_authenticator as stauth
from google.cloud import firestore
from llama_index.agent.openai import OpenAIAgent
from llama_index.core import VectorStoreIndex
from llama_index.core.tools import FunctionTool, QueryEngineTool
from llama_index.embeddings.openai import (
    OpenAIEmbedding,
    OpenAIEmbeddingMode,
    OpenAIEmbeddingModelType,
)
from llama_index.llms.openai import OpenAI
from llama_index.vector_stores.postgres import PGVectorStore

from tools import (
    account_details,
    add,
    bereken_EBITDA,
    bereken_eigen_vermogen,
    bereken_OMZET,
    bereken_VERLIES,
    companies_ids_api_call,
    company_api_call,
    describe_tables,
    has_tax_decreased_api_call,
    list_tables,
    load_data,
    multiply,
    people_api_call,
    period_api_call,
    period_id_fetcher,
    reconciliation_api_call,
)

st.set_page_config(layout="wide", page_title="Fintrax Knowledge Center", page_icon="images/FINTRAX_EMBLEM_POS@2x_TRANSPARENT.png")

#Load tools


# Set up OpenAI client
OPENAI_API_KEY = st.secrets['OPENAI_API_KEY']
COLLECTION_NAME = "openai_vectors"  # Milvus collection name
client = OpenAI(api_key=OPENAI_API_KEY)

#Authentication
import yaml
from yaml.loader import SafeLoader

with open('credentials.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

#Vector DB Setup
cloud_host = st.secrets["db_host"]
cloud_port = st.secrets["db_port"]
cloud_db_name = st.secrets["db_name"]
cloud_db_pwd = st.secrets["db_pwd"]
cloud_db_user = st.secrets["db_user"]

@st.cache_resource
def create_db_connection():
    cloud_aws_vector_store = PGVectorStore.from_params(
        database=cloud_db_name,
        host=cloud_host,
        password=cloud_db_pwd,
        port=cloud_port,
        user=cloud_db_user,
        table_name="800_chunk_400_overlap",
        embed_dim=756,  # openai embedding dimension
    )
    return cloud_aws_vector_store
cloud_aws_vector_store = create_db_connection()

@st.cache_resource
def vector_store_index(_cloud_aws_vector_store):
    index = VectorStoreIndex.from_vector_store(cloud_aws_vector_store,embed_model=OpenAIEmbedding(mode=OpenAIEmbeddingMode.SIMILARITY_MODE, model=OpenAIEmbeddingModelType.TEXT_EMBED_3_SMALL, dimensions=756))
    return index
index = vector_store_index(cloud_aws_vector_store)

llm = OpenAI(model="gpt-4o", temperature=0,
             system_prompt="""Het is jouw taak om een feitelijk antwoord op de gesteld vraag op basis van de gegeven context en wat je weet zelf weet.
BEANTWOORD ENKEL DE VRAAG ALS HET EEN FINANCIELE VRAAG IS!
BEANTWOORD ENKEL ALS DE VRAAG RELEVANTE CONTEXT HEEFT!!
Als de context codes of vakken bevatten, moet de focus op de codes en vakken liggen.
Je antwoord MAG NIET iets zeggen als “volgens de passage” of “context”.
Maak je antwoord overzichtelijk met opsommingstekens indien nodig.
Jij bent een vertrouwd financieel expert in België die mensen helpt met perfect advies.
GEEF VOLDOENDE INFORMATIE!
             """)

query_engine = index.as_query_engine(llm=llm)
budget_tool = QueryEngineTool.from_defaults(
    query_engine,
    name="Financiele_informatie",
    description="Een RAG database met een extensieve hoeveelheid informatie rond belastingen en wetgevingen. Roep dit aan voor algemene financiele vragen en wanneer er gevraagd wordt achter vakken en codes.",
)
@st.cache_resource
def load_tools():
    multiply_tool = FunctionTool.from_defaults(fn=multiply)
    add_tool = FunctionTool.from_defaults(fn=add)
    company_tool = FunctionTool.from_defaults(fn=company_api_call)
    companies_tool = FunctionTool.from_defaults(fn=companies_ids_api_call)
    people_tool = FunctionTool.from_defaults(fn=people_api_call)
    period_tool = FunctionTool.from_defaults(fn=period_api_call)
    tarief_tax_tool = FunctionTool.from_defaults(fn=has_tax_decreased_api_call)
    period_tool = FunctionTool.from_defaults(fn=period_id_fetcher)
    account_tool = FunctionTool.from_defaults(fn=account_details)
    EBITDA_tool = FunctionTool.from_defaults(fn=bereken_EBITDA)
    bereken_OMZET_tool = FunctionTool.from_defaults(fn=bereken_OMZET)
    bereken_VERLIES_tool = FunctionTool.from_defaults(fn=bereken_VERLIES)
    reconciliation_tool = FunctionTool.from_defaults(fn=reconciliation_api_call)
    list_tables_tool = FunctionTool.from_defaults(fn=list_tables)
    describe_tables_tool = FunctionTool.from_defaults(fn=describe_tables)
    load_data_tool = FunctionTool.from_defaults(fn=load_data)
    eigen_vermogen_tool = FunctionTool.from_defaults(bereken_eigen_vermogen)
    return [reconciliation_tool, budget_tool, tarief_tax_tool, companies_tool,account_tool, period_tool, company_tool, 
            EBITDA_tool, list_tables_tool, describe_tables_tool, load_data_tool, bereken_OMZET_tool,
            eigen_vermogen_tool]
tools = load_tools()


system_prompt = '''
Het is jouw taak om een feitelijk antwoord op de gesteld vraag op basis van de gegeven context en wat je weet zelf weet.
BEANTWOORD ENKEL DE VRAAG ALS HET EEN FINANCIELE VRAAG IS!
BEANTWOORD ENKEL ALS DE VRAAG RELEVANTE CONTEXT HEEFT!!
Als de context codes of vakken bevatten, moet de focus op de codes en vakken liggen.
Je antwoord MAG NIET iets zeggen als “volgens de passage” of “context”.
Maak je antwoord overzichtelijk met opsommingstekens indien nodig.
Jij bent een vertrouwd financieel expert in België die mensen helpt met perfect advies.
Als er een berekening gevraagd wordt waarvoor er geen geschikte tool is, antwoord dan met "Sorry, dit kan ik nog niet berekenen."
GEEF VOLDOENDE INFORMATIE!
'''

@st.cache_resource
def create_agent():
    llm = OpenAI(model="gpt-4o", temperature=0)
    agent = OpenAIAgent.from_tools(
        tools, verbose=True, llm=llm, system_prompt=system_prompt
    )
    return agent

agent = create_agent()



#CSS injection that makes the user input right-aligned
st.markdown(
    """
<style>
    .st-emotion-cache-1c7y2kd {
        flex-direction: row-reverse;
        text-align: right;
    }
</style>
""",
    unsafe_allow_html=True,
)

email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

if st.session_state.get("UUID") is None:
    st.session_state["UUID"] = uuid4().hex

if st.session_state["authentication_status"] is None or st.session_state["authentication_status"] is False :
    st.title("Welkom bij het Knowledge Center!")
    try:
        authenticator.login()
    except stauth.LoginError as e:
        st.error(e)

    if st.session_state["authentication_status"]:
        st.session_state["active_section"] = "Chatbot"

    elif st.session_state["authentication_status"] is False:
        st.error('Username/password is incorrect')
    elif st.session_state["authentication_status"] is None:
        st.warning('Please enter your username and password')
        st.session_state["active_section"] = "Username"



col1, col2, col3 = st.sidebar.columns([1,6,1])
col2.image("images/thumbnail-modified.png")

firestore_string = st.secrets["FIRESTORE"]
firestore_cred = json.loads(firestore_string)
db = firestore.Client.from_service_account_info(firestore_cred)


def popup():
    if "username" not in st.session_state and st.session_state.username is not None:
        st.toast("U moet inloggen voor deze functionaliteit.")

if 'state_dict' not in st.session_state:
    st.session_state['state_dict'] = {}
    
with st.sidebar.container(border=True):
    sidecol1, sidecol2, sidecode3 = st.columns(3)
    sidecol2.title("Acties")

    chatbot_button = st.button("Knowledge Center", use_container_width=True,on_click=popup)
    upload_button = st.button("Upload Files", use_container_width=True,on_click=popup)
    connecties = st.button("Connecties", use_container_width=True,on_click=popup)
    voorkeuren = st.button("Voorkeuren", use_container_width=True,on_click=popup)
    rapporten = st.button("Rapporten", use_container_width=True, on_click=popup)
    uitloggen = st.button("Log Uit", use_container_width=True, on_click=popup)


if st.secrets["PROD"] == "False" and "user_name" in st.session_state:
        if os.path.exists(f"analytics/{st.session_state.user_name}.json"):
            streamlit_analytics2.start_tracking(load_from_json=f"analytics/{st.session_state.user_name}.json")
        else:
            streamlit_analytics2.main.reset_counts()
            streamlit_analytics2.start_tracking()

else:
    streamlit_analytics2.start_tracking()
if "active_section" not in st.session_state:
    st.session_state["active_section"] = "Username"
if chatbot_button and "username" in st.session_state and st.session_state.username is not None or st.session_state.active_section == "Username" and st.session_state.username is not None:
    st.session_state["active_section"] = "Chatbot"

if upload_button and "username" in st.session_state and st.session_state.username is not None:
    st.session_state["active_section"] = "Upload Files"

if connecties and "username" in st.session_state and st.session_state.username is not None:
    st.session_state["active_section"] = "Connecties"

if voorkeuren and "username" in st.session_state and st.session_state.username is not None:
    st.session_state["active_section"] = "Voorkeuren"

if rapporten and "username" in st.session_state and st.session_state.username is not None:
    st.session_state["active_section"] = "Rapporten"

if uitloggen and "username" in st.session_state and st.session_state.username is not None:
    st.session_state["active_section"] = "Uitloggen"


if st.session_state["active_section"] == "Chatbot":
    st.title("Knowledge Center")
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

    if "openai_model" not in st.session_state:
        st.session_state["openai_model"] = "gpt-4o"

    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "Hallo, hoe kan ik je helpen? Stel mij al je financiële vragen!"}]

    for message in st.session_state.messages:
        if message["role"] != "system":
            if message["role"] == "assistant": 
                with st.chat_message(message["role"], avatar="images/thumbnail.png"):
                    st.markdown(message["content"])
            else:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

    if prompt := st.chat_input("Stel hier je vraag!"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant", avatar="images/thumbnail.png"):
           
            with st.spinner("Thinking..."):
                mess = agent.stream_chat(prompt)
            response = st.write_stream(mess.response_gen)
        st.session_state.messages.append({"role": "assistant", "content": response})

# If Upload Files is selected
elif st.session_state["active_section"] == "Upload Files":
    st.title("Upload Files")
    st.markdown("Deze functie is nog niet beschikbaar")
    uploaded_file = st.file_uploader("Choose a file", type=["txt", "pdf", "docx", "csv"])
    
    if uploaded_file is not None:
        # Display basic information about the uploaded file
        st.write(f"Uploaded file: {uploaded_file.name}")
        
        # Example: Reading text files and displaying the content
        if uploaded_file.type == "text/plain":
            content = uploaded_file.read().decode("utf-8")
            st.text_area("File Content", content, height=300)
        
        # Example for handling other file types (CSV, PDF, etc.) can be added here
        file_type = uploaded_file.type
        st.write(f"File type: {file_type}")


# If Upload Files is selected
elif st.session_state["active_section"] == "Connecties":
    st.title("Connecties")
    st.markdown("Deze functie is nog niet beschikbaar, verbinden met de API gaat nog geen data doorgeven.")

    fin1,fin2,fin3 = st.columns(3)
    with fin1:
        with st.container(border=True):
            fincol1, fincol2 = st.columns(2)
            fincol1.image("images/silverfin-logo.png")
            fincol2.subheader("Silverfin")
            fincol2.markdown("""Naam: Fiduciaire ABC
                                ID: 561
                                mark@abcaccouting.be""")
            verbindfin = st.button("Connecteer Silverfin", use_container_width=True)
            if verbindfin:
                with st.spinner("Verbinden..."):
                    time.sleep(3)
                st.success("Verbonden!")

    with fin2:
        with st.container(border=True):
            fincol1, fincol2 = st.columns(2)
            fincol1.image("images/mmf-logo.png")
            fincol2.subheader("MyMinFin")
            fincol2.markdown("""Naam: FIDUCIAIRE ABC 
                                support@fintrax.io""")
            verbindmy = st.button("Connecteer MyMinFin", use_container_width=True)
            if verbindmy:
                with st.spinner("Verbinden..."):
                    time.sleep(3)
                st.success("Verbonden!")


elif st.session_state["active_section"] == "Voorkeuren":
    st.title("Voorkeuren")
    st.markdown("Maak hier je standaardvragen of rapporten aan")
    # Initialize session state to store questions
    if 'questions' not in st.session_state:
        st.session_state.questions = []

    # Function to add a new question input field
    def add_question():
        st.session_state.questions.append("")
    
    def remove_question():
        if st.session_state.questions != 0:
            st.session_state.questions=st.session_state.questions[:-1]


    add, rem, _ = st.columns([1,1,30])
    # Button to add a new question dynamically
    add.button('\+', on_click=add_question)
    rem.button('\-', on_click=remove_question)

    # Display the input fields for the questions dynamically
    for i, question in enumerate(st.session_state.questions):
        st.session_state.questions[i] = st.text_input(f"Question {i+1}:", value=question, key=f"question_{i}")

    # Button to save the form
    if st.button("Opslaan"):
        st.success("Opgeslagen!")

elif st.session_state["active_section"] == "Rapporten":
    st.title("Rapporten")
    st.markdown("Deze functie is nog niet beschikbaar")
    if "questions" in st.session_state:
        st.dataframe({"Vragen": st.session_state.questions}, width=800)
        if st.button('Voer Uit'):
            time.sleep(3)
            st.success("Uitgevoerd!")

elif st.session_state["active_section"] == "Uitloggen":
    st.title("Welkom bij het Knowledge Center!")
    try:
        authenticator.login()
    except stauth.LoginError as e:
        st.error(e)

    if st.session_state["authentication_status"]:
        authenticator.logout()    

    elif st.session_state["authentication_status"] is False:
        st.error('Username/password is incorrect')
    elif st.session_state["authentication_status"] is None:
        st.warning('Please enter your username and password')
        st.session_state["active_section"] = "Username"
          


if st.secrets["PROD"] == "False" and "username" in st.session_state and "UUID" in st.session_state:
    streamlit_analytics2.stop_tracking(save_to_json=f"analytics/{st.session_state.email}.json", unsafe_password=st.secrets["ANALYTICS_PWD"])
    doc_ref = db.collection('users').document(str(st.session_state.email))
    
    analytics_data = pd.read_json(f"analytics/{st.session_state.email}.json").to_dict()
    analytics_data["chat"] = {st.session_state.get('UUID'): st.session_state.get("messages")}
    doc_ref.set(analytics_data, merge=True)
else:
    streamlit_analytics2.stop_tracking()
