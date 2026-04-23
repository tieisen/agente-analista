import time, io
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime
from agente import AgenteAnaliseDados

agente = AgenteAnaliseDados()

st.set_page_config(page_title="Agente de Análise de Dados com OpenIA", page_icon="✨")
st.title("Agente de Análise de Dados com OpenIA")

SUGGESTIONS = {
    ":blue[:material/local_library:] O que esse agente faz?": (
        "Quem é você e o que você pode fazer?"
    ),
    ":green[:material/database:] O que você pode acessar?": (
        "Qual o período dos dados que você tem acesso? Quais as colunas e o que representam?"
    ),
    ":orange[:material/multiline_chart:] Crie um gráfico de evolução de vendas": (
        "Monte um gráfico de evolução semanal do faturamento acumulado, de janeiro até março"
    ),
    ":red[:material/deployed_code:] Extraia um relatório de vendas por marca": (
        "Preciso de um relatório com o ranking top 10 dos clientes que mais compraram as seguintes marcas: DAILUS, FELPS e WELLA. Separado por marca. Mostrar o nome do cliente e valor total de compra nos últimos 3 meses"
    ),
}

def response_generator(response):    
    if isinstance(response, list):
        for line in response:
            yield "\n"
            for word in line.split():
                time.sleep(0.05)
                yield word + " "

def get_graph_bytes():
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    return buf

def to_excel(df):
    output = io.BytesIO()
    # Usando xlsxwriter como engine para garantir compatibilidade
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    processed_data = output.getvalue()
    return processed_data

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

if "context" not in st.session_state:
    st.session_state.context = []

user_just_clicked_suggestion = (
    "selected_suggestion" in st.session_state and st.session_state.selected_suggestion
)

user_just_asked_initial_question = (
    "initial_question" in st.session_state and st.session_state.initial_question
)

has_message_history = (
    "messages" in st.session_state and len(st.session_state.messages) > 0
)

if not has_message_history and not user_just_clicked_suggestion:
    with st.container():
        st.chat_input("Faça uma pergunta...", key="initial_question")

        selected_suggestion = st.pills(
            label="Examples",
            label_visibility="collapsed",
            options=SUGGESTIONS.keys(),
            key="selected_suggestion",
        )
    st.stop()

prompt = st.chat_input("O que vamos analisar hoje?")

if not prompt:
    if user_just_asked_initial_question:
        prompt = st.session_state.initial_question
    if user_just_clicked_suggestion:
        prompt = SUGGESTIONS[st.session_state.selected_suggestion]

chat_box = st.container(border=True,height=550)

# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with chat_box.chat_message(message["role"]):
        st.markdown(message["content"])
        
        if "chart" in message:
            st.image(message["chart"], width='stretch')
            
        if "data_df" in message:
            with st.expander("Ver dados"):
                st.dataframe(message["data_df"], width='stretch') 

if prompt:
    # Display user message in chat message container
    with chat_box.chat_message("user"):
        # st.markdown(prompt)
        if user_just_clicked_suggestion or user_just_asked_initial_question:
            prompt = "✨ "+prompt
        st.markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Display assistant response in chat message container
    with chat_box.chat_message("assistant"):
        with st.spinner(":speech_balloon:"):
            agent_output, dataframe = agente.analisar(prompt)
        stream = agent_output if agent_output != 'Agent stopped due to max iterations.' else 'Desculpe, não consegui processar sua solicitação. Tente formular a pergunta de outra forma, por gentileza.'
        st.write_stream(response_generator(stream.split('\n')))
        
        if dataframe is not None:            
            xlsx_data = to_excel(dataframe)
            st.session_state.messages[-1]["data_file"] = xlsx_data            
            st.download_button(
                label="📥 Baixar Excel",
                data=xlsx_data,
                file_name=f"tabela_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        if plt.get_fignums(): # Verifica se há figuras criadas
            st.pyplot(plt.gcf(),width='stretch') # Pega a figura atual e exibe no Streamlit
            graph_bytes = get_graph_bytes()
            st.session_state.messages[-1]["chart"] = graph_bytes            
            st.download_button(
                label="📥 Baixar Gráfico",
                data=graph_bytes,
                file_name=f"grafico_{datetime.now().strftime('%Y%m%d_%H%M')}.png",
                mime="image/png",
                key=f"dl_btn_{len(st.session_state.messages)}"
            )      
            plt.close('all') # Limpa a memória para o próximo gráfico
    # Add assistant response to chat history
    st.session_state.messages.append({"role": "assistant", "content": stream})    