import time, io
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from datetime import datetime
from agente import AgenteAnaliseDados

agente = AgenteAnaliseDados()

st.title("Agente de Análise de Dados com OpenIA")

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

if prompt:= st.chat_input("O que vamos analisar hoje?"):
    # Display user message in chat message container
    with chat_box.chat_message("user"):
        # st.markdown(prompt)
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