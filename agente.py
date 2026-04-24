import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_experimental.agents import create_pandas_dataframe_agent
from etl import Loader, Transformer
from configLog import configLog
logger = configLog(__name__)

load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_AGENTE")
PATH_LLM_INSTRUCTIONS = '.llm'

class AgenteAnaliseDados:
    
    def __init__(self):
        self.dicionario = None
        self.regras = None
        self.diretrizes = None
        self.dataframe = None
        self.prefix_instrucao = None        
        self.path_llm_instr = PATH_LLM_INSTRUCTIONS
        self.loader = Loader()
        self.transformer = Transformer()
        self.agente = self.carregar_agente()        
    
    def load_context(self,file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()

    def carregar_arquivos(self):                
        path = '.llm'
        dicionario = self.load_context(os.path.join(self.path_llm_instr, 'dicionario_dados.toon'))
        if not dicionario:
            raise ValueError("O arquivo 'dicionario_dados.toon' está vazio ou não foi encontrado.")
        regras = self.load_context(os.path.join(self.path_llm_instr, 'regras_negocio.md'))
        if not regras:
            raise ValueError("O arquivo 'regras_negocio.md' está vazio ou não foi encontrado.")
        diretrizes = self.load_context(os.path.join(self.path_llm_instr, 'diretrizes.md'))
        if not diretrizes:
            raise ValueError("O arquivo 'diretrizes.md' está vazio ou não foi encontrado.")
        
        self.loader.list_files_processed()
        self.dataframe = self.loader.load_all_files()
        self.mapeamentos = self.transformer.load_mapamentos()        
        self.dicionario = dicionario
        self.regras = regras
        self.diretrizes = diretrizes
        
        return

    def montar_prompt(self):
        prompt = f"""
        Você é um Agente de Análise de Dados especializado no mercado de distribuição de cosméticos e produtos de beleza.
        Criado com o objetivo de tornar o acesso aos dados mais fácil e intuitivo para a gerência durante a tomada de decisão.        
        
        1. ESTRUTURA DOS DADOS (Formato TOON):
        Use esta definição para identificar nomes de colunas, tipos de dados e o que cada campo representa no dataframe.
        {self.dicionario}

        2. REGRAS DE NEGÓCIO (Markdown):
        Use estes bulletpoints para aplicar a lógica correta aos cálculos e filtros.
        {self.regras}

        3. DIRETRIZES (Markdown):
        {self.diretrizes}
        """
        
        self.prefix_instrucao = prompt
        return

    def carregar_agente(self):
        self.carregar_arquivos()
        self.montar_prompt()
        
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        # llm = ChatOpenAI(model="gpt-5.4-nano-2026-03-17", temperature=0)

        return create_pandas_dataframe_agent(
            llm,
            self.dataframe,
            verbose=True,
            agent_type="tool-calling",
            prefix=self.prefix_instrucao,
            allow_dangerous_code=True,
            agent_executor_kwargs={"handle_parsing_errors":True},
            return_intermediate_steps=True,
            max_iterations=5
        )

    def analisar(self, pergunta):
        if self.agente is None:
            self.agente = self.carregar_agente()           
        
        # logger.debug(f"Analyzing question: {pergunta}")
        str_input = self.transformer.processar_input(pergunta)        
        # logger.debug(f"Processed input: {str_input}")
        st.session_state.context.append({"role": "user", "content": str_input})
        
        agent_invoke = {
            "input":str_input,
            "chat_history": st.session_state.context
        }

        logger.debug(f"Invoking agent with input: {agent_invoke}")                
        response = self.agente.invoke(agent_invoke)        
        st.session_state.context.append({"role": "assistant", "content": response.get('output','')})

        df_para_download = None
        codigo_gerado = ""
        for action, observation in response.get("intermediate_steps", []):
            codigo_gerado += action.tool_input['query'] + "\n"
            # codigo_gerado += action.tool_input + "\n"
            if isinstance(observation, pd.DataFrame):
                df_para_download = observation

        if response.get('output') in ['Agent stopped due to max iterations.','Agent stopped due to iteration limit or time limit.']:
            stream = 'Desculpe, não consegui processar sua solicitação. Tente formular a pergunta de outra forma, por gentileza.'
        else:
            stream = self.transformer.processar_output(response.get('output'))

        st.session_state.context.append({
                "role": "assistant",
                "content": response.get('output',''),
                "data_df": df_para_download,
                "code": codigo_gerado
            })

        st.session_state.messages.append({
                "role": "assistant",
                "content": stream,
                "data_df": df_para_download,
                "code": codigo_gerado
            })

        logger.debug(f"Agent response: {st.session_state.messages[-1]}")

        return True