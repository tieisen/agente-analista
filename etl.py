import pandas as pd
import os, json, re
from configLog import configLog
logger = configLog(__name__)

COLUNAS_CRIPTOGRAFADAS = [
    "EMPRESA",
    "ORIGEM_PEDIDO",
    "TIPO_PEDIDO",
    "NOME_VENDEDOR",
    "NOME_CLIENTE",
    "NOME_PRODUTO",
    "LINHA_PRODUTO",
    "GRUPO_PRODUTO",
    "MARCA_PRODUTO"
]

class Loader:
    
    def __init__(self):
        self.raw_file_path = '.data/raw'
        self.processed_file_path = '.data/processed'
        self.current_dir = None
        self.current_files_list = None

    def list_files_raw(self):
        try:
            files = os.listdir(self.raw_file_path)
            self.current_dir = self.raw_file_path
            self.current_files_list = [f for f in files if f.endswith('.csv')]
            return self.current_files_list
        except FileNotFoundError:
            logger.error(f"Diretório {self.raw_file_path} não encontrado.")
            return []

    def list_files_processed(self):
        try:
            files = os.listdir(self.processed_file_path)
            self.current_dir = self.processed_file_path
            self.current_files_list = [f for f in files if f.endswith('.csv')]
            return self.current_files_list
        except FileNotFoundError as e:
            logger.error(f"Diretório {self.processed_file_path} não encontrado. {e}")
            return []
    
    def load_file(self, file_name):
        return pd.read_csv(os.path.join(self.current_dir, file_name))
    
    def load_all_files(self):
        if 'RAW' in self.current_dir.upper():
            dataframes = {}
            for file_name in self.current_files_list:
                dataframes[file_name] = self.load_file(file_name)
        else:
            dataframes = []
            for file_name in self.current_files_list:
                dataframes.append(self.load_file(file_name))
        return pd.concat(dataframes)
    
class Transformer:
    
    def __init__(self):
        self.mapeamentos_path = '.data/depara'
        self.processed_file_path = '.data/processed'
        self.mapeamentos = {}

    def load_mapamentos(self):
        try:
            files = os.listdir(self.mapeamentos_path)
            if not files:
                print(f"Nenhum arquivo encontrado em {self.mapeamentos_path}.")
                return
            
            mapeamentos = {}
            try:
                for file_name in files:
                    with open(os.path.join(self.mapeamentos_path, file_name), 'r', encoding='utf-8') as f:
                        mapeamentos[file_name.replace('_map.json','')] = json.load(f)
                self.mapeamentos = mapeamentos
            except Exception as e:
                logger.error(str(e))
                logger.info("File path: %s",os.path.join(self.mapeamentos_path, file_name))
                logger.info("Mapeamentos: %s",mapeamentos)
                
            return 
            
        except FileNotFoundError:
            logger.error(f"Arquivo {file_name} não encontrado.")
            return

    def mapear_dados_criptografados(self, df):
        
        novo = False
        atualizado = False
                  
        colunas = [col for col in COLUNAS_CRIPTOGRAFADAS if col in df.columns]       
        for coluna in colunas:
            valores_unicos = df[coluna].unique()
            if coluna not in self.mapeamentos:
                self.mapeamentos[coluna] = {valor: f"{coluna}_{i}" for i, valor in enumerate(valores_unicos)}
                novo = True
            else:
                dados_mapeados = self.mapeamentos[coluna]
                for valor in valores_unicos:
                    if valor not in dados_mapeados:                        
                        proximo_indice = len(dados_mapeados)
                        self.mapeamentos[coluna][valor] = f"{coluna}_{proximo_indice}"
                        atualizado = True
        
            if any([novo,atualizado]):
                try:
                    with open(os.path.join(self.mapeamentos_path, f"{coluna}_map.json"), 'w', encoding='utf-8') as f:
                        json.dump(self.mapeamentos[coluna], f, ensure_ascii=False, indent=4)
                    atualizado = False
                except Exception as e:
                    logger.error(str(e))
                    logger.info("Mapeamentos: %s",self.mapeamentos)
                    logger.info("Coluna: %s",coluna)

        return self.mapeamentos

    def substituir_valores(self,df):
        colunas = [col for col in COLUNAS_CRIPTOGRAFADAS if col in df.columns]        
        for coluna in colunas:
            df[coluna] = df[coluna].map(self.mapeamentos[coluna])
        return df

    def salvar_dataframe_anonimizado(self, file_name:str, df:pd.DataFrame):
        df.to_csv(os.path.join(self.processed_file_path, f"anon_{file_name}"),index=False)
        return True
        
    def preprocessar(self, file_name, df):
        print("::: CARREGANDO MAPEAMENTOS :::")
        self.load_mapamentos()
        print("::: MAPEANDO DADOS :::")
        self.mapear_dados_criptografados(df)
        print("::: SUBSTITUINDO VALORES :::")
        df = self.substituir_valores(df)
        print("::: SALVANDO ARQUIVO :::")
        self.salvar_dataframe_anonimizado(file_name,df)
        return df
    
    def processar_input(self, input_raw):
        
        input = re.sub(r"[^\w\s]",'',input_raw.upper()).split(' ')
        resultados = []
        encontrado = {}
        
        # logger.info("input: %s", input)

        for coluna, valores in self.mapeamentos.items():
            for chave, valor in valores.items():
                if chave in input:
                    resultados.append({"origem":chave,"destino":valor})

        if not resultados:
            # logger.info(f"processar_input:: Resultados não encontrados.") 
            return input_raw.upper()

        if len(resultados) == 1:
            encontrado = resultados[0]
        else:            
            tem_marca = "MARCA" in input_raw.upper()
            tem_grupo = "GRUPO" in input_raw.upper()
            tem_produto = "PRODUTO" in input_raw.upper()
            
            # logger.info("tem_marca: %s",tem_marca)
            # logger.info("tem_grupo: %s",tem_grupo)
            # logger.info("tem_produto: %s",tem_produto)
            
            if tem_marca and not all([tem_grupo,tem_produto]):
                encontrado = [r for r in resultados if "MARCA" in r.get('destino')]
            elif tem_grupo and not all([tem_marca,tem_produto]):
                encontrado = [r for r in resultados if "GRUPO" in r.get('destino')]
            elif tem_produto and not all([tem_marca,tem_grupo]):
                encontrado = [r for r in resultados if "PRODUTO" in r.get('destino')]
            else:
                raise ValueError("Ambiguidade: especifique MARCA, GRUPO ou PRODUTO")

        input_tratado:str = input_raw
        for e in encontrado:
            input_tratado = input_tratado.replace(e.get('origem'),e.get('destino'))
        return input_tratado
    
    def processar_output(self, output_raw):
        
        input = re.sub(r"[^\w\s]",'',output_raw.upper()).split(' ')
        resultados = []

        for coluna, valores in self.mapeamentos.items():
            for chave, valor in valores.items():
                if valor in input:
                    resultados.append({"origem":valor,"destino":chave})

        if not resultados:
            # logger.info(f"processar_output:: Resultados não encontrados.") 
            return output_raw
        
        output_tratado:str = output_raw
        for r in resultados:
            output_tratado = output_tratado.replace(r.get('origem'),r.get('destino'))            
        # logger.info(f"processar_output:: resultados={resultados}")
            
        return output_tratado