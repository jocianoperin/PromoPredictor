# main.py na raiz do projeto
from src.data.create_tables import create_table_if_not_exists
from src.data.data_cleaner import delete_data, update_data
from src.data.index_manager import create_indexes
from src.data.promotion_processor import fetch_all_products, process_chunks
from src.data.promotion_sales_processor import process_promotions_in_chunks
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

def main():
    try:
        logger.info("Iniciando o processo de inicialização do projeto...")
        
        # Primeira tarefa: criar as tabelas no banco de dados, se não existirem
        logger.info("Criando tabelas 'vendasexport' e 'vendasprodutosexport', se não existirem...")
        create_table_if_not_exists()  

        # Limpeza de dados nas tabelas recém-criadas ou existentes
        logger.info("Iniciando a limpeza de dados nas tabelas 'vendasexport' e 'vendasprodutosexport'...")
        delete_data("vendasprodutosexport", "ValorTotal <= 0 OR Quantidade <= 0")
        delete_data("vendasexport", "TotalPedido <= 0")
        
        # Exemplo de chamada para atualizar dados (ajuste conforme necessário)
        # logger.info("Atualizando dados na tabela 'vendasexport'...")
        # update_data("vendasexport", "Status='Em Análise'", "TotalPedido > 5000")

        # Etapa de criação de índices
        logger.info("Iniciando o processo de criação de índices.")
        # Criação de índices para 'vendasprodutosexport'
        indexes_vendasprodutosexport = [
            ("idx_vendasprodutosexport_codigovenda", "vendasprodutosexport", "CodigoVenda"),
            ("idx_vendasprodutosexport_codigoproduto", "vendasprodutosexport", "CodigoProduto"),
            ("idx_vendasprodutosexport_codigosecao", "vendasprodutosexport", "CodigoSecao"),
            ("idx_vendasprodutosexport_codigogrupo", "vendasprodutosexport", "CodigoGrupo"),
            ("idx_vendasprodutosexport_codigosubgrupo", "vendasprodutosexport", "CodigoSubGrupo"),
            ("idx_vendasprodutosexport_secaogrupo", "vendasprodutosexport", "CodigoSecao, CodigoGrupo"),
            ("idx_vendasprodutosexport_valorunitario", "vendasprodutosexport", "ValorUnitario"),
            ("idx_vendasprodutosexport_quantidade", "vendasprodutosexport", "Quantidade"),
            ("idx_vendasprodutosexport_desconto", "vendasprodutosexport", "Desconto"),
            ("idx_vendasprodutosexport_precoempromocao", "vendasprodutosexport", "PrecoemPromocao"),
        ]
        create_indexes(indexes_vendasprodutosexport)

        # Criação de índices para 'vendasexport'
        indexes_vendasexport = [
            ("idx_codigo", "vendasexport", "Codigo"),
            ("idx_data", "vendasexport", "Data"),
            ("idx_codigocliente", "vendasexport", "CodigoCliente"),
            ("idx_data_codigocliente", "vendasexport", "Data, CodigoCliente"),
            ("idx_totalpedido", "vendasexport", "TotalPedido"),
        ]
        create_indexes(indexes_vendasexport)

        # Buscar todos os produtos para processamento de promoções
        logger.info("Iniciando a busca de todos os produtos para processamento de promoções.")
        products = fetch_all_products()
        logger.info(f"Busca concluída. {len(products)} produtos encontrados para processamento.")

        if products:
            logger.info(f"Iniciando o processamento de {len(products)} produtos para identificação de promoções.")

            # Dividir 'products' em chunks de tamanho 'chunk_size'
            chunk_size = 10  # Defina o tamanho do chunk desejado
            for i in range(0, len(products), chunk_size):
                product_chunk = products[i:i + chunk_size]
                logger.info(f"Processando chunk de produtos {i+1} até {i+chunk_size}")
                process_chunks(product_chunk)  # Aqui você passa um único chunk de cada vez

            logger.info("Processamento de promoções concluído com sucesso.")
        else:
            logger.info("Nenhum produto para processar.")

         # Processamento das promoções em chunks e cálculo dos indicadores de vendas
        logger.info("Iniciando o processamento das promoções para cálculo dos indicadores de vendas...")
        process_promotions_in_chunks()

        logger.info("Processo de inicialização do projeto concluído com sucesso.")
    except Exception as e:
        logger.error(f"Erro durante o processo de inicialização do projeto: {e}")

if __name__ == "__main__":
    main()
