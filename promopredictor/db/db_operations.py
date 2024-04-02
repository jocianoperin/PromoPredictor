import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from ..db.db_config import get_db_connection
from ..logging_config import get_logger

logger = get_logger(__name__)

class PromotionsDB:
    def __init__(self, conn):
        self.conn = conn

    def create_db_connection(self):
        """Cria e retorna uma nova conexão ao banco de dados usando as configurações armazenadas."""
        try:
            # Cria uma nova conexão ao banco de dados
            conn = get_db_connection()
            return conn
        except mysql.connector.Error as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise

    def create_promotions_table_if_not_exists(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS promotions_identified (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        CodigoProduto INT NOT NULL,
                        Data DATE NOT NULL,
                        ValorUnitario DECIMAL(10, 2) NOT NULL,
                        ValorTabela DECIMAL(10, 2) NOT NULL,
                        UNIQUE KEY unique_promocao (CodigoProduto, Data)
                    );
                """)
                self.conn.commit()
                logger.info("Tabela 'promotions_identified' verificada/criada com sucesso.")
        except mysql.connector.Error as e:
            logger.error("Erro ao criar a tabela 'promotions_identified': %s", e)
            self.conn.rollback()

    def insert_promotion(self, promo):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO promotions_identified (CodigoProduto, Data, ValorUnitario, ValorTabela)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE ValorUnitario = VALUES(ValorUnitario), ValorTabela = VALUES(ValorTabela);
                """, (promo['CodigoProduto'], promo['Data'], promo['ValorUnitario'], promo['ValorTabela']))
                self.conn.commit()
                logger.info("Promoção inserida/atualizada com sucesso.")
        except mysql.connector.Error as e:
            logger.error("Erro ao inserir/atualizar promoção: %s", e)
            self.conn.rollback()

    def process_product_chunk(self, product_chunk):
        """Processa um subconjunto de produtos para identificar promoções, usando uma conexão individual ao banco de dados."""
        # Inicializa o total de promoções identificadas para este chunk
        total_promotions = 0

        # Cria uma nova conexão ao banco de dados para a thread atual
        with self.create_db_connection() as conn:
            try:
                cursor = conn.cursor()

                for product_data in product_chunk:
                    codigo = product_data['CodigoProduto']
                    entries = product_data['Entries']
                    promo_count = 0
                    
                    for i in range(1, len(entries)):
                        avg_cost = sum(e['ValorCusto'] for e in entries[:i]) / i
                        avg_sale_price = sum(e['ValorUnitario'] for e in entries[:i]) / i
                        
                        current_entry = entries[i]
                        if (current_entry['ValorUnitario'] < avg_sale_price * 0.95 and
                            abs(current_entry['ValorCusto'] - avg_cost) < avg_cost * 0.05):
                            
                            # Preparação dos dados para inserção
                            data_to_insert = (
                                current_entry['CodigoProduto'],
                                current_entry['Data'],
                                current_entry['ValorUnitario'],
                                avg_sale_price
                            )
                            
                            # Executa a inserção usando a conexão da thread atual
                            cursor.execute("""
                                INSERT INTO promotions_identified (CodigoProduto, Data, ValorUnitario, ValorTabela)
                                VALUES (%s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE ValorUnitario = VALUES(ValorUnitario), ValorTabela = VALUES(ValorTabela);
                            """, data_to_insert)
                            
                            promo_count += 1
                            
                    if promo_count > 0:
                        logger.info(f"Identificadas e inseridas {promo_count} promoções para o produto {codigo}.")
                    
                    total_promotions += promo_count
                
                # Confirma todas as operações realizadas na transação atual
                conn.commit()

            except mysql.connector.Error as e:
                logger.error(f"Erro no processamento do chunk: {e}")
                conn.rollback()  # Reverte a transação atual caso ocorra erro
            finally:
                if cursor is not None:
                    cursor.close()  # Garante o fechamento do cursor

        return total_promotions

    def identify_and_insert_promotions(self):
        self.create_promotions_table_if_not_exists()
        logger.info("Iniciando a identificação e inserção de promoções.")
        try:
            with self.conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                SELECT 
                    vp.CodigoProduto, 
                    v.Data, 
                    vp.ValorUnitario, 
                    vp.ValorCusto
                FROM vendasprodutosexport vp
                JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
                ORDER BY vp.CodigoProduto, v.Data;
                """)
                products_data = cursor.fetchall()

            '''Este trecho de código agrupa dados de promoções por produto, prepara esses dados para processamento paralelo
                e utiliza ThreadPoolExecutor para identificar promoções em paralelo, melhorando a eficiência para grandes volumes de dados.
                Cada grupo de dados de produto é processado independentemente, e o total de promoções identificadas é acumulado, otimizando
                o tempo de processamento através da execução concorrente.'''
            # Agrupar dados por CodigoProduto
            product_history = {}
            for row in products_data:
                if row['CodigoProduto'] not in product_history:
                    product_history[row['CodigoProduto']] = []
                product_history[row['CodigoProduto']].append(row)

            # Preparar dados para processamento paralelo
            product_chunks = [dict(CodigoProduto=codigo, Entries=entries) for codigo, entries in product_history.items()]

            total_promotions = 0
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(self.process_product_chunk, [chunk]) for chunk in product_chunks]
                for future in as_completed(futures):
                    total_promotions += future.result()

            logger.info(f"Processamento concluído. Total de promoções identificadas e inseridas: {total_promotions}.")

        except mysql.connector.Error as e:
            logger.error(f"Erro ao identificar e inserir promoções: {e}")
            self.conn.rollback()


    def get_all_promotions(self):
        """
        Recupera todas as promoções armazenadas na tabela de promoções.
        """
        promotions = []
        try:
            with self.conn.cursor(dictionary=True) as cursor:
                cursor.execute("""
                    SELECT CodigoProduto, Data AS DataPromocao, ValorUnitario, ValorTabela
                    FROM promotions_identified;
                """)
                promotions = cursor.fetchall()
                logger.info(f"{len(promotions)} promoções recuperadas.")
        except mysql.connector.Error as e:
            logger.error("Erro ao recuperar promoções: %s", e)
        return promotions
    
    def create_promotion_success_analysis_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS promotion_success_analysis (
            id INT AUTO_INCREMENT PRIMARY KEY,
            CodigoProduto INT NOT NULL,
            DataPromocao DATE NOT NULL,
            QuantidadeVendida INT,
            ValorTotalVendido DECIMAL(10, 2),
            UNIQUE KEY unique_promotion (CodigoProduto, DataPromocao)
        );
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.conn.commit()
                logger.info("Tabela 'promotion_success_analysis' verificada/criada com sucesso.")
        except mysql.connector.Error as e:
            self.conn.rollback()
            logger.error(f"Erro ao criar a tabela 'promotion_success_analysis': {e}")

    def insert_promotion_analysis_results(self, results):
        insert_query = """
        INSERT INTO promotion_success_analysis 
            (CodigoProduto, DataPromocao, QuantidadeVendida, ValorTotalVendido)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            QuantidadeVendida = VALUES(QuantidadeVendida), 
            ValorTotalVendido = VALUES(ValorTotalVendido);
        """
        try:
            with self.conn.cursor() as cursor:
                for result in results:
                    cursor.execute(insert_query, 
                        (result['CodigoProduto'], result['DataPromocao'], 
                         result['QuantidadeVendida'], result['ValorTotalVendido']))
                self.conn.commit()
                logger.info("Resultados da análise de promoções inseridos com sucesso.")
        except mysql.connector.Error as e:
            self.conn.rollback()
            logger.error(f"Erro ao inserir resultados da análise de sucesso das promoções: {e}")
    
    def analyze_promotion_success(self):
        self.create_promotion_success_analysis_table()
        query = """
        SELECT pi.CodigoProduto, COUNT(vp.CodigoVenda) AS QuantidadeVendida, 
               SUM(vp.ValorUnitario) AS ValorTotalVendido, pi.Data AS DataPromocao
        FROM promotions_identified pi
        JOIN vendasprodutosexport vp ON pi.CodigoProduto = vp.CodigoProduto
        JOIN vendasexport v ON vp.CodigoVenda = v.Codigo
        WHERE v.Data = pi.Data
        GROUP BY pi.CodigoProduto, pi.Data
        ORDER BY QuantidadeVendida DESC, ValorTotalVendido DESC;
        """
        results = []
        try:
            with self.conn.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
        except mysql.connector.Error as e:
            logger.error(f"Erro ao analisar sucesso das promoções: {e}")

        self.insert_promotion_analysis_results(results)

class DatabaseCleaner:
    def __init__(self, conn):
        self.conn = conn

    def clean_data(self):
        self._clean_vendas()
        self._clean_vendas_produtos()

    def _clean_vendas(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM vendasexport WHERE TotalPedido <= 0")
                affected_rows = cursor.rowcount
                self.conn.commit()
                logger.info(f"Limpeza na tabela 'vendas': {affected_rows} linhas removidas.")
        except mysql.connector.Error as e:
            logger.error(f"Erro durante a limpeza da tabela 'vendas': {e}")
            self.conn.rollback()

    def _clean_vendas_produtos(self):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("DELETE FROM vendasprodutosexport WHERE ValorTotal <= 0 OR Quantidade <= 0")
                affected_rows = cursor.rowcount
                self.conn.commit()
                logger.info(f"Limpeza na tabela 'vendas_produtos': {affected_rows} linhas removidas.")
        except mysql.connector.Error as e:
            logger.error(f"Erro durante a limpeza da tabela 'vendas_produtos': {e}")
            self.conn.rollback()

class DatabaseOptimizer:
    def __init__(self, conn):
        self.conn = conn

    def create_indexes(self):
        # Método para criar índices nas tabelas vendasexport e vendasprodutosexport
        try:
            with self.conn.cursor() as cursor:
                # Lista de comandos SQL para criar índices
                index_commands = [
                    #VendasExport
                    #1. Índices em Colunas de Filtragem e Junção
                    "CREATE INDEX idx_codigo ON vendasexport(Codigo);",
                    "CREATE INDEX idx_data ON vendasexport(Data);",
                    "CREATE INDEX idx_codigocliente ON vendasexport(CodigoCliente);",

                    #2. Índices Compostos
                    "CREATE INDEX idx_data_codigocliente ON vendasexport(Data, CodigoCliente);",

                    #3. Índices em Colunas de Ordenação
                    "CREATE INDEX idx_totalpedido ON vendasexport(TotalPedido);",


                    #VendasProdutosExport
                    #1. Índice para Colunas de Junção
                    "CREATE INDEX idx_vendasprodutosexport_codigovenda ON vendasprodutosexport(CodigoVenda);",
                    "CREATE INDEX idx_vendasprodutosexport_codigoproduto ON vendasprodutosexport(CodigoProduto);",

                    #2. Índice para Operações de Filtragem
                    "CREATE INDEX idx_vendasprodutosexport_codigosecao ON vendasprodutosexport(CodigoSecao);",
                    "CREATE INDEX idx_vendasprodutosexport_codigogrupo ON vendasprodutosexport(CodigoGrupo);",
                    "CREATE INDEX idx_vendasprodutosexport_codigosubgrupo ON vendasprodutosexport(CodigoSubGrupo);",

                    #3. Índice Composto para Consultas Específicas
                    "CREATE INDEX idx_vendasprodutosexport_secaogrupo ON vendasprodutosexport(CodigoSecao, CodigoGrupo);",

                    #4. Índices para Colunas de Ordenação e Agrupamento
                    "CREATE INDEX idx_vendasprodutosexport_valorunitario ON vendasprodutosexport(ValorUnitario);",
                    "CREATE INDEX idx_vendasprodutosexport_quantidade ON vendasprodutosexport(Quantidade);",

                    #5. Avaliar Índices para Colunas de Desconto e Promoção
                    "CREATE INDEX idx_vendasprodutosexport_desconto ON vendasprodutosexport(Desconto);",
                    "CREATE INDEX idx_vendasprodutosexport_precoempromocao ON vendasprodutosexport(PrecoemPromocao);"
                ]
                for command in index_commands:
                    cursor.execute(command)
                    logger.info(f"Índice criado com sucesso: {command}")
                self.conn.commit()
                logger.info("Todos os índices foram criados.")
        except mysql.connector.Error as e:
            logger.error("Erro ao criar índices: %s", e)
            self.conn.rollback()