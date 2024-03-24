# PromoPredictor: IA para Estratégias de Promoção Inteligente
O PromoPredictor é um sistema desenvolvido para identificar promoções eficazes em produtos de supermercados com base em dados históricos de vendas. Utilizando algoritmos de análise de dados e conexão direta com o banco de dados MariaDB, o sistema analisa as vendas passadas, os preços e os custos dos produtos para prever quais produtos devem entrar em promoção para maximizar vendas e lucros. O sistema também realiza uma limpeza inicial nos dados para garantir a qualidade e a precisão das análises.


## Componentes do Sistema
O PromoPredictor é composto por vários scripts Python, organizados em uma estrutura de pacote para fácil manutenção e expansão:
- db_config.py: Configura a conexão com o banco de dados MariaDB, contendo as credenciais de acesso e parâmetros de conexão.
- logging_config.py: Define a configuração de logging do sistema, garantindo que todas as operações importantes sejam registradas em arquivos de log para monitoramento e debugging.
- cleaning.py: Responsável pela limpeza inicial dos dados, este script remove entradas inválidas ou indesejadas, como vendas com valores negativos ou produtos vendidos com quantidades iguais a zero.
- promotions.py: Analisa os dados de vendas e custos dos produtos para identificar possíveis promoções. O script verifica mudanças significativas nos preços de venda em relação aos preços tabelados, sem alterações correspondentes nos custos, indicando uma promoção.


## Funcionalidades
Limpeza de Dados
O cleaning.py é executado para preparar os dados para análise. Este script:

Remove vendas com valor total menor ou igual a zero.
Exclui registros de produtos vendidos com quantidades ou valores totais menores ou iguais a zero.
Identificação de Promoções
O promotions.py processa os dados limpos para identificar promoções. Este script:

Consulta o banco de dados para encontrar produtos com preços de venda abaixo dos preços tabelados, indicando potenciais promoções.
Salva os detalhes das promoções identificadas em uma nova tabela no banco de dados para análises futuras e tomadas de decisão.


## Log de Operações
Todas as operações significativas e quaisquer erros são registrados em arquivos de log, facilitando o monitoramento do sistema e a resolução de problemas. A configuração de logging é definida em logging_config.py, que organiza os logs em arquivos rotativos para gerenciamento eficiente do espaço de armazenamento.


## Tecnologias Utilizadas
- Linguagens de programação: Python (futuramente MoJo)
- Bibliotecas de Machine Learning: [Inserir bibliotecas]
- Banco de dados: MariaDB


## Instalação e Execução
Descreva os passos de instalação do ambiente, como criar o ambiente virtual com conda ou pip, instalar dependências e como estruturar o banco de dados MariaDB antes de executar os scripts.

Para executar o sistema:

Certifique-se de que o banco de dados MariaDB esteja configurado e acessível.
Execute 

```bash
python -m promopredictor.src.cleaning.cleaning
```

para iniciar a limpeza dos dados.

Execute

```bash
python -m promopredictor.src.promotions.promotions
```

para identificar promoções.



```bash
# Exemplo de código de instalação
