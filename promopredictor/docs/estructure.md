├── data/                  # Dados brutos, limpos e processados
├── docs/                  # Documentação
├── models/                # Modelos treinados e scripts de treinamento
├── notebooks/             # Jupyter notebooks para exploração e apresentações
├── references/            # Documentação do domínio, artigos de referência, etc.
├── reports/               # Relatórios gerados, como HTML, PDF, LaTeX, etc.
├── requirements.txt       # Dependências do projeto
├── src/                   # Código fonte para uso no projeto
│   ├── __init__.py        # Torna src um pacote Python
│   ├── data/              # Scripts para download ou geração de dados
│   ├── features/          # Scripts para transformação de features
│   ├── models/            # Modelos e algoritmos de ML
│   ├── services/          # Serviços de banco de dados e outras integrações
│   ├── utils/             # Funções utilitárias
│   └── visualization/     # Scripts de visualização
├── tests/                 # Testes automatizados
├── .gitignore             # Ignorar arquivos no git
└── README.md              # Informações do projeto para outros desenvolvedores

├── data/
│   ├── clean/              # Dados que foram limpos e estão prontos para serem usados
│   └── processed/          # Dados que foram transformados para serem usados pelos modelos
├── docs/
│   ├── db_schema.md        # Documentação do esquema do banco de dados
│   └── optimization_report.md  # Relatório sobre otimizações de banco de dados
├── models/
│   └── promotion_analysis.py  # Script para cálculo das melhores promoções
├── notebooks/
│   ├── exploratory_data_analysis.ipynb  # Análise exploratória dos dados
│   └── promotion_success_evaluation.ipynb  # Avaliação das promoções
├── references/
│   ├── data_sources.md     # Descrição das fontes de dados
│   └── research_papers/    # Artigos de referência
├── reports/
│   └── best_promotions_report.pdf  # Relatório das melhores promoções
├── src/
│   ├── __init__.py         # Torna src um pacote Python
│   ├── api/                # Contém a API para integração com o ERP
│   │   ├── __init__.py
│   │   ├── app.py          # Aplicação principal da API
│   │   └── endpoints.py    # Definições dos endpoints da API
│   ├── data/
│   │   ├── clean_data.py   # Script para limpeza dos dados
│   │   └── create_tables.py  # Script para criação de tabelas no BD
│   ├── features/
│   │   └── feature_engineering.py  # Scripts para transformação de features
│   ├── models/
│   │   └── model_training.py  # Scripts para treinar os modelos de ML
│   ├── services/
│   │   └── database_service.py  # Interação com o banco de dados
│   ├── utils/
│   │   └── utilities.py    # Funções utilitárias comuns
│   └── visualization/
│       └── visualize_data.py  # Scripts para visualização de dados
├── tests/
│   └── test_promotion_analysis.py  # Testes para o script de análise de promoções
├── .gitignore
├── README.md
└── requirements.txt