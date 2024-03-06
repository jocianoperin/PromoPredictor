import pandas as pd

file_path = '/home/jociano/Projects/PromoPredictor-1/dataset/vendasexport.csv'

# Lê apenas a primeira linha para obter os nomes das colunas
df = pd.read_csv(file_path, nrows=0)
print(df.columns.tolist())

def load_data(file_path, usecols=None):
    """
    Load and optionally filter CSV data from a given file path.
    
    Parameters:
    - file_path: str, path to the CSV file.
    - usecols: list of str, optional, columns to load from the CSV file.
    
    Returns:
    - DataFrame containing the loaded and optionally filtered data.
    """
    return pd.read_csv(file_path, usecols=usecols)

def clean_data(vendas_df, vendas_produtos_df):
    """
    Clean the data by removing sales and products with negative or zero values and filtering unnecessary columns.
    
    Parameters:
    - vendas_df: DataFrame, sales data.
    - vendas_produtos_df: DataFrame, products sold data.
    
    Returns:
    - Tuple of cleaned DataFrames: (cleaned sales data, cleaned products sold data).
    """
    # Remove sales with total order values <= 0
    vendas_df_clean = vendas_df[vendas_df['TotalPedido'] > 0]
    
    # Remove products with total values <= 0 and quantity <= 0
    vendas_produtos_df_clean = vendas_produtos_df[(vendas_produtos_df['ValorTotal'] > 0) & (vendas_produtos_df['Quantidade'] > 0)]
    
    return vendas_df_clean, vendas_produtos_df_clean

def main():
    # Paths to the datasets
    vendas_path = '/home/jociano/Projects/PromoPredictor-1/dataset/vendasexport.csv'
    vendas_produtos_path = '/home/jociano/Projects/PromoPredictor-1/dataset/vendasprodutosexport.csv'
    
    # Necessary columns for each dataset
    necessary_vendas_columns = ['Codigo', 'Data', 'Hora', 'CodigoCliente', 'TotalPedido']
    necessary_vendas_produtos_columns = [
        'CodigoVenda', 'CodigoProduto', 'UNVenda', 'Quantidade', 'ValorTabela',
        'ValorUnitario', 'ValorTotal', 'Desconto', 'CodigoSecao', 'CodigoGrupo',
        'CodigoSubGrupo', 'CodigoFabricante', 'ValorCusto', 'ValorCustoGerencial', 'PrecoemPromocao'
    ]
    
    # Load the datasets with necessary columns
    vendas_df = load_data(vendas_path, usecols=necessary_vendas_columns)
    vendas_produtos_df = load_data(vendas_produtos_path, usecols=necessary_vendas_produtos_columns)
    
    # Clean the datasets
    vendas_df_clean, vendas_produtos_df_clean = clean_data(vendas_df, vendas_produtos_df)
    
    # Example of what you can do next: print the shape of the cleaned datasets
    print(f"Cleaned Sales Data Shape: {vendas_df_clean.shape}")
    print(f"Cleaned Products Sold Data Shape: {vendas_produtos_df_clean.shape}")

if __name__ == "__main__":
    main()