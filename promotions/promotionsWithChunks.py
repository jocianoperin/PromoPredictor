import pandas as pd

def load_data_in_chunks(file_path, usecols=None, sep=';', chunksize=50000):
    """
    Load CSV data in chunks from a given file path.
    
    Parameters:
    - file_path: str, path to the CSV file.
    - usecols: list of str, optional, columns to load from the CSV file.
    - sep: str, delimiter used in the CSV file.
    - chunksize: int, number of rows per chunk.
    
    Returns:
    - Iterator of DataFrame chunks containing the loaded data.
    """
    return pd.read_csv(file_path, usecols=usecols, sep=sep, chunksize=chunksize)

def identify_promotions(vendas_df, chunk):
    """
    Identify products sold in promotions based on the criteria that if the product
    is sold at a price lower than usual without a corresponding reduction in cost,
    it indicates a promotion within the chunk.
    
    Parameters:
    - vendas_df: DataFrame, sales data.
    - chunk: DataFrame, chunk of product sales data.
    
    Returns:
    - DataFrame chunk containing information about promotions.
    """
    data_merged = pd.merge(chunk, vendas_df, left_on="CodigoVenda", right_on="Codigo")
    data_merged['EmPromocao'] = data_merged.apply(
        lambda row: row['ValorUnitario'] < row['ValorTabela'] if pd.notnull(row.get('PrecoemPromocao', None)) else False, axis=1
    )
    return data_merged[data_merged['EmPromocao']]

def main():
    vendas_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendas.csv'
    vendas_produtos_clean_path = '/home/jociano/Projects/PromoPredictor/datasetclean/cleaned_vendasprodutos.csv'
    promotions_output_path = '/home/jociano/Projects/PromoPredictor/datasetclean/promotion/promotions_identified_with_chunks.csv'

    vendas_columns = ['Codigo', 'Data', 'TotalPedido']
    vendas_produtos_columns = ['CodigoVenda', 'CodigoProduto', 'ValorUnitario', 'ValorTabela', 'PrecoemPromocao']
    
    # Load vendas_df normally since it's likely smaller and used as a reference
    vendas_df = pd.read_csv(vendas_clean_path, usecols=vendas_columns, sep=';')

    # Initialize an empty DataFrame to accumulate promotions identified in chunks
    promotions_df = pd.DataFrame()

    for chunk in load_data_in_chunks(vendas_produtos_clean_path, usecols=vendas_produtos_columns, sep=';', chunksize=10000):
        promotions_chunk = identify_promotions(vendas_df, chunk)
        promotions_df = pd.concat([promotions_df, promotions_chunk])

    # Save the accumulated promotions to a CSV file
    promotions_df.to_csv(promotions_output_path, index=False)
    print(f"Promotions identified and saved to {promotions_output_path}")

if __name__ == "__main__":
    main()
