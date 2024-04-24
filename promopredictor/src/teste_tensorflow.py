import tensorflow as tf

# Verificar se a GPU está disponível
if tf.config.list_physical_devices('GPU'):
    print('GPU disponível!')
else:
    print('GPU não disponível.')