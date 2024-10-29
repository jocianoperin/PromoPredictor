import tensorflow as tf

print("Versão do TensorFlow:", tf.__version__)
print("Dispositivos físicos disponíveis:", tf.config.list_physical_devices())

if tf.config.list_physical_devices('GPU'):
    print("TensorFlow está utilizando a GPU.")
else:
    print("TensorFlow não está utilizando a GPU.")
