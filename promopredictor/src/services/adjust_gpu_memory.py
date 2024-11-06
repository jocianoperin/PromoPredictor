# src/services/adjust_gpu_memory.py

import tensorflow as tf
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ===========================
# Configurações de GPU
# ===========================

def adjust_gpu_memory():
    """
    Ajusta o uso de memória da GPU para otimizar o treinamento.
    """
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
        try:
            # Definir limite de memória manualmente
            for gpu in gpus:
                tf.config.experimental.set_virtual_device_configuration(
                    gpu,
                    [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=3500)]
                )
            logical_gpus = tf.config.experimental.list_logical_devices('GPU')
            logger.info(f"{len(gpus)} GPUs físicas, {len(logical_gpus)} GPUs lógicas configuradas.")
        except RuntimeError as e:
            logger.error(f"Erro ao configurar a GPU: {e}")
