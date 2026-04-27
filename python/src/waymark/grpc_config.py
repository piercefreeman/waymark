GRPC_MAX_MESSAGE_SIZE_BYTES = 25 * 1024 * 1024

GRPC_CHANNEL_OPTIONS = (
    ("grpc.max_send_message_length", GRPC_MAX_MESSAGE_SIZE_BYTES),
    ("grpc.max_receive_message_length", GRPC_MAX_MESSAGE_SIZE_BYTES),
)
