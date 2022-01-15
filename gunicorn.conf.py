from globals import SSL_CONTEXT

if len(SSL_CONTEXT) > 1:
    certfile=SSL_CONTEXT[0]
    keyfile=SSL_CONTEXT[1]
